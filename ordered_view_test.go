package natsstore

import (
	"context"
	"encoding/base64"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/storage"
)

const viewTestNamespace = "sessions"

// newViewTestStore builds an ordered store over the fake seam with both backoff
// windows collapsed, so a test that drives a retry or a view re-attach does not
// pay for the production waits.
func newViewTestStore(t *testing.T) (*orderedStore, *fakeOrderedSeam) {
	t.Helper()
	f := newFakeOrderedSeam()
	store := newOrderedStore(f)
	store.retryBase = 0
	store.viewRetryBase = time.Millisecond
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return store, f
}

func viewCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func viewID(orderingScope, stableKey string) storage.OrderedID {
	return storage.OrderedID{
		Namespace:     viewTestNamespace,
		OrderingScope: orderingScope,
		StableKey:     storage.StableKey(stableKey),
	}
}

// viewLabel names a record by the two identity components that vary within a
// view, which is also the pair the frozen sort keys tie-break on.
func viewLabel(id storage.OrderedID) string {
	return id.OrderingScope + "/" + string(id.StableKey)
}

func viewLabels(records []storage.OrderedRecord) []string {
	labels := make([]string, 0, len(records))
	for _, record := range records {
		labels = append(labels, viewLabel(record.ID))
	}
	return labels
}

func mustViewCreate(t *testing.T, ctx context.Context, store *orderedStore, id storage.OrderedID, rankingScope string, rank storage.Rank, due storage.Due) storage.OrderedRecord {
	t.Helper()
	record, created, err := store.Create(ctx, id, rankingScope, []byte(viewLabel(id)), rank, due)
	if err != nil {
		t.Fatalf("Create(%s): %v", viewLabel(id), err)
	}
	if !created {
		t.Fatalf("Create(%s) reported an existing record", viewLabel(id))
	}
	return record
}

func mustListRanked(t *testing.T, ctx context.Context, store *orderedStore, rankingScope string, after storage.RankedCursor, limit int) storage.RankedPage {
	t.Helper()
	page, err := store.ListRanked(ctx, viewTestNamespace, rankingScope, after, limit)
	if err != nil {
		t.Fatalf("ListRanked(%q, limit=%d): %v", rankingScope, limit, err)
	}
	return page
}

func mustListDue(t *testing.T, ctx context.Context, store *orderedStore, bound int64, after storage.DueCursor, limit int) storage.DuePage {
	t.Helper()
	page, err := store.ListDue(ctx, viewTestNamespace, bound, after, limit)
	if err != nil {
		t.Fatalf("ListDue(bound=%d, limit=%d): %v", bound, limit, err)
	}
	return page
}

func mustListOrdered(t *testing.T, ctx context.Context, store *orderedStore, orderingScope string, afterOrder uint64, limit int) storage.OrderedPage {
	t.Helper()
	page, err := store.ListOrdered(ctx, viewTestNamespace, orderingScope, afterOrder, limit)
	if err != nil {
		t.Fatalf("ListOrdered(%q, after=%d): %v", orderingScope, afterOrder, err)
	}
	return page
}

func ranked(value int64) storage.Rank { return storage.Rank{Ranked: true, Value: value} }

func dueAt(millis int64) storage.Due {
	return storage.Due{State: storage.DueAt, UnixMillis: millis}
}

var notDue = storage.Due{State: storage.NotDue}

// TestOrderedViewBootstrapsFromLastPerSubjectHeads proves the view is built from
// the live head of every record subject rather than from anything this process
// wrote. The records are planted straight into the stream model, so a view that
// only learned about records through its own store's mutations would see none of
// them.
func TestOrderedViewBootstrapsFromLastPerSubjectHeads(t *testing.T) {
	t.Parallel()
	store, f := newViewTestStore(t)
	ctx := viewCtx(t)

	planted := []storage.OrderedRecord{
		{ID: viewID("tenant/a", "high"), RankingScope: "workers", Revision: 1, Order: 1, Rank: ranked(20), Due: dueAt(30), Value: []byte("high")},
		{ID: viewID("tenant/a", "low"), RankingScope: "workers", Revision: 4, Order: 2, Rank: ranked(1), Due: dueAt(10), Value: []byte("low")},
		{ID: viewID("tenant/b", "mid"), RankingScope: "workers", Revision: 2, Order: 3, Rank: ranked(10), Due: dueAt(20), Value: []byte("mid")},
	}
	for _, record := range planted {
		seedOrderedRecord(t, f, record)
	}

	page := mustListRanked(t, ctx, store, "workers", "", 10)
	if got, want := viewLabels(page.Records), []string{"tenant/a/high", "tenant/b/mid", "tenant/a/low"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListRanked = %v, want %v", got, want)
	}
	duePage := mustListDue(t, ctx, store, 100, "", 10)
	if got, want := viewLabels(duePage.Records), []string{"tenant/a/low", "tenant/b/mid", "tenant/a/high"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue = %v, want %v", got, want)
	}
	if len(duePage.Records) > 0 && duePage.Records[0].Revision != 4 {
		t.Errorf("ListDue returned revision %d for tenant/a/low, want the stored head revision 4", duePage.Records[0].Revision)
	}
}

// TestOrderedViewAppliesLiveUpdates proves a warm view keeps tracking the stream
// after its first query: a rank move committed later is visible to the next
// query without rebuilding anything.
func TestOrderedViewAppliesLiveUpdates(t *testing.T) {
	t.Parallel()
	store, _ := newViewTestStore(t)
	ctx := viewCtx(t)

	high := mustViewCreate(t, ctx, store, viewID("tenant/a", "high"), "workers", ranked(20), notDue)
	low := mustViewCreate(t, ctx, store, viewID("tenant/a", "low"), "workers", ranked(1), notDue)
	if got, want := viewLabels(mustListRanked(t, ctx, store, "workers", "", 10).Records), []string{"tenant/a/high", "tenant/a/low"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ListRanked(before move) = %v, want %v", got, want)
	}

	moved, err := store.Update(ctx, low.ID, low.Revision, []byte("low-moved"), ranked(30), notDue)
	if err != nil {
		t.Fatalf("Update(rank move): %v", err)
	}
	page := mustListRanked(t, ctx, store, "workers", "", 10)
	if got, want := viewLabels(page.Records), []string{"tenant/a/low", "tenant/a/high"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListRanked(after move) = %v, want %v", got, want)
	}
	if len(page.Records) != 2 || page.Records[0].Revision != moved.Revision || string(page.Records[0].Value) != "low-moved" || page.Records[1].ID != high.ID {
		t.Errorf("ListRanked(after move) did not page the current authoritative records: %v", page.Records)
	}
}

// TestOrderedViewResumesAfterWatchFailure proves the view re-attaches when its
// subscription fails: the first attach is rejected, and the store's next query
// still observes the complete current state.
func TestOrderedViewResumesAfterWatchFailure(t *testing.T) {
	t.Parallel()
	store, f := newViewTestStore(t)
	ctx := viewCtx(t)

	f.mu.Lock()
	f.watchErrs = []error{&OrderedStreamOpError{Stream: "OI_sessions", Op: "watch", Cause: errors.New("connection lost")}}
	f.mu.Unlock()

	mustViewCreate(t, ctx, store, viewID("tenant/a", "only"), "workers", ranked(5), dueAt(5))
	page := mustListDue(t, ctx, store, 100, "", 10)
	if got, want := viewLabels(page.Records), []string{"tenant/a/only"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue(after watch failure) = %v, want %v", got, want)
	}
	if starts := f.watchStartCount(); starts < 2 {
		t.Errorf("watchStream was started %d times, want at least 2 (the rejected attach plus a retry)", starts)
	}
}

// TestOrderedMaterializedViewCatchesUpBeforeRead is the barrier proof. It pins
// the view at a known watermark, commits a record the view has not seen, and
// asserts the query does not return until the view has applied through at least
// the stream tip captured when the query started.
func TestOrderedMaterializedViewCatchesUpBeforeRead(t *testing.T) {
	t.Parallel()
	store, f := newViewTestStore(t)
	ctx := viewCtx(t)

	// Warm the view so the barrier is the only thing left to wait for.
	mustListDue(t, ctx, store, 100, "", 10)

	release := f.holdDeliveries()
	t.Cleanup(release)
	mustViewCreate(t, ctx, store, viewID("tenant/a", "late"), "workers", ranked(1), dueAt(5))
	tip, err := f.streamTip(ctx, "")
	if err != nil {
		t.Fatalf("streamTip: %v", err)
	}
	if tip == 0 {
		t.Fatal("streamTip returned 0 after a committed create")
	}

	type result struct {
		page storage.DuePage
		err  error
	}
	results := make(chan result, 1)
	go func() {
		page, err := store.ListDue(ctx, viewTestNamespace, 100, "", 10)
		results <- result{page: page, err: err}
	}()

	select {
	case got := <-results:
		t.Fatalf("ListDue returned %v (err %v) while the view was still behind the captured tip %d", viewLabels(got.page.Records), got.err, tip)
	case <-time.After(150 * time.Millisecond):
	}

	release()
	got := <-results
	if got.err != nil {
		t.Fatalf("ListDue: %v", got.err)
	}
	if labels, want := viewLabels(got.page.Records), []string{"tenant/a/late"}; !reflect.DeepEqual(labels, want) {
		t.Errorf("ListDue(after barrier) = %v, want %v", labels, want)
	}
	view, err := store.view(viewTestNamespace)
	if err != nil {
		t.Fatalf("view: %v", err)
	}
	if mark := view.currentWatermark(); mark < tip {
		t.Errorf("view watermark = %d after the read, want at least the captured tip %d", mark, tip)
	}
}

// TestOrderedWarmQueryDoesNotEnumerateSubjects is Invariant 15's proof
// obligation. A warm ListDue/ListRanked may touch only the sorted range it
// returns plus the binary-search probes that locate it; it may not walk the
// namespace, and it may not read history back out of the stream.
func TestOrderedWarmQueryDoesNotEnumerateSubjects(t *testing.T) {
	t.Parallel()
	store, f := newViewTestStore(t)
	ctx := viewCtx(t)

	const records = 256
	const limit = 10
	for i := range records {
		key := "session-" + strconv.Itoa(i)
		mustViewCreate(t, ctx, store, viewID("tenant/a", key), "workers", ranked(int64(i)), dueAt(int64(i)))
	}
	// Warm the view: after this the barrier is satisfied with no further work.
	mustListDue(t, ctx, store, math.MaxInt64, "", limit)
	mustListRanked(t, ctx, store, "workers", "", limit)

	// A generous ceiling that is still far below enumeration: the page itself
	// plus two binary searches over the index, plus slack.
	bound := uint64(limit + 4*bitLength(records) + 8)

	for _, query := range []struct {
		name string
		run  func()
	}{
		{name: "ListDue", run: func() { mustListDue(t, ctx, store, math.MaxInt64, "", limit) }},
		{name: "ListRanked", run: func() { mustListRanked(t, ctx, store, "workers", "", limit) }},
	} {
		t.Run(query.name, func(t *testing.T) {
			before := store.queryStats()
			beforeGets := f.getCount()
			query.run()
			after := store.queryStats()

			if visited := after.IndexVisited - before.IndexVisited; visited > bound {
				t.Errorf("%s visited %d index entries over %d records, want no more than %d: the read is enumerating rather than walking a bounded range", query.name, visited, records, bound)
			}
			if applied := after.MessagesApplied - before.MessagesApplied; applied != 0 {
				t.Errorf("%s applied %d stream messages, want 0: a warm view is already at the barrier", query.name, applied)
			}
			if tips := after.StreamTipReads - before.StreamTipReads; tips != 1 {
				t.Errorf("%s performed %d stream tip reads, want exactly 1", query.name, tips)
			}
			if gets := f.getCount() - beforeGets; gets != 0 {
				t.Errorf("%s performed %d per-subject reads, want 0: pages come from the view, never from a subject walk", query.name, gets)
			}
		})
	}
}

// bitLength is the number of binary-search probes an index of n entries can
// need, used only to size the bound above.
func bitLength(n int) int {
	bits := 0
	for n > 0 {
		bits++
		n >>= 1
	}
	return bits
}

// TestOrderedListRankedOrdersByRankStableKeyAndOrderingScope pins the frozen
// descending sort key, including BOTH tiebreakers, and builds each tie group in
// both insertion orders: a comparator that dropped a tiebreaker would still
// produce the right page for whichever insertion order happened to be tested.
func TestOrderedListRankedOrdersByRankStableKeyAndOrderingScope(t *testing.T) {
	t.Parallel()
	// Every record shares rank 10 except the bookends, so the page order is
	// decided by the two tiebreakers.
	group := []storage.OrderedID{
		viewID("tenant/b", "same"),
		viewID("tenant/a", "same"),
		viewID("tenant/a", "alpha"),
		viewID("tenant/a", "zeta"),
	}
	want := []string{"tenant/a/top", "tenant/a/zeta", "tenant/b/same", "tenant/a/same", "tenant/a/alpha", "tenant/a/bottom"}

	for _, insertion := range []struct {
		name  string
		order []int
	}{
		{name: "forward", order: []int{0, 1, 2, 3}},
		{name: "reverse", order: []int{3, 2, 1, 0}},
	} {
		t.Run(insertion.name, func(t *testing.T) {
			t.Parallel()
			store, _ := newViewTestStore(t)
			ctx := viewCtx(t)
			mustViewCreate(t, ctx, store, viewID("tenant/a", "top"), "workers", ranked(20), notDue)
			for _, index := range insertion.order {
				mustViewCreate(t, ctx, store, group[index], "workers", ranked(10), notDue)
			}
			mustViewCreate(t, ctx, store, viewID("tenant/a", "bottom"), "workers", ranked(1), notDue)
			mustViewCreate(t, ctx, store, viewID("tenant/a", "unranked"), "workers", storage.Rank{}, notDue)
			mustViewCreate(t, ctx, store, viewID("tenant/a", "other-scope"), "catalog", ranked(1000), notDue)

			if got := viewLabels(mustListRanked(t, ctx, store, "workers", "", 10).Records); !reflect.DeepEqual(got, want) {
				t.Errorf("ListRanked = %v, want %v", got, want)
			}
		})
	}
}

// TestOrderedListDueOrdersByDueStableKeyAndOrderingScope is the ascending
// counterpart, with the same both-insertion-orders construction.
func TestOrderedListDueOrdersByDueStableKeyAndOrderingScope(t *testing.T) {
	t.Parallel()
	group := []storage.OrderedID{
		viewID("tenant/b", "same"),
		viewID("tenant/a", "same"),
		viewID("tenant/a", "alpha"),
		viewID("tenant/a", "zeta"),
	}
	want := []string{"tenant/a/first", "tenant/a/alpha", "tenant/a/same", "tenant/b/same", "tenant/a/zeta", "tenant/a/last"}

	for _, insertion := range []struct {
		name  string
		order []int
	}{
		{name: "forward", order: []int{0, 1, 2, 3}},
		{name: "reverse", order: []int{3, 2, 1, 0}},
	} {
		t.Run(insertion.name, func(t *testing.T) {
			t.Parallel()
			store, _ := newViewTestStore(t)
			ctx := viewCtx(t)
			mustViewCreate(t, ctx, store, viewID("tenant/a", "first"), "workers", storage.Rank{}, dueAt(5))
			for _, index := range insertion.order {
				mustViewCreate(t, ctx, store, group[index], "workers", storage.Rank{}, dueAt(10))
			}
			mustViewCreate(t, ctx, store, viewID("tenant/a", "last"), "workers", storage.Rank{}, dueAt(20))
			mustViewCreate(t, ctx, store, viewID("tenant/a", "never"), "workers", storage.Rank{}, notDue)

			if got := viewLabels(mustListDue(t, ctx, store, 100, "", 10).Records); !reflect.DeepEqual(got, want) {
				t.Errorf("ListDue = %v, want %v", got, want)
			}
			// The bound is inclusive and excludes everything later.
			if got, want := viewLabels(mustListDue(t, ctx, store, 10, "", 10).Records), want[:5]; !reflect.DeepEqual(got, want) {
				t.Errorf("ListDue(bound=10) = %v, want %v", got, want)
			}
		})
	}
}

// TestOrderedPagesAreBoundedAndResumable walks both keyset listings a page at a
// time and proves the limit is honoured, the cursor resumes exactly where the
// previous page stopped, and an exhausted result set reports an empty cursor.
func TestOrderedPagesAreBoundedAndResumable(t *testing.T) {
	t.Parallel()
	store, _ := newViewTestStore(t)
	ctx := viewCtx(t)

	const records = 7
	for i := range records {
		key := "session-" + strconv.Itoa(i)
		mustViewCreate(t, ctx, store, viewID("tenant/a", key), "workers", ranked(int64(i)), dueAt(int64(i)))
	}

	var rankedLabels []string
	var rankedCursor storage.RankedCursor
	for pages := 0; ; pages++ {
		if pages > records {
			t.Fatal("ListRanked pagination did not terminate")
		}
		page := mustListRanked(t, ctx, store, "workers", rankedCursor, 2)
		if len(page.Records) > 2 {
			t.Fatalf("ListRanked returned %d records for limit 2", len(page.Records))
		}
		rankedLabels = append(rankedLabels, viewLabels(page.Records)...)
		if page.NextCursor == "" {
			break
		}
		rankedCursor = page.NextCursor
	}
	wantRanked := []string{"tenant/a/session-6", "tenant/a/session-5", "tenant/a/session-4", "tenant/a/session-3", "tenant/a/session-2", "tenant/a/session-1", "tenant/a/session-0"}
	if !reflect.DeepEqual(rankedLabels, wantRanked) {
		t.Errorf("paged ListRanked = %v, want %v", rankedLabels, wantRanked)
	}
	// An exhausted result set reports an EMPTY cursor, and it must do so on the
	// page that exhausts it rather than on a following empty one. Both boundary
	// cases are asserted: a limit larger than the result set, and a limit that
	// consumes it exactly.
	if page := mustListRanked(t, ctx, store, "workers", "", records+1); page.NextCursor != "" {
		t.Errorf("ListRanked(limit past the end) next cursor = %q, want empty", page.NextCursor)
	}
	if page := mustListRanked(t, ctx, store, "workers", "", records); page.NextCursor != "" {
		t.Errorf("ListRanked(limit exactly the result set) next cursor = %q, want empty", page.NextCursor)
	}
	if page := mustListRanked(t, ctx, store, "workers", "", records-1); page.NextCursor == "" {
		t.Error("ListRanked(one short of the result set) issued no cursor")
	}

	var dueLabels []string
	var dueCursor storage.DueCursor
	for pages := 0; ; pages++ {
		if pages > records {
			t.Fatal("ListDue pagination did not terminate")
		}
		page := mustListDue(t, ctx, store, 100, dueCursor, 3)
		if len(page.Records) > 3 {
			t.Fatalf("ListDue returned %d records for limit 3", len(page.Records))
		}
		dueLabels = append(dueLabels, viewLabels(page.Records)...)
		if page.NextCursor == "" {
			break
		}
		dueCursor = page.NextCursor
	}
	wantDue := []string{"tenant/a/session-0", "tenant/a/session-1", "tenant/a/session-2", "tenant/a/session-3", "tenant/a/session-4", "tenant/a/session-5", "tenant/a/session-6"}
	if !reflect.DeepEqual(dueLabels, wantDue) {
		t.Errorf("paged ListDue = %v, want %v", dueLabels, wantDue)
	}
	if page := mustListDue(t, ctx, store, 100, "", records+1); page.NextCursor != "" {
		t.Errorf("ListDue(limit past the end) next cursor = %q, want empty", page.NextCursor)
	}
	if page := mustListDue(t, ctx, store, 100, "", records); page.NextCursor != "" {
		t.Errorf("ListDue(limit exactly the result set) next cursor = %q, want empty", page.NextCursor)
	}
	// The eligible end, not the index length, decides exhaustion: a bound that
	// excludes the tail must still report the page it stopped on as final.
	if page := mustListDue(t, ctx, store, 2, "", 3); page.NextCursor != "" {
		t.Errorf("ListDue(limit exactly the eligible range) next cursor = %q, want empty", page.NextCursor)
	}
	if page := mustListDue(t, ctx, store, 2, "", 2); page.NextCursor == "" {
		t.Error("ListDue(one short of the eligible range) issued no cursor")
	}
}

// TestOrderedListOrderedIncludesTombstonesRankedAndDueDoNot pins the split in
// CF3: the acceptance-order stream is immutable history and keeps its tombstone,
// while a tombstone is unranked and canonically not-due, so it leaves both
// current views.
func TestOrderedListOrderedIncludesTombstonesRankedAndDueDoNot(t *testing.T) {
	t.Parallel()
	store, _ := newViewTestStore(t)
	ctx := viewCtx(t)

	first := mustViewCreate(t, ctx, store, viewID("tenant/a", "first"), "workers", ranked(10), dueAt(10))
	mustViewCreate(t, ctx, store, viewID("tenant/a", "second"), "workers", ranked(5), dueAt(20))
	mustViewCreate(t, ctx, store, viewID("tenant/b", "other-scope"), "workers", ranked(1), dueAt(30))

	tombstone, err := store.Delete(ctx, first.ID, first.Revision)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	page := mustListOrdered(t, ctx, store, "tenant/a", 0, 10)
	if got, want := viewLabels(page.Records), []string{"tenant/a/first", "tenant/a/second"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ListOrdered = %v, want %v (the acceptance stream keeps its tombstone)", got, want)
	}
	if !page.Records[0].Deleted || page.Records[0].Revision != tombstone.Revision {
		t.Errorf("ListOrdered returned %#v for the deleted record, want the tombstone", page.Records[0])
	}
	if page.NextAfterOrder != page.Records[1].Order {
		t.Errorf("ListOrdered next order = %d, want %d", page.NextAfterOrder, page.Records[1].Order)
	}
	// The order scope is exclusive: tenant/b has its own acceptance stream.
	if got, want := viewLabels(mustListOrdered(t, ctx, store, "tenant/b", 0, 10).Records), []string{"tenant/b/other-scope"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListOrdered(tenant/b) = %v, want %v", got, want)
	}
	// The exclusive cursor skips exactly the records already returned.
	resumed := mustListOrdered(t, ctx, store, "tenant/a", page.NextAfterOrder, 10)
	if len(resumed.Records) != 0 || resumed.NextAfterOrder != 0 {
		t.Errorf("ListOrdered(after=%d) = %#v, want an exhausted page", page.NextAfterOrder, resumed)
	}

	if got, want := viewLabels(mustListRanked(t, ctx, store, "workers", "", 10).Records), []string{"tenant/a/second", "tenant/b/other-scope"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListRanked = %v, want %v (a tombstone is unranked)", got, want)
	}
	if got, want := viewLabels(mustListDue(t, ctx, store, 100, "", 10).Records), []string{"tenant/a/second", "tenant/b/other-scope"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue = %v, want %v (a tombstone is not due)", got, want)
	}
}

// TestOrderedListOrderedResumeAfterMaxUint64IsExhausted pins the exclusive
// cursor's terminal value. Incrementing MaxUint64 would wrap to zero and
// redeliver the entire scope.
func TestOrderedListOrderedResumeAfterMaxUint64IsExhausted(t *testing.T) {
	t.Parallel()
	store, f := newViewTestStore(t)
	ctx := viewCtx(t)
	record := liveOrderedRecord(viewID("tenant/a", "last"), 1, math.MaxUint64)
	seedOrderedRecord(t, f, record)

	page := mustListOrdered(t, ctx, store, "tenant/a", math.MaxUint64, 10)
	if len(page.Records) != 0 || page.NextAfterOrder != 0 {
		t.Fatalf("ListOrdered(after MaxUint64) = %#v, want an exhausted page", page)
	}
}

// TestOrderedNotDueMoveLeavesTheDueIndex proves a record that moves to not_due
// is removed from the due index rather than merely filtered at read time.
func TestOrderedNotDueMoveLeavesTheDueIndex(t *testing.T) {
	t.Parallel()
	store, _ := newViewTestStore(t)
	ctx := viewCtx(t)

	due := mustViewCreate(t, ctx, store, viewID("tenant/a", "due"), "workers", ranked(1), dueAt(5))
	mustViewCreate(t, ctx, store, viewID("tenant/a", "stays"), "workers", ranked(2), dueAt(9))
	if got, want := viewLabels(mustListDue(t, ctx, store, 100, "", 10).Records), []string{"tenant/a/due", "tenant/a/stays"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ListDue(before move) = %v, want %v", got, want)
	}

	if _, err := store.Update(ctx, due.ID, due.Revision, []byte("parked"), ranked(1), notDue); err != nil {
		t.Fatalf("Update(not due): %v", err)
	}
	if got, want := viewLabels(mustListDue(t, ctx, store, 100, "", 10).Records), []string{"tenant/a/stays"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue(after not-due move) = %v, want %v", got, want)
	}
	// Unranking is the same removal on the other index.
	if _, err := store.Update(ctx, due.ID, due.Revision+1, []byte("parked"), storage.Rank{}, notDue); err != nil {
		t.Fatalf("Update(unranked): %v", err)
	}
	if got, want := viewLabels(mustListRanked(t, ctx, store, "workers", "", 10).Records), []string{"tenant/a/stays"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListRanked(after unrank) = %v, want %v", got, want)
	}
}

// TestOrderedDueMoveRepositions proves a due time that moves backwards
// repositions the record in the ascending index instead of leaving a stale entry
// behind.
func TestOrderedDueMoveRepositions(t *testing.T) {
	t.Parallel()
	store, _ := newViewTestStore(t)
	ctx := viewCtx(t)

	mustViewCreate(t, ctx, store, viewID("tenant/a", "early"), "workers", storage.Rank{}, dueAt(10))
	mustViewCreate(t, ctx, store, viewID("tenant/a", "middle"), "workers", storage.Rank{}, dueAt(20))
	later := mustViewCreate(t, ctx, store, viewID("tenant/a", "later"), "workers", storage.Rank{}, dueAt(30))

	moved, err := store.Update(ctx, later.ID, later.Revision, []byte("later-moved"), storage.Rank{}, dueAt(1))
	if err != nil {
		t.Fatalf("Update(due move): %v", err)
	}
	page := mustListDue(t, ctx, store, 100, "", 10)
	if got, want := viewLabels(page.Records), []string{"tenant/a/later", "tenant/a/early", "tenant/a/middle"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ListDue(after move) = %v, want %v", got, want)
	}
	if page.Records[0].Revision != moved.Revision || string(page.Records[0].Value) != "later-moved" {
		t.Errorf("ListDue(after move) returned a stale record %#v", page.Records[0])
	}
	if got, want := len(mustListDue(t, ctx, store, 5, "", 10).Records), 1; got != want {
		t.Errorf("ListDue(bound=5) returned %d records, want %d", got, want)
	}
}

// TestOrderedListsValidateArguments pins the fail-closed validation every
// listing performs before it touches a view.
func TestOrderedListsValidateArguments(t *testing.T) {
	t.Parallel()
	store, _ := newViewTestStore(t)
	ctx := viewCtx(t)

	listings := []struct {
		name string
		call func(namespace string, limit int) (int, error)
	}{
		{name: "ListOrdered", call: func(namespace string, limit int) (int, error) {
			page, err := store.ListOrdered(ctx, namespace, "tenant/a", 0, limit)
			return len(page.Records), err
		}},
		{name: "ListRanked", call: func(namespace string, limit int) (int, error) {
			page, err := store.ListRanked(ctx, namespace, "workers", "", limit)
			return len(page.Records), err
		}},
		{name: "ListDue", call: func(namespace string, limit int) (int, error) {
			page, err := store.ListDue(ctx, namespace, 1, "", limit)
			return len(page.Records), err
		}},
	}
	for _, listing := range listings {
		for _, limit := range []int{0, -1, storage.MaxOrderedPageLimit + 1} {
			t.Run(listing.name+" limit "+strconv.Itoa(limit), func(t *testing.T) {
				records, err := listing.call(viewTestNamespace, limit)
				var invalid *storage.InvalidOrderedLimitError
				if !errors.As(err, &invalid) || records != 0 {
					t.Fatalf("%s(limit=%d) = %d records, %T %v, want an *InvalidOrderedLimitError and no records", listing.name, limit, records, err, err)
				}
			})
		}
		t.Run(listing.name+" invalid namespace", func(t *testing.T) {
			records, err := listing.call("Bad Namespace", 1)
			var invalid *storage.InvalidNameError
			if !errors.As(err, &invalid) || records != 0 {
				t.Fatalf("%s(bad namespace) = %d records, %T %v, want an *InvalidNameError and no records", listing.name, records, err, err)
			}
		})
	}
	if _, err := store.ListOrdered(ctx, viewTestNamespace, "Bad Scope", 0, 1); !errors.As(err, new(*storage.InvalidNameError)) {
		t.Errorf("ListOrdered(bad ordering scope) = %T %v, want *InvalidNameError", err, err)
	}
	if _, err := store.ListRanked(ctx, viewTestNamespace, "Bad Scope", "", 1); !errors.As(err, new(*storage.InvalidNameError)) {
		t.Errorf("ListRanked(bad ranking scope) = %T %v, want *InvalidNameError", err, err)
	}
}

// TestOrderedCursorsFailClosed exercises all four fail-closed rules on both
// cursor kinds. The malformed and unknown-version tokens come from the
// provider's own probe constructors, which is what F4.4 hands to
// storetest.OrderedCursorProbe.
func TestOrderedCursorsFailClosed(t *testing.T) {
	t.Parallel()
	store, _ := newViewTestStore(t)
	ctx := viewCtx(t)

	mustViewCreate(t, ctx, store, viewID("tenant/a", "one"), "workers", ranked(2), dueAt(2))
	mustViewCreate(t, ctx, store, viewID("tenant/a", "two"), "workers", ranked(1), dueAt(1))
	mustViewCreate(t, ctx, store, viewID("tenant/a", "future"), "workers", ranked(0), dueAt(101))
	rankedCursor := mustListRanked(t, ctx, store, "workers", "", 1).NextCursor
	dueCursor := mustListDue(t, ctx, store, 100, "", 1).NextCursor
	if rankedCursor == "" || dueCursor == "" {
		t.Fatalf("expected both listings to issue a cursor, got ranked=%q due=%q", rankedCursor, dueCursor)
	}

	rankedCases := []struct {
		name   string
		cursor storage.RankedCursor
		rule   storage.OrderedCursorRule
		scope  string
	}{
		{name: "malformed", cursor: storage.RankedCursor(orderedMalformedCursorToken(storage.RankedCursorKind)), rule: storage.OrderedCursorMalformed, scope: "workers"},
		{name: "unknown version", cursor: storage.RankedCursor(orderedUnknownVersionCursorToken(storage.RankedCursorKind)), rule: storage.OrderedCursorUnknownVersion, scope: "workers"},
		{name: "wrong kind", cursor: storage.RankedCursor(dueCursor), rule: storage.OrderedCursorWrongKind, scope: "workers"},
		{name: "query mismatch", cursor: rankedCursor, rule: storage.OrderedCursorQueryMismatch, scope: "catalog"},
	}
	for _, testCase := range rankedCases {
		t.Run("ListRanked "+testCase.name, func(t *testing.T) {
			page, err := store.ListRanked(ctx, viewTestNamespace, testCase.scope, testCase.cursor, 10)
			requireCursorRejection(t, "ListRanked", storage.RankedCursorKind, testCase.rule, len(page.Records), err)
		})
	}
	t.Run("ListRanked namespace mismatch", func(t *testing.T) {
		page, err := store.ListRanked(ctx, "other", "workers", rankedCursor, 10)
		requireCursorRejection(t, "ListRanked", storage.RankedCursorKind, storage.OrderedCursorQueryMismatch, len(page.Records), err)
	})

	dueCases := []struct {
		name   string
		cursor storage.DueCursor
		rule   storage.OrderedCursorRule
		bound  int64
	}{
		{name: "malformed", cursor: storage.DueCursor(orderedMalformedCursorToken(storage.DueCursorKind)), rule: storage.OrderedCursorMalformed, bound: 100},
		{name: "unknown version", cursor: storage.DueCursor(orderedUnknownVersionCursorToken(storage.DueCursorKind)), rule: storage.OrderedCursorUnknownVersion, bound: 100},
		{name: "wrong kind", cursor: storage.DueCursor(rankedCursor), rule: storage.OrderedCursorWrongKind, bound: 100},
		{name: "query mismatch", cursor: dueCursor, rule: storage.OrderedCursorQueryMismatch, bound: 101},
	}
	for _, testCase := range dueCases {
		t.Run("ListDue "+testCase.name, func(t *testing.T) {
			page, err := store.ListDue(ctx, viewTestNamespace, testCase.bound, testCase.cursor, 10)
			requireCursorRejection(t, "ListDue", storage.DueCursorKind, testCase.rule, len(page.Records), err)
		})
	}
	t.Run("ListDue namespace mismatch", func(t *testing.T) {
		page, err := store.ListDue(ctx, "other", 100, dueCursor, 10)
		requireCursorRejection(t, "ListDue", storage.DueCursorKind, storage.OrderedCursorQueryMismatch, len(page.Records), err)
	})
	t.Run("ListDue resume tuple exceeds live bound", func(t *testing.T) {
		// This is a valid provider token with bindings matching the live request,
		// not a rewritten header. Its position could never have been issued by a
		// page bounded at 100 because the tuple itself is due at 101.
		body := base64.RawURLEncoding.EncodeToString([]byte(`{"k":"due","ns":"sessions","bound":100,"due":101,"sk":"two","os":"tenant/a"}`))
		forged := storage.DueCursor("oi1." + body)
		page, err := store.ListDue(ctx, viewTestNamespace, 100, forged, 10)
		requireCursorRejection(t, "ListDue", storage.DueCursorKind, storage.OrderedCursorQueryMismatch, len(page.Records), err)
	})
}

func requireCursorRejection(t *testing.T, operation string, kind storage.OrderedCursorKind, rule storage.OrderedCursorRule, records int, err error) {
	t.Helper()
	var invalid *storage.InvalidOrderedCursorError
	if !errors.As(err, &invalid) {
		t.Fatalf("%s = %T %v, want *InvalidOrderedCursorError", operation, err, err)
	}
	if invalid.Kind != kind {
		t.Errorf("%s cursor error kind = %q, want %q", operation, invalid.Kind, kind)
	}
	if invalid.Rule != rule {
		t.Errorf("%s cursor error rule = %v, want %v", operation, invalid.Rule, rule)
	}
	if records != 0 {
		t.Errorf("%s returned %d records with a rejected cursor, want a fail-closed empty page", operation, records)
	}
}

// TestOrderedCursorsDoNotLeakTheirBytes proves the rejection error carries only
// a bounded length, never the token, so an opaque cursor cannot be echoed back.
func TestOrderedCursorsDoNotLeakTheirBytes(t *testing.T) {
	t.Parallel()
	store, _ := newViewTestStore(t)
	ctx := viewCtx(t)

	token := orderedMalformedCursorToken(storage.RankedCursorKind)
	_, err := store.ListRanked(ctx, viewTestNamespace, "workers", storage.RankedCursor(token), 10)
	if err == nil {
		t.Fatal("ListRanked accepted a malformed cursor")
	}
	if strings.Contains(err.Error(), token) {
		t.Errorf("ListRanked cursor error %q echoed the rejected token", err.Error())
	}
	var invalid *storage.InvalidOrderedCursorError
	if errors.As(err, &invalid) && int(invalid.CursorLength) != len(token) {
		t.Errorf("cursor error length = %d, want %d", invalid.CursorLength, len(token))
	}
}

// TestOrderedCursorsAreInstanceIndependent proves a cursor carries position, not
// authority: a token issued by one store resumes correctly on a second store
// over the same stream, with no per-process key involved.
func TestOrderedCursorsAreInstanceIndependent(t *testing.T) {
	t.Parallel()
	first, f := newViewTestStore(t)
	ctx := viewCtx(t)
	for i := range 4 {
		mustViewCreate(t, ctx, first, viewID("tenant/a", "session-"+strconv.Itoa(i)), "workers", ranked(int64(i)), dueAt(int64(i)))
	}
	page := mustListRanked(t, ctx, first, "workers", "", 2)
	if page.NextCursor == "" {
		t.Fatal("ListRanked issued no cursor")
	}

	second := newOrderedStore(f)
	second.retryBase = 0
	second.viewRetryBase = time.Millisecond
	t.Cleanup(func() {
		if err := second.Close(context.Background()); err != nil {
			t.Errorf("Close(second): %v", err)
		}
	})
	resumed, err := second.ListRanked(ctx, viewTestNamespace, "workers", page.NextCursor, 10)
	if err != nil {
		t.Fatalf("ListRanked(second store, resumed): %v", err)
	}
	if got, want := viewLabels(resumed.Records), []string{"tenant/a/session-1", "tenant/a/session-0"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListRanked(second store, resumed) = %v, want %v", got, want)
	}
}

// TestOrderedViewStopsOnClose is the lifecycle seam F4.4 wires: Close cancels
// every namespace view's goroutine, and a query afterwards fails closed instead
// of starting a new one.
func TestOrderedViewStopsOnClose(t *testing.T) {
	t.Parallel()
	f := newFakeOrderedSeam()
	store := newOrderedStore(f)
	store.retryBase = 0
	store.viewRetryBase = time.Millisecond
	ctx := viewCtx(t)

	mustViewCreate(t, ctx, store, viewID("tenant/a", "one"), "workers", ranked(1), dueAt(1))
	mustListDue(t, ctx, store, 100, "", 10)
	if got := f.watcherCount(); got != 1 {
		t.Fatalf("live watchers before Close = %d, want 1", got)
	}

	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := f.watcherCount(); got != 0 {
		t.Errorf("live watchers after Close = %d, want 0", got)
	}
	if _, err := store.ListDue(ctx, viewTestNamespace, 100, "", 10); !errors.As(err, new(*OrderedStoreClosedError)) {
		t.Errorf("ListDue after Close = %T %v, want *OrderedStoreClosedError", err, err)
	}
	if err := store.Close(ctx); err != nil {
		t.Errorf("Close (second call): %v", err)
	}
}

// TestOrderedStoreCloseCancelsEveryViewBeforeWaiting proves a timed-out Close
// cannot strand views that happen to follow the first slow view in the map.
// Both goroutines deliberately wait behind allowExit after observing cancel, so
// Close must cancel the complete set before any wait can finish.
func TestOrderedStoreCloseCancelsEveryViewBeforeWaiting(t *testing.T) {
	t.Parallel()
	store := newOrderedStore(newFakeOrderedSeam())
	allowExit := make(chan struct{})
	views := make([]*orderedView, 0, 2)
	for _, namespace := range []string{"sessions", "archive"} {
		viewCtx, cancelView := context.WithCancel(context.Background())
		view := &orderedView{namespace: namespace, cancel: cancelView, done: make(chan struct{})}
		views = append(views, view)
		store.views[namespace] = view
		go func() {
			<-viewCtx.Done()
			<-allowExit
			close(view.done)
		}()
	}
	t.Cleanup(func() {
		for _, view := range views {
			view.cancel()
		}
	})

	closeCtx, cancelClose := context.WithCancel(context.Background())
	cancelClose()
	err := store.Close(closeCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Close(expired context) = %T %v, want context.Canceled", err, err)
	}
	close(allowExit)
	for _, view := range views {
		select {
		case <-view.done:
		case <-time.After(time.Second):
			t.Errorf("view %q was not cancelled by Close", view.namespace)
		}
	}
	for _, namespace := range []string{"sessions", "archive"} {
		if !strings.Contains(err.Error(), namespace) {
			t.Errorf("Close error %q does not include failed view %q", err, namespace)
		}
	}
}

// TestOrderedViewFatalConfigurationFailsQueriesImmediately proves a namespace
// whose stream cannot ever serve this layout reports that fact instead of
// blocking every reader until their deadlines expire.
func TestOrderedViewFatalConfigurationFailsQueriesImmediately(t *testing.T) {
	t.Parallel()
	store, f := newViewTestStore(t)
	ctx := viewCtx(t)

	fatal := &OrderedStreamConfigError{Stream: "OI_sessions", Reason: "atomic publish is disabled"}
	f.mu.Lock()
	f.watchErrs = []error{fatal, fatal, fatal, fatal}
	f.mu.Unlock()
	mustViewCreate(t, ctx, store, viewID("tenant/a", "one"), "workers", ranked(1), dueAt(1))

	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := store.ListDue(deadline, viewTestNamespace, 100, "", 10)
	var configErr *OrderedStreamConfigError
	if !errors.As(err, &configErr) {
		t.Fatalf("ListDue = %T %v, want the view's fatal *OrderedStreamConfigError", err, err)
	}
	if starts := f.watchStartCount(); starts != 1 {
		t.Errorf("watchStream was started %d times, want exactly 1: a permanently misconfigured stream must not be retried", starts)
	}
}

// TestOrderedViewRebootsAfterRepairableConfigurationFatal proves a config
// failure does not poison the process forever. An operator may repair stream
// retention out of band; the next query must bootstrap a fresh subscription.
func TestOrderedViewRebootsAfterRepairableConfigurationFatal(t *testing.T) {
	t.Parallel()
	store, f := newViewTestStore(t)
	ctx := viewCtx(t)

	fatal := &OrderedStreamConfigError{Stream: "OI_sessions", Reason: "max age is nonzero"}
	f.mu.Lock()
	f.watchErrs = []error{fatal}
	f.mu.Unlock()
	mustViewCreate(t, ctx, store, viewID("tenant/a", "one"), "workers", ranked(1), dueAt(1))

	if _, err := store.ListDue(ctx, viewTestNamespace, 100, "", 10); !errors.As(err, new(*OrderedStreamConfigError)) {
		t.Fatalf("ListDue(before repair) = %T %v, want *OrderedStreamConfigError", err, err)
	}
	page, err := store.ListDue(ctx, viewTestNamespace, 100, "", 10)
	if err != nil {
		t.Fatalf("ListDue(after repair): %v", err)
	}
	if got, want := viewLabels(page.Records), []string{"tenant/a/one"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue(after repair) = %v, want %v", got, want)
	}
	if starts := f.watchStartCount(); starts != 2 {
		t.Errorf("watchStream was started %d times, want 2 (fatal view plus fresh view)", starts)
	}
}

// TestOrderedQueryHonoursContextWhileWaitingForTheBarrier proves a caller that
// gives up while the view is behind gets its own context error rather than
// blocking forever.
func TestOrderedQueryHonoursContextWhileWaitingForTheBarrier(t *testing.T) {
	t.Parallel()
	store, f := newViewTestStore(t)
	ctx := viewCtx(t)

	mustListDue(t, ctx, store, 100, "", 10)
	release := f.holdDeliveries()
	t.Cleanup(release)
	mustViewCreate(t, ctx, store, viewID("tenant/a", "late"), "workers", ranked(1), dueAt(1))

	bounded, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	page, err := store.ListDue(bounded, viewTestNamespace, 100, "", 10)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ListDue = %T %v, want context.DeadlineExceeded", err, err)
	}
	if len(page.Records) != 0 {
		t.Errorf("ListDue returned %d records with an expired context, want none", len(page.Records))
	}
}

// TestOrderedViewIsIdempotentUnderRedelivery proves a re-attached subscription
// that replays a head the view already holds does not double-insert it into a
// sorted index.
func TestOrderedViewIsIdempotentUnderRedelivery(t *testing.T) {
	t.Parallel()
	store, f := newViewTestStore(t)
	ctx := viewCtx(t)

	record := mustViewCreate(t, ctx, store, viewID("tenant/a", "one"), "workers", ranked(1), dueAt(1))
	mustListDue(t, ctx, store, 100, "", 10)

	view, err := store.view(viewTestNamespace)
	if err != nil {
		t.Fatalf("view: %v", err)
	}
	subject, err := orderedRecordSubject(record.ID)
	if err != nil {
		t.Fatalf("orderedRecordSubject: %v", err)
	}
	data := f.dataOf(subject)
	for range 3 {
		view.applyMessage(subject, 1, data)
	}
	if got, want := viewLabels(mustListDue(t, ctx, store, 100, "", 10).Records), []string{"tenant/a/one"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue after redelivery = %v, want %v", got, want)
	}
	if got, want := viewLabels(mustListRanked(t, ctx, store, "workers", "", 10).Records), []string{"tenant/a/one"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListRanked after redelivery = %v, want %v", got, want)
	}
	if got, want := len(mustListOrdered(t, ctx, store, "tenant/a", 0, 10).Records), 1; got != want {
		t.Errorf("ListOrdered after redelivery returned %d records, want %d", got, want)
	}

	// A re-attach replays heads in stream order, and a reordered or duplicated
	// delivery must never move the view BACKWARDS: an older revision redelivered
	// after a newer one is dropped, not applied.
	updated, err := store.Update(ctx, record.ID, record.Revision, []byte("newer"), ranked(9), dueAt(9))
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	view.applyMessage(subject, 1, data)
	page := mustListDue(t, ctx, store, 100, "", 10)
	if len(page.Records) != 1 {
		t.Fatalf("ListDue after a stale redelivery returned %d records, want 1", len(page.Records))
	}
	if page.Records[0].Revision != updated.Revision || string(page.Records[0].Value) != "newer" {
		t.Errorf("ListDue after a stale redelivery returned revision %d value %q, want revision %d value %q",
			page.Records[0].Revision, page.Records[0].Value, updated.Revision, "newer")
	}
	if page.Records[0].Due.UnixMillis != 9 {
		t.Errorf("a stale redelivery moved the record back to due %d", page.Records[0].Due.UnixMillis)
	}
}

// TestOrderedViewRejectsCorruptRecordPayloads proves a record subject carrying
// bytes this package cannot decode fails the namespace's queries closed rather
// than silently paging a namespace with a record missing from it.
func TestOrderedViewRejectsCorruptRecordPayloads(t *testing.T) {
	t.Parallel()
	store, f := newViewTestStore(t)
	ctx := viewCtx(t)

	record := liveOrderedRecord(viewID("tenant/a", "corrupt"), 1, 1)
	subject, err := orderedRecordSubject(record.ID)
	if err != nil {
		t.Fatalf("orderedRecordSubject: %v", err)
	}
	f.mu.Lock()
	f.setLocked(subject, []byte("{not a record}"))
	f.mu.Unlock()

	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := store.ListDue(bounded, viewTestNamespace, 100, "", 10); !errors.As(err, new(*OrderedCodecError)) {
		t.Fatalf("ListDue over a corrupt record subject = %T %v, want an *OrderedCodecError", err, err)
	}
	if _, err := store.ListDue(bounded, viewTestNamespace, 100, "", 10); !errors.As(err, new(*OrderedCodecError)) {
		t.Fatalf("second ListDue over a corrupt record subject = %T %v, want the sticky *OrderedCodecError", err, err)
	}
	if starts := f.watchStartCount(); starts != 1 {
		t.Errorf("corrupt payload started %d views, want 1: deterministic corruption remains sticky", starts)
	}
}

// TestOrderedViewIgnoresCounterSubjects proves the per-scope order counters that
// share the namespace's stream never reach a query result, and that they still
// advance the view's watermark (they are part of the sequence a barrier waits
// for).
func TestOrderedViewIgnoresCounterSubjects(t *testing.T) {
	t.Parallel()
	store, _ := newViewTestStore(t)
	ctx := viewCtx(t)

	mustViewCreate(t, ctx, store, viewID("tenant/a", "one"), "workers", ranked(1), dueAt(1))
	page := mustListDue(t, ctx, store, 100, "", 10)
	if got, want := viewLabels(page.Records), []string{"tenant/a/one"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ListDue = %v, want %v", got, want)
	}
	view, err := store.view(viewTestNamespace)
	if err != nil {
		t.Fatalf("view: %v", err)
	}
	if got := view.recordCount(); got != 1 {
		t.Errorf("view holds %d records, want 1: the counter subject must not be indexed", got)
	}
}

// TestOrderedCursorProbeTokensAreRejectable pins the two token shapes F4.4 hands
// to storetest.OrderedCursorProbe: both must be nonempty, must not be tokens
// this provider could ever issue, and must classify as their intended rule.
func TestOrderedCursorProbeTokensAreRejectable(t *testing.T) {
	t.Parallel()
	for _, kind := range []storage.OrderedCursorKind{storage.RankedCursorKind, storage.DueCursorKind} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			malformed := orderedMalformedCursorToken(kind)
			unknown := orderedUnknownVersionCursorToken(kind)
			if len(malformed) < 8 || len(unknown) < 8 {
				t.Fatalf("probe tokens are too short to be distinguishable: %q %q", malformed, unknown)
			}
			if _, err := decodeOrderedCursorPayload(kind, malformed); !hasCursorRule(err, storage.OrderedCursorMalformed) {
				t.Errorf("decode(malformed) = %v, want OrderedCursorMalformed", err)
			}
			if _, err := decodeOrderedCursorPayload(kind, unknown); !hasCursorRule(err, storage.OrderedCursorUnknownVersion) {
				t.Errorf("decode(unknown version) = %v, want OrderedCursorUnknownVersion", err)
			}
		})
	}
}

// TestOrderedCursorRoundTripsAndRejectsTampering table-drives the cursor codec
// itself, which is the part a listing test can only observe indirectly.
func TestOrderedCursorRoundTripsAndRejectsTampering(t *testing.T) {
	t.Parallel()
	payload := orderedCursorPayload{
		Kind:          string(storage.RankedCursorKind),
		Namespace:     viewTestNamespace,
		RankingScope:  "workers",
		Rank:          -17,
		StableKey:     "Session/One.v2 Ünicode",
		OrderingScope: "tenant/a",
	}
	token, err := encodeOrderedCursor(payload)
	if err != nil {
		t.Fatalf("encodeOrderedCursor: %v", err)
	}
	decoded, err := decodeOrderedCursorPayload(storage.RankedCursorKind, token)
	if err != nil {
		t.Fatalf("decodeOrderedCursorPayload: %v", err)
	}
	if decoded != payload {
		t.Errorf("cursor round trip = %#v, want %#v", decoded, payload)
	}

	body := token[strings.IndexByte(token, '.')+1:]
	cases := []struct {
		name  string
		token string
		rule  storage.OrderedCursorRule
	}{
		{name: "empty body", token: "oi1.", rule: storage.OrderedCursorMalformed},
		{name: "no version prefix", token: base64.RawURLEncoding.EncodeToString([]byte("{}")), rule: storage.OrderedCursorMalformed},
		{name: "non numeric version", token: "oix." + body, rule: storage.OrderedCursorMalformed},
		{name: "zero-padded version", token: "oi01." + body, rule: storage.OrderedCursorMalformed},
		{name: "multiply-padded version", token: "oi001." + body, rule: storage.OrderedCursorMalformed},
		{name: "signed version", token: "oi+1." + body, rule: storage.OrderedCursorMalformed},
		{name: "future version", token: "oi2." + body, rule: storage.OrderedCursorUnknownVersion},
		{name: "zero version", token: "oi0." + body, rule: storage.OrderedCursorUnknownVersion},
		{name: "not base64", token: "oi1.!!!!", rule: storage.OrderedCursorMalformed},
		{name: "not json", token: "oi1." + base64.RawURLEncoding.EncodeToString([]byte("nope")), rule: storage.OrderedCursorMalformed},
		{name: "unknown field", token: "oi1." + base64.RawURLEncoding.EncodeToString([]byte(`{"k":"ranked","zz":1}`)), rule: storage.OrderedCursorMalformed},
		{name: "trailing json", token: "oi1." + base64.RawURLEncoding.EncodeToString([]byte(`{"k":"ranked"}{"k":"ranked"}`)), rule: storage.OrderedCursorMalformed},
		{name: "unknown kind", token: "oi1." + base64.RawURLEncoding.EncodeToString([]byte(`{"k":"sideways"}`)), rule: storage.OrderedCursorMalformed},
		{name: "other kind", token: "oi1." + base64.RawURLEncoding.EncodeToString([]byte(`{"k":"due"}`)), rule: storage.OrderedCursorWrongKind},
		{name: "oversized noise", token: "oi1." + strings.Repeat("A", maxOrderedCursorBytes), rule: storage.OrderedCursorMalformed},
		// The length cap has to be what rejects this one: the body is a
		// perfectly well-formed payload of the right kind, so every later gate
		// would wave it through.
		{name: "oversized but otherwise valid", token: oversizedOrderedCursorToken(t), rule: storage.OrderedCursorMalformed},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			_, err := decodeOrderedCursorPayload(storage.RankedCursorKind, testCase.token)
			if !hasCursorRule(err, testCase.rule) {
				t.Fatalf("decode(%s) = %v, want rule %v", testCase.name, err, testCase.rule)
			}
		})
	}
}

// oversizedOrderedCursorToken builds a token past maxOrderedCursorBytes whose
// payload is otherwise entirely valid, so only the length cap can reject it.
func oversizedOrderedCursorToken(t *testing.T) string {
	t.Helper()
	token, err := encodeOrderedCursor(orderedCursorPayload{
		Kind:          string(storage.RankedCursorKind),
		Namespace:     viewTestNamespace,
		RankingScope:  "workers",
		StableKey:     strings.Repeat("k", maxOrderedCursorBytes),
		OrderingScope: "tenant/a",
	})
	if err != nil {
		t.Fatalf("encodeOrderedCursor: %v", err)
	}
	if len(token) <= maxOrderedCursorBytes {
		t.Fatalf("oversized probe token is only %d bytes, want more than %d", len(token), maxOrderedCursorBytes)
	}
	return token
}

func hasCursorRule(err error, rule storage.OrderedCursorRule) bool {
	var invalid *storage.InvalidOrderedCursorError
	return errors.As(err, &invalid) && invalid.Rule == rule
}

// TestOrderedViewsAreIsolatedByNamespace proves one view per namespace, each
// with its own subscription and its own records.
func TestOrderedViewsAreIsolatedByNamespace(t *testing.T) {
	t.Parallel()
	store, f := newViewTestStore(t)
	ctx := viewCtx(t)

	mustViewCreate(t, ctx, store, viewID("tenant/a", "here"), "workers", ranked(1), dueAt(1))
	other := storage.OrderedID{Namespace: "archive", OrderingScope: "tenant/a", StableKey: "there"}
	if _, _, err := store.Create(ctx, other, "workers", []byte("there"), ranked(1), dueAt(1)); err != nil {
		t.Fatalf("Create(archive): %v", err)
	}

	if got, want := viewLabels(mustListDue(t, ctx, store, 100, "", 10).Records), []string{"tenant/a/here"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue(sessions) = %v, want %v", got, want)
	}
	archivePage, err := store.ListDue(ctx, "archive", 100, "", 10)
	if err != nil {
		t.Fatalf("ListDue(archive): %v", err)
	}
	if got, want := viewLabels(archivePage.Records), []string{"tenant/a/there"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue(archive) = %v, want %v", got, want)
	}
	if got := f.watcherCount(); got != 2 {
		t.Errorf("live watchers = %d, want one per open namespace (2)", got)
	}
}

// TestOrderedListsAreConcurrentlySafe drives the view from several readers while
// writers mutate it, which is what -race is here to check.
func TestOrderedListsAreConcurrentlySafe(t *testing.T) {
	t.Parallel()
	store, _ := newViewTestStore(t)
	ctx := viewCtx(t)

	for i := range 16 {
		mustViewCreate(t, ctx, store, viewID("tenant/a", "session-"+strconv.Itoa(i)), "workers", ranked(int64(i)), dueAt(int64(i)))
	}
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := range 8 {
				if _, err := store.ListRanked(ctx, viewTestNamespace, "workers", "", 5); err != nil {
					t.Errorf("ListRanked: %v", err)
					return
				}
				if _, err := store.ListDue(ctx, viewTestNamespace, 100, "", 5); err != nil {
					t.Errorf("ListDue: %v", err)
					return
				}
				id := viewID("tenant/b", "worker-"+strconv.Itoa(worker)+"-"+strconv.Itoa(round))
				if _, _, err := store.Create(ctx, id, "workers", []byte("v"), ranked(int64(round)), dueAt(int64(round))); err != nil {
					t.Errorf("Create: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestOrderedPagesOwnTheirValues proves a caller can mutate a returned Value
// without reaching into the view.
func TestOrderedPagesOwnTheirValues(t *testing.T) {
	t.Parallel()
	store, _ := newViewTestStore(t)
	ctx := viewCtx(t)

	mustViewCreate(t, ctx, store, viewID("tenant/a", "one"), "workers", ranked(1), dueAt(1))
	page := mustListDue(t, ctx, store, 100, "", 10)
	if len(page.Records) != 1 {
		t.Fatalf("ListDue returned %d records, want 1", len(page.Records))
	}
	page.Records[0].Value[0] = 'X'
	again := mustListDue(t, ctx, store, 100, "", 10)
	if got, want := string(again.Records[0].Value), "tenant/a/one"; got != want {
		t.Errorf("ListDue value after caller mutation = %q, want %q", got, want)
	}
}
