//go:build integration

package natsstore

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/looprig/storage"
	"github.com/nats-io/nats.go/jetstream"
)

// The unit tests drive the view through a fake seam that models JetStream's
// per-subject retention and fences. These tests drive the PRODUCTION seam
// against the embedded server, so the parts a model cannot vouch for are
// exercised for real: an ordered ephemeral consumer with DeliverLastPerSubject
// over a MaxMsgsPerSubject=1 stream, the tail that follows it, the stream tip a
// barrier is captured from, and the layout verification a read performs without
// provisioning anything.

func viewIntegrationID(orderingScope, key string) storage.OrderedID {
	return storage.OrderedID{
		Namespace:     "sessions/live",
		OrderingScope: orderingScope,
		StableKey:     storage.StableKey(key),
	}
}

func viewIntegrationLabels(records []storage.OrderedRecord) []string {
	labels := make([]string, 0, len(records))
	for _, record := range records {
		labels = append(labels, record.ID.OrderingScope+"/"+string(record.ID.StableKey))
	}
	return labels
}

// TestOrderedViewAgainstEmbeddedServer proves the whole read path over a real
// stream: bootstrap from the live heads, then a live update the tail must carry
// into both current indexes.
func TestOrderedViewAgainstEmbeddedServer(t *testing.T) {
	store, _ := newOrderedTestStore(t)
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	ctx := testCtx(t)
	namespace := viewIntegrationID("", "").Namespace

	high := viewIntegrationID("tenant.a", "high")
	low := viewIntegrationID("tenant.a", "low")
	other := viewIntegrationID("tenant.b", "other")
	for _, seed := range []struct {
		id   storage.OrderedID
		rank int64
		due  int64
	}{
		{id: high, rank: 30, due: 30},
		{id: low, rank: 10, due: 10},
		{id: other, rank: 20, due: 20},
	} {
		if _, _, err := store.Create(ctx, seed.id, "catalog/main", []byte(seed.id.StableKey),
			storage.Rank{Ranked: true, Value: seed.rank},
			storage.Due{State: storage.DueAt, UnixMillis: seed.due}); err != nil {
			t.Fatalf("Create(%s): %v", seed.id.StableKey, err)
		}
	}

	rankedPage, err := store.ListRanked(ctx, namespace, "catalog/main", "", 10)
	if err != nil {
		t.Fatalf("ListRanked: %v", err)
	}
	if got, want := viewIntegrationLabels(rankedPage.Records), []string{"tenant.a/high", "tenant.b/other", "tenant.a/low"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ListRanked = %v, want %v", got, want)
	}
	duePage, err := store.ListDue(ctx, namespace, 100, "", 10)
	if err != nil {
		t.Fatalf("ListDue: %v", err)
	}
	if got, want := viewIntegrationLabels(duePage.Records), []string{"tenant.a/low", "tenant.b/other", "tenant.a/high"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ListDue = %v, want %v", got, want)
	}

	// A live update must reach the warm view through the tail, and the barrier
	// must make it visible to the very next query.
	current, err := store.Get(ctx, low)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := store.Update(ctx, low, current.Revision, []byte("promoted"),
		storage.Rank{Ranked: true, Value: 99}, storage.Due{State: storage.NotDue}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	rankedPage, err = store.ListRanked(ctx, namespace, "catalog/main", "", 10)
	if err != nil {
		t.Fatalf("ListRanked(after update): %v", err)
	}
	if got, want := viewIntegrationLabels(rankedPage.Records), []string{"tenant.a/low", "tenant.a/high", "tenant.b/other"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListRanked(after update) = %v, want %v", got, want)
	}
	if string(rankedPage.Records[0].Value) != "promoted" {
		t.Errorf("ListRanked(after update) returned a stale value %q", rankedPage.Records[0].Value)
	}
	duePage, err = store.ListDue(ctx, namespace, 100, "", 10)
	if err != nil {
		t.Fatalf("ListDue(after update): %v", err)
	}
	if got, want := viewIntegrationLabels(duePage.Records), []string{"tenant.b/other", "tenant.a/high"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue(after update) = %v, want %v", got, want)
	}

	// The acceptance stream keeps its tombstone; the current views drop it.
	deleted, err := store.Get(ctx, high)
	if err != nil {
		t.Fatalf("Get(high): %v", err)
	}
	if _, err := store.Delete(ctx, high, deleted.Revision); err != nil {
		t.Fatalf("Delete(high): %v", err)
	}
	orderedPage, err := store.ListOrdered(ctx, namespace, "tenant.a", 0, 10)
	if err != nil {
		t.Fatalf("ListOrdered: %v", err)
	}
	if got, want := viewIntegrationLabels(orderedPage.Records), []string{"tenant.a/high", "tenant.a/low"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ListOrdered = %v, want %v", got, want)
	}
	if !orderedPage.Records[0].Deleted {
		t.Errorf("ListOrdered omitted the tombstone state of %s", orderedPage.Records[0].ID.StableKey)
	}
	if orderedPage.NextAfterOrder != orderedPage.Records[1].Order {
		t.Errorf("ListOrdered next order = %d, want %d", orderedPage.NextAfterOrder, orderedPage.Records[1].Order)
	}
}

// TestOrderedViewSeesAnotherWritersCommits is the property a per-process cache
// could not have: the view is derived from the stream, so a record written by a
// SEPARATE store over a separate seam is visible to this store's next query
// without any coordination between them.
func TestOrderedViewSeesAnotherWritersCommits(t *testing.T) {
	engine := newOrderedTestEngine(t)
	reader := newOrderedStore(newOrderedTestSeam(t, engine))
	writer := newOrderedStore(newOrderedTestSeam(t, engine))
	t.Cleanup(func() {
		if err := reader.Close(context.Background()); err != nil {
			t.Errorf("Close(reader): %v", err)
		}
		if err := writer.Close(context.Background()); err != nil {
			t.Errorf("Close(writer): %v", err)
		}
	})
	ctx := testCtx(t)
	namespace := viewIntegrationID("", "").Namespace

	first := viewIntegrationID("tenant.a", "first")
	if _, _, err := writer.Create(ctx, first, "catalog/main", []byte("first"),
		storage.Rank{Ranked: true, Value: 1}, storage.Due{State: storage.DueAt, UnixMillis: 1}); err != nil {
		t.Fatalf("Create(first): %v", err)
	}
	// Warm the reader's view on the state as it stands.
	if page, err := reader.ListDue(ctx, namespace, 100, "", 10); err != nil {
		t.Fatalf("ListDue(warm): %v", err)
	} else if len(page.Records) != 1 {
		t.Fatalf("ListDue(warm) = %v, want one record", viewIntegrationLabels(page.Records))
	}

	second := viewIntegrationID("tenant.a", "second")
	if _, _, err := writer.Create(ctx, second, "catalog/main", []byte("second"),
		storage.Rank{Ranked: true, Value: 2}, storage.Due{State: storage.DueAt, UnixMillis: 2}); err != nil {
		t.Fatalf("Create(second): %v", err)
	}
	page, err := reader.ListDue(ctx, namespace, 100, "", 10)
	if err != nil {
		t.Fatalf("ListDue(after foreign write): %v", err)
	}
	if got, want := viewIntegrationLabels(page.Records), []string{"tenant.a/first", "tenant.a/second"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ListDue(after foreign write) = %v, want %v: the barrier must carry another writer's commit", got, want)
	}
}

// TestOrderedListsPageOverARealStream walks both keyset listings to exhaustion
// against the server, which is where a cursor that failed to advance would hang
// rather than merely misorder.
func TestOrderedListsPageOverARealStream(t *testing.T) {
	store, _ := newOrderedTestStore(t)
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	ctx := testCtx(t)
	namespace := viewIntegrationID("", "").Namespace

	const records = 25
	for i := range records {
		id := viewIntegrationID("tenant.a", "session-"+strconv.Itoa(i))
		if _, _, err := store.Create(ctx, id, "catalog/main", []byte(id.StableKey),
			storage.Rank{Ranked: true, Value: int64(i)},
			storage.Due{State: storage.DueAt, UnixMillis: int64(i)}); err != nil {
			t.Fatalf("Create(%s): %v", id.StableKey, err)
		}
	}

	var seen []string
	var cursor storage.DueCursor
	for pages := 0; ; pages++ {
		if pages > records {
			t.Fatal("ListDue pagination did not terminate over a real stream")
		}
		page, err := store.ListDue(ctx, namespace, 1000, cursor, 4)
		if err != nil {
			t.Fatalf("ListDue: %v", err)
		}
		if len(page.Records) > 4 {
			t.Fatalf("ListDue returned %d records for limit 4", len(page.Records))
		}
		seen = append(seen, viewIntegrationLabels(page.Records)...)
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(seen) != records {
		t.Fatalf("paged ListDue returned %d records, want %d: %v", len(seen), records, seen)
	}
	for i, label := range seen {
		if want := "tenant.a/session-" + strconv.Itoa(i); label != want {
			t.Fatalf("paged ListDue[%d] = %q, want %q", i, label, want)
		}
	}

	var ranked []string
	var rankedCursor storage.RankedCursor
	for pages := 0; ; pages++ {
		if pages > records {
			t.Fatal("ListRanked pagination did not terminate over a real stream")
		}
		page, err := store.ListRanked(ctx, namespace, "catalog/main", rankedCursor, 7)
		if err != nil {
			t.Fatalf("ListRanked: %v", err)
		}
		ranked = append(ranked, viewIntegrationLabels(page.Records)...)
		if page.NextCursor == "" {
			break
		}
		rankedCursor = page.NextCursor
	}
	if len(ranked) != records {
		t.Fatalf("paged ListRanked returned %d records, want %d", len(ranked), records)
	}
	for i, label := range ranked {
		if want := "tenant.a/session-" + strconv.Itoa(records-1-i); label != want {
			t.Fatalf("paged ListRanked[%d] = %q, want %q", i, label, want)
		}
	}
}

// TestOrderedListsOnAnUnwrittenNamespaceAreEmpty proves a read never provisions
// a stream: querying a namespace nothing has written yields an empty page, and
// the stream still does not exist afterwards.
func TestOrderedListsOnAnUnwrittenNamespaceAreEmpty(t *testing.T) {
	store, seam := newOrderedTestStore(t)
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	ctx := testCtx(t)

	page, err := store.ListDue(ctx, "sessions/empty", 100, "", 10)
	if err != nil {
		t.Fatalf("ListDue(unwritten namespace): %v", err)
	}
	if len(page.Records) != 0 || page.NextCursor != "" {
		t.Errorf("ListDue(unwritten namespace) = %#v, want an empty page", page)
	}
	ordered, err := store.ListOrdered(ctx, "sessions/empty", "tenant.a", 0, 10)
	if err != nil {
		t.Fatalf("ListOrdered(unwritten namespace): %v", err)
	}
	if len(ordered.Records) != 0 || ordered.NextAfterOrder != 0 {
		t.Errorf("ListOrdered(unwritten namespace) = %#v, want an empty page", ordered)
	}

	name, err := orderedStreamName("sessions/empty")
	if err != nil {
		t.Fatalf("orderedStreamName: %v", err)
	}
	if _, err := seam.js.Stream(ctx, name); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Errorf("Stream(%q) = %v, want ErrStreamNotFound: a read must never provision a stream", name, err)
	}
}

// TestOrderedViewRefusesAForeignStream proves the layout gate runs on the READ
// path too: a stream occupying an ordered namespace's name that this layout did
// not provision fails queries with a typed error instead of being consumed as if
// it were ours.
func TestOrderedViewRefusesAForeignStream(t *testing.T) {
	store, seam := newOrderedTestStore(t)
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	ctx := testCtx(t)

	const namespace = "sessions/foreign"
	spec, err := orderedNamespaceSpec(namespace)
	if err != nil {
		t.Fatalf("orderedNamespaceSpec: %v", err)
	}
	config := orderedStreamConfig(spec)
	config.Description = "someone else's stream"
	if _, err := seam.js.CreateStream(ctx, config); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	// The stream must hold something. An EMPTY stream has a tip of zero, so
	// there is nothing for a view to catch up to and an empty page is the
	// correct answer whoever provisioned it — the layout only starts to matter
	// once the stream carries state a reader would otherwise be shown.
	subject, err := orderedCounterSubject(namespace, "tenant.a")
	if err != nil {
		t.Fatalf("orderedCounterSubject: %v", err)
	}
	if _, err := seam.js.Publish(ctx, subject, []byte("1")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err = store.ListDue(bounded, namespace, 100, "", 10)
	var configErr *OrderedStreamConfigError
	if !errors.As(err, &configErr) {
		t.Fatalf("ListDue(foreign stream) = %T %v, want *OrderedStreamConfigError", err, err)
	}
}

// TestOrderedStoreCloseStopsTheServerSideConsumer proves the lifecycle seam over
// a real server: Close ends the view goroutine and takes the ephemeral consumer
// with it, so a closed store leaves nothing running on the stream. F4.4's
// TestStoreCloseStopsOrderedView asserts the same seam from the composite.
func TestOrderedStoreCloseStopsTheServerSideConsumer(t *testing.T) {
	store, seam := newOrderedTestStore(t)
	ctx := testCtx(t)
	namespace := viewIntegrationID("", "").Namespace

	id := viewIntegrationID("tenant.a", "only")
	if _, _, err := store.Create(ctx, id, "catalog/main", []byte("only"),
		storage.Rank{Ranked: true, Value: 1}, storage.Due{State: storage.DueAt, UnixMillis: 1}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := store.ListDue(ctx, namespace, 100, "", 10); err != nil {
		t.Fatalf("ListDue: %v", err)
	}

	name, err := orderedStreamName(namespace)
	if err != nil {
		t.Fatalf("orderedStreamName: %v", err)
	}
	stream, err := seam.js.Stream(ctx, name)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.State.Consumers == 0 {
		t.Fatal("the warm view left no consumer on the stream")
	}

	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := store.ListDue(ctx, namespace, 100, "", 10); !errors.As(err, new(*OrderedStoreClosedError)) {
		t.Errorf("ListDue after Close = %T %v, want *OrderedStoreClosedError", err, err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		info, err = stream.Info(ctx)
		if err != nil {
			t.Fatalf("Info(after Close): %v", err)
		}
		if info.State.Consumers == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the stream still has %d consumers after Close", info.State.Consumers)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
