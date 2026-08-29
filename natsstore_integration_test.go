//go:build integration

package natsstore

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/looprig/storage"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestOpenEmbeddedReportsCanonicalStoragePath(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	realParent := filepath.Join(root, "real")
	if err := os.Mkdir(realParent, 0o700); err != nil {
		t.Fatalf("Mkdir(%q): %v", realParent, err)
	}
	linkedParent := filepath.Join(root, "linked")
	if err := os.Symlink(realParent, linkedParent); err != nil {
		if runtime.GOOS == "windows" || errors.Is(err, os.ErrPermission) {
			t.Skipf("symlink capability unavailable: %v", err)
		}
		t.Fatalf("Symlink(%q, %q): %v", realParent, linkedParent, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	st, err := Open(ctx, Options{EmbeddedDir: filepath.Join(linkedParent, "jetstream")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = st.Close(cleanupCtx)
	})

	reporters := []struct {
		name     string
		reporter storage.PathReporter
	}{{name: "store", reporter: st}}
	for _, candidate := range []struct {
		name     string
		reporter storage.PathReporter
	}{
		{name: "ledger", reporter: st.Ledger.(storage.PathReporter)},
		{name: "leaser", reporter: st.Leaser.(storage.PathReporter)},
		{name: "kv", reporter: st.KV.(storage.PathReporter)},
		{name: "blobs", reporter: st.Blobs.(storage.PathReporter)},
		{name: "ordered-index", reporter: st.OrderedIndex.(storage.PathReporter)},
	} {
		reporters = append(reporters, candidate)
	}

	canonical, err := filepath.EvalSymlinks(filepath.Join(realParent, "jetstream"))
	if err != nil {
		t.Fatalf("EvalSymlinks(expected path): %v", err)
	}
	want := []string{canonical}
	first := reporters[0].reporter.StoragePaths()
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("store StoragePaths() = %v, want %v", first, want)
	}
	first[0] = filepath.Join(root, "mutated")

	for _, reporter := range reporters {
		t.Run(reporter.name, func(t *testing.T) {
			t.Parallel()
			if got := reporter.reporter.StoragePaths(); !reflect.DeepEqual(got, want) {
				t.Errorf("StoragePaths() = %v, want %v", got, want)
			}
		})
	}
}

// TestOpenEmbeddedRoundtrip is the public-entry-point end-to-end proof: Open on a PLAIN
// t.TempDir() — deliberately NOT under $XDG_DATA_HOME and NOT under $HOME (we set no XDG
// env) — stands up an owned embedded engine and wires all five primitives, each of which
// round-trips through the returned Store. This is the containment-lift proof: OpenEngine
// would reject this dir (it escapes the home/XDG root), but Open's embedded mode uses the
// caller-owned dir directly. It then proves Close is idempotent and that reopening the
// SAME dir sees the persisted ledger record (durability + idempotent bucket rebind).
func TestOpenEmbeddedRoundtrip(t *testing.T) {
	dir := t.TempDir() // absolute, under the OS temp root — outside home/XDG on purpose

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	st, err := Open(ctx, Options{EmbeddedDir: dir})
	if err != nil {
		t.Fatalf("Open(EmbeddedDir=%q): %v", dir, err)
	}

	exerciseLedger(ctx, t, st)
	exerciseKV(ctx, t, st)
	exerciseBlobs(ctx, t, st)
	exerciseLeaser(ctx, t, st)
	exerciseOrderedIndex(ctx, t, st)

	if err := st.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := st.Close(ctx); err != nil {
		t.Fatalf("second Close (want idempotent nil): %v", err)
	}

	// Reopen the same StoreDir: the ledger record must persist (durable StoreDir) and the
	// buckets must rebind (CreateOrUpdate, not a fresh Create that would fail).
	st2, err := Open(ctx, Options{EmbeddedDir: dir})
	if err != nil {
		t.Fatalf("Open (reopen): %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = st2.Close(cleanupCtx)
	})
	tip, err := st2.Ledger.Tip(ctx, "sessions/main")
	if err != nil {
		t.Fatalf("reopen Ledger.Tip: %v", err)
	}
	if tip != 1 {
		t.Errorf("reopen ledger tip = %d, want 1 (StoreDir not durable)", tip)
	}
	id := storage.OrderedID{Namespace: "sessions", OrderingScope: "acceptance", StableKey: "command/A:B_1"}
	reopened, err := st2.OrderedIndex.Get(ctx, id)
	if err != nil {
		t.Fatalf("reopen OrderedIndex.Get: %v", err)
	}
	if reopened.Order == 0 || string(reopened.Value) != "ordered-value" {
		t.Errorf("reopened ordered record = {Order:%d Value:%q}, want nonzero order and %q", reopened.Order, reopened.Value, "ordered-value")
	}
}

func TestRemoteWireStoreRetainsOrderedLifecycle(t *testing.T) {
	eng, err := openEngineAt(t.TempDir(), 0, maxPayload)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	conn, err := nats.Connect("", nats.InProcessServer(eng.srv), nats.Name("natsstore-remote-wire-test"))
	if err != nil {
		t.Fatalf("connect independent client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// This is openRemote's complete post-connect path over an independent NATS
	// client, without requiring a separately managed external server.
	st, err := wireStore(ctx, conn, newLocalPathReporter(), conn.Drain)
	if err != nil {
		conn.Close()
		t.Fatalf("wireStore(remote connection): %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = st.Close(cleanupCtx)
	})
	if st.ordered == nil || st.OrderedIndex != st.ordered {
		t.Fatal("remote wire path did not retain the Composite's ordered lifecycle owner")
	}
	if paths := st.StoragePaths(); len(paths) != 0 {
		t.Fatalf("remote StoragePaths = %v, want none", paths)
	}
	if _, err := st.OrderedIndex.ListDue(ctx, "remote-empty", 100, "", 1); err != nil {
		t.Fatalf("ListDue(remote unwritten namespace): %v", err)
	}
	st.ordered.mu.Lock()
	view := st.ordered.views["remote-empty"]
	st.ordered.mu.Unlock()
	if view == nil {
		t.Fatal("remote query did not retain its namespace view")
	}
	if err := st.Close(ctx); err != nil {
		t.Fatalf("Close(remote): %v", err)
	}
	select {
	case <-view.done:
	default:
		t.Error("remote ordered view outlived Store.Close")
	}
	// Store owns only the independent connection; its backing engine remains live.
	if !eng.Conn().IsConnected() {
		t.Error("closing the remote-shaped Store closed the backing engine connection")
	}
}

func exerciseOrderedIndex(ctx context.Context, t *testing.T, st *Store) {
	t.Helper()
	if st.OrderedIndex == nil {
		t.Fatal("Open returned a composite with nil OrderedIndex")
	}
	id := storage.OrderedID{Namespace: "sessions", OrderingScope: "acceptance", StableKey: "command/A:B_1"}
	record, created, err := st.OrderedIndex.Create(ctx, id, "workers", []byte("ordered-value"), storage.Rank{Ranked: true, Value: 7}, storage.Due{State: storage.DueAt, UnixMillis: 9})
	if err != nil || !created {
		t.Fatalf("OrderedIndex.Create = created %v, err %v", created, err)
	}
	page, err := st.OrderedIndex.ListOrdered(ctx, "sessions", "acceptance", 0, 1)
	if err != nil {
		t.Fatalf("OrderedIndex.ListOrdered: %v", err)
	}
	if len(page.Records) != 1 || page.Records[0].ID != id || page.Records[0].Order != record.Order {
		t.Errorf("OrderedIndex.ListOrdered = %+v, want created record %+v", page.Records, record)
	}
}

func TestOrderedIndexMultiHandleConcurrentOrder(t *testing.T) {
	dir := t.TempDir()
	eng, err := openEngineAt(dir, 0, maxPayload)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	leftJS, err := jetstream.New(eng.Conn())
	if err != nil {
		_ = eng.Close()
		t.Fatalf("jetstream.New: %v", err)
	}
	rightConn, err := nats.Connect("", nats.InProcessServer(eng.srv), nats.Name("natsstore-multi-handle-right"))
	if err != nil {
		_ = eng.Close()
		t.Fatalf("connect second client: %v", err)
	}
	rightJS, err := jetstream.New(rightConn)
	if err != nil {
		rightConn.Close()
		_ = eng.Close()
		t.Fatalf("jetstream.New(second client): %v", err)
	}
	left := newOrderedStore(newJetStreamOrderedSeam(eng.Conn(), leftJS))
	right := newOrderedStore(newJetStreamOrderedSeam(rightConn, rightJS))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = left.Close(cleanupCtx)
		_ = right.Close(cleanupCtx)
		rightConn.Close()
		_ = eng.Close()
	})

	const writers = 100
	type result struct {
		record storage.OrderedRecord
		err    error
	}
	results := make(chan result, writers)
	start := make(chan struct{})
	for i := range writers {
		index := left
		if i%2 != 0 {
			index = right
		}
		go func(i int, index *orderedStore) {
			<-start
			id := storage.OrderedID{Namespace: "sessions", OrderingScope: "acceptance", StableKey: storage.StableKey("multi-" + strconv.Itoa(i))}
			record, created, err := index.Create(ctx, id, "workers", []byte("value"), storage.Rank{}, storage.Due{State: storage.NotDue})
			if err == nil && !created {
				err = errors.New("distinct create reported created=false")
			}
			results <- result{record: record, err: err}
		}(i, index)
	}
	close(start)
	orders := make(map[uint64]struct{}, writers)
	for range writers {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent Create: %v", result.err)
		}
		if result.record.Order == 0 {
			t.Fatal("concurrent Create returned zero order")
		}
		if _, duplicate := orders[result.record.Order]; duplicate {
			t.Fatalf("duplicate order %d across independent handles", result.record.Order)
		}
		orders[result.record.Order] = struct{}{}
	}
	page, err := left.ListOrdered(ctx, "sessions", "acceptance", 0, writers)
	if err != nil {
		t.Fatalf("ListOrdered: %v", err)
	}
	if len(page.Records) != writers {
		t.Fatalf("ListOrdered returned %d records, want %d", len(page.Records), writers)
	}
	for i := 1; i < len(page.Records); i++ {
		if page.Records[i].Order <= page.Records[i-1].Order {
			t.Fatalf("orders are not strictly increasing at %d: %d then %d", i, page.Records[i-1].Order, page.Records[i].Order)
		}
	}
}

// TestStoreCloseStopsOrderedView proves a Store returned by public Open owns its
// derived OrderedIndex views: Close cancels their subscriptions, waits for each
// done signal, refuses later queries, and remains idempotent.
func TestStoreCloseStopsOrderedView(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := Open(ctx, Options{EmbeddedDir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := st.Close(cleanupCtx); err != nil {
			t.Errorf("cleanup Close: %v", err)
		}
	})
	if st.ordered == nil {
		t.Fatal("Open did not retain the ordered lifecycle owner")
	}
	ordered := st.ordered

	id := storage.OrderedID{Namespace: "sessions", OrderingScope: "acceptance", StableKey: "one"}
	if _, created, err := st.OrderedIndex.Create(ctx, id, "workers", []byte("value"), storage.Rank{}, storage.Due{State: storage.NotDue}); err != nil || !created {
		t.Fatalf("Create = created %v, err %v", created, err)
	}
	if _, err := st.OrderedIndex.ListDue(ctx, "sessions", 100, "", 1); err != nil {
		t.Fatalf("ListDue: %v", err)
	}
	// A read of an unwritten namespace starts the same retrying subscription
	// lifecycle even though its stream has not been provisioned yet.
	if page, err := st.OrderedIndex.ListDue(ctx, "empty", 100, "", 1); err != nil || len(page.Records) != 0 {
		t.Fatalf("ListDue(unwritten namespace) = records %d, err %v; want empty page, nil", len(page.Records), err)
	}
	ordered.mu.Lock()
	views := make([]*orderedView, 0, len(ordered.views))
	for _, view := range ordered.views {
		views = append(views, view)
	}
	ordered.mu.Unlock()
	if len(views) != 2 {
		t.Fatalf("before Close: views=%d, want 2", len(views))
	}

	if err := st.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, view := range views {
		select {
		case <-view.done:
		default:
			t.Errorf("view %q done is open after Store.Close returned", view.namespace)
		}
	}
	if _, err := ordered.ListDue(ctx, "sessions", 100, "", 1); !errors.As(err, new(*OrderedStoreClosedError)) {
		t.Errorf("ListDue after Close = %T %v, want *OrderedStoreClosedError", err, err)
	}
	if err := st.Close(ctx); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
}

// exerciseLedger appends a record, reads it back through a bounded cursor, and checks the
// tip — the ledger's Append/Read/Tip contract.
func exerciseLedger(ctx context.Context, t *testing.T, st *Store) {
	t.Helper()
	const name = "sessions/main"
	if err := st.Ledger.Append(ctx, name, 0, []byte("rec-1")); err != nil {
		t.Fatalf("Ledger.Append: %v", err)
	}
	tip, err := st.Ledger.Tip(ctx, name)
	if err != nil {
		t.Fatalf("Ledger.Tip: %v", err)
	}
	if tip != 1 {
		t.Fatalf("Ledger.Tip = %d, want 1", tip)
	}
	cur, err := st.Ledger.Read(ctx, name, 1)
	if err != nil {
		t.Fatalf("Ledger.Read: %v", err)
	}
	defer func() { _ = cur.Close() }()
	rec, err := cur.Next(ctx)
	if err != nil {
		t.Fatalf("Cursor.Next: %v", err)
	}
	if rec.Seq != 1 || string(rec.Payload) != "rec-1" {
		t.Errorf("record = {Seq:%d, Payload:%q}, want {1, %q}", rec.Seq, rec.Payload, "rec-1")
	}
	if _, err := cur.Next(ctx); !errors.Is(err, io.EOF) {
		t.Errorf("second Next = %v, want io.EOF", err)
	}
}

// exerciseKV creates a key, reads it back at revision 1, and lists it by prefix — the KV
// store's Put/Get/Keys contract.
func exerciseKV(ctx context.Context, t *testing.T, st *Store) {
	t.Helper()
	const key = "catalog/a"
	rev, err := st.KV.Put(ctx, key, 0, []byte("v1"))
	if err != nil {
		t.Fatalf("KV.Put: %v", err)
	}
	if rev != 1 {
		t.Fatalf("KV.Put rev = %d, want 1", rev)
	}
	val, gotRev, err := st.KV.Get(ctx, key)
	if err != nil {
		t.Fatalf("KV.Get: %v", err)
	}
	if string(val) != "v1" || gotRev != 1 {
		t.Errorf("KV.Get = (%q, %d), want (%q, 1)", val, gotRev, "v1")
	}
	keys, err := st.KV.Keys(ctx, "catalog/")
	if err != nil {
		t.Fatalf("KV.Keys: %v", err)
	}
	if len(keys) != 1 || keys[0] != key {
		t.Errorf("KV.Keys = %v, want [%q]", keys, key)
	}
}

// exerciseBlobs puts content-addressed bytes, reads them back, and lists by prefix — the
// blob store's Put/Get/List contract.
func exerciseBlobs(ctx context.Context, t *testing.T, st *Store) {
	t.Helper()
	const key = "blobs/x"
	if err := st.Blobs.Put(ctx, key, strings.NewReader("some-bytes")); err != nil {
		t.Fatalf("Blobs.Put: %v", err)
	}
	rc, err := st.Blobs.Get(ctx, key)
	if err != nil {
		t.Fatalf("Blobs.Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}
	if string(got) != "some-bytes" {
		t.Errorf("blob = %q, want %q", got, "some-bytes")
	}
	names, err := st.Blobs.List(ctx, "blobs/")
	if err != nil {
		t.Fatalf("Blobs.List: %v", err)
	}
	if len(names) != 1 || names[0] != key {
		t.Errorf("Blobs.List = %v, want [%q]", names, key)
	}
}

// exerciseLeaser acquires a lease, proves a second acquire is refused while it is held,
// then releases it — the leaser's Acquire/Release + single-holder contract.
func exerciseLeaser(ctx context.Context, t *testing.T, st *Store) {
	t.Helper()
	const name = "locks/main"
	lease, err := st.Leaser.Acquire(ctx, name)
	if err != nil {
		t.Fatalf("Leaser.Acquire: %v", err)
	}
	if lease.Epoch() < 1 {
		t.Errorf("lease epoch = %d, want >= 1", lease.Epoch())
	}
	if _, err := st.Leaser.Acquire(ctx, name); !errors.As(err, new(*storage.LeaseHeldError)) {
		t.Errorf("second Acquire while held = %v, want *LeaseHeldError", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("Lease.Release: %v", err)
	}
}
