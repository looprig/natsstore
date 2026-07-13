//go:build integration

package natsstore

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/looprig/storage"
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
		t.Fatalf("Symlink(%q, %q): %v", realParent, linkedParent, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	st, err := Open(ctx, Options{EmbeddedDir: filepath.Join(linkedParent, "jetstream")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close(ctx) })

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
// env) — stands up an owned embedded engine and wires all four primitives, each of which
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
	t.Cleanup(func() { _ = st2.Close(ctx) })
	tip, err := st2.Ledger.Tip(ctx, "sessions/main")
	if err != nil {
		t.Fatalf("reopen Ledger.Tip: %v", err)
	}
	if tip != 1 {
		t.Errorf("reopen ledger tip = %d, want 1 (StoreDir not durable)", tip)
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
