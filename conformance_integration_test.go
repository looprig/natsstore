//go:build integration

package natsstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/storage"
	"github.com/looprig/storage/storetest"
	"github.com/nats-io/nats.go/jetstream"
)

type orderedCursorProbe struct{}

func (orderedCursorProbe) MalformedCursor(t *testing.T, kind storage.OrderedCursorKind) string {
	t.Helper()
	return orderedMalformedCursorToken(kind)
}

func (orderedCursorProbe) UnknownVersionCursor(t *testing.T, kind storage.OrderedCursorKind) string {
	t.Helper()
	return orderedUnknownVersionCursorToken(kind)
}

func newOrderedBackend(t *testing.T, root string, counter *atomic.Uint64) *orderedStore {
	t.Helper()
	n := counter.Add(1)
	dir := filepath.Join(root, "oi"+strconv.FormatUint(n, 10), "jetstream")
	eng, err := OpenEngine(EngineOptions{DataDir: dir, SyncInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Open engine: %v", err)
	}
	jsx, err := jetstream.New(eng.Conn())
	if err != nil {
		_ = eng.Close()
		t.Fatalf("jetstream.New: %v", err)
	}
	index := newOrderedStore(newJetStreamOrderedSeam(eng.Conn(), jsx))
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := index.Close(closeCtx); err != nil {
			t.Errorf("close ordered index: %v", err)
		}
		if err := eng.Close(); err != nil {
			t.Errorf("close engine: %v", err)
		}
	})
	return index
}

func TestOrderedIndexConformance(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", root)
	var counter atomic.Uint64
	factory := func(t *testing.T) storage.OrderedIndex {
		return newOrderedBackend(t, root, &counter)
	}
	boundedWork := storetest.OrderedIndexCounterFunc(func(t *testing.T, ctx context.Context, index storage.OrderedIndex) {
		ordered := index.(*orderedStore)
		const records = 256
		const limit = 3
		for i := range records {
			id := storage.OrderedID{Namespace: "sessions", OrderingScope: "acceptance", StableKey: storage.StableKey("key-" + strconv.Itoa(i))}
			if _, created, err := ordered.Create(ctx, id, "workers", []byte("value"), storage.Rank{Ranked: true, Value: int64(i)}, storage.Due{State: storage.DueAt, UnixMillis: int64(i)}); err != nil || !created {
				t.Fatalf("Create(%s) = created %v, err %v", id.StableKey, created, err)
			}
		}
		if _, err := ordered.ListDue(ctx, "sessions", records-1, "", limit); err != nil {
			t.Fatalf("warm ListDue: %v", err)
		}
		before := ordered.queryStats()
		if _, err := ordered.ListDue(ctx, "sessions", records-1, "", limit); err != nil {
			t.Fatalf("measured ListDue: %v", err)
		}
		after := ordered.queryStats()
		if got := after.MessagesApplied - before.MessagesApplied; got != 0 {
			t.Errorf("warm ListDue applied %d stream messages, want 0", got)
		}
		if got := after.StreamTipReads - before.StreamTipReads; got != 1 {
			t.Errorf("warm ListDue read the stream tip %d times, want 1", got)
		}
		if got := after.RecordsCopied - before.RecordsCopied; got != limit {
			t.Errorf("warm ListDue copied %d records, want %d", got, limit)
		}
		if got := after.IndexVisited - before.IndexVisited; got > 48 {
			t.Errorf("warm ListDue visited %d index entries over %d records for limit %d, want at most 48", got, records, limit)
		}
	})
	storetest.TestOrderedIndex(t, factory, orderedCursorProbe{}, boundedWork)
}

// TestLedgerConformance runs the full storage ledger conformance suite against the
// JetStream-backed ledger over an embedded, in-process engine (no network). Every
// factory call stands up a FRESH engine on its own StoreDir (a unique subdirectory
// under the confined test root), so each subtest's backend is fully isolated and the
// ledger sees the caller's real, unmangled names — the suite asserts on
// ConflictError.Name/InvalidNameError.Name, so name rewriting is not an option. The
// suite reuses a logical name (e.g. "sessions/cas") across a subtest's inner cases;
// a fresh engine per case keeps those from colliding on one stream.
func TestLedgerConformance(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", root) // confinement root for every per-backend StoreDir
	var counter atomic.Uint64

	storetest.TestLedger(t, func(t *testing.T) storage.Ledger {
		n := counter.Add(1)
		dir := filepath.Join(root, "e"+strconv.FormatUint(n, 10), "jetstream")
		eng, err := OpenEngine(EngineOptions{DataDir: dir, SyncInterval: 50 * time.Millisecond})
		if err != nil {
			t.Fatalf("Open engine: %v", err)
		}
		t.Cleanup(func() { _ = eng.Close() })
		return newLedgerStore(newJetStreamSeam(eng.JetStream()))
	})
}

// newLeaserBackend stands up a fresh embedded engine on its own StoreDir, provisions a
// lease KV bucket over it, and returns a leaserStore over the production KV seam at the
// given application TTL. Each call is fully isolated (its own engine + bucket), so
// conformance subtests never collide on one bucket.
func newLeaserBackend(t *testing.T, root string, counter *atomic.Uint64, ttl time.Duration) *leaserStore {
	t.Helper()
	n := counter.Add(1)
	dir := filepath.Join(root, "l"+strconv.FormatUint(n, 10), "jetstream")
	eng, err := OpenEngine(EngineOptions{DataDir: dir, SyncInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Open engine: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	js, err := jetstream.New(eng.Conn())
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	kv, err := js.CreateKeyValue(ctx, leaseBucketConfig("looprig_leases", ttl))
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}
	return newLeaserStore(newJetStreamKVSeam(kv), ttl, time.Now)
}

// newKVBackend stands up a fresh embedded engine on its own StoreDir, provisions a KV
// bucket over it, and returns a kvStore over the production KV-store seam. Each call is
// fully isolated (its own engine + bucket), so conformance subtests never collide on one
// bucket and every revision counts from 1 — the suite asserts the first create returns
// rev 1. The bucket is DISTINCT from the lease bucket.
func newKVBackend(t *testing.T, root string, counter *atomic.Uint64) *kvStore {
	t.Helper()
	n := counter.Add(1)
	dir := filepath.Join(root, "kv"+strconv.FormatUint(n, 10), "jetstream")
	eng, err := OpenEngine(EngineOptions{DataDir: dir, SyncInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Open engine: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	js, err := jetstream.New(eng.Conn())
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	kv, err := js.CreateKeyValue(ctx, kvBucketConfig("looprig_kv"))
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}
	return newKVStore(newJetStreamKVStoreSeam(kv))
}

// TestKVConformance runs the full storage KV conformance suite against the JetStream-KV-
// backed store over an embedded, in-process engine (no network). Every factory call gets
// a FRESH engine + bucket on its own StoreDir, so each subtest is isolated, revisions
// start at 1, and the store sees the caller's real, unmangled keys (the suite asserts on
// KeyNotFoundError.Key / ConflictError.Name / InvalidNameError.Name). It exercises the
// 1 MiB value floor, revision-CAS conflicts leaving state unchanged, and sorted+deduped
// prefix listings.
func TestKVConformance(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", root)
	var counter atomic.Uint64

	storetest.TestKV(t, func(t *testing.T) storage.KV {
		return newKVBackend(t, root, &counter)
	})
}

// newBlobsBackend stands up a fresh embedded engine on its own StoreDir, provisions an
// ObjectStore over it, and returns a blobStore over the production object seam. Each call
// is fully isolated (its own engine + object store), so conformance subtests never collide
// on one store and the store sees the caller's real, unmangled keys.
func newBlobsBackend(t *testing.T, root string, counter *atomic.Uint64) *blobStore {
	t.Helper()
	n := counter.Add(1)
	dir := filepath.Join(root, "ob"+strconv.FormatUint(n, 10), "jetstream")
	eng, err := OpenEngine(EngineOptions{DataDir: dir, SyncInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Open engine: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	js, err := jetstream.New(eng.Conn())
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	obj, err := js.CreateObjectStore(ctx, objectStoreConfig("looprig_blobs"))
	if err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}
	return newBlobStore(newJetStreamObjectSeam(obj, js))
}

// TestBlobsConformance runs the full storage Blobs conformance suite against the
// JetStream-ObjectStore-backed store over an embedded, in-process engine (no network).
// Every factory call gets a FRESH engine + object store on its own StoreDir, so each
// subtest is isolated and the store sees the caller's real, unmangled keys (the suite
// asserts on BlobNotFoundError.Key / BlobConflictError.Key / InvalidNameError.Name). It
// exercises the 1 MiB blob floor, the content-addressed identical-no-op /
// different-conflict semantics (original untouched), and sorted+deduped prefix listings.
func TestBlobsConformance(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", root)
	var counter atomic.Uint64

	storetest.TestBlobs(t, func(t *testing.T) storage.Blobs {
		return newBlobsBackend(t, root, &counter)
	})
}

// TestBlobReaderCloseBoundWithMissingChunks is the provider-native nonvacuity
// proof for Blob reader shutdown. It leaves valid object metadata in place but
// purges the referenced chunks, forcing nats.go's ObjectResult.Read to wait on
// its network pipe. Close must wait behind that active Read, and both must return
// within the provider's advertised bound rather than the caller's later deadline.
func TestBlobReaderCloseBoundWithMissingChunks(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", root)
	eng, err := OpenEngine(EngineOptions{DataDir: filepath.Join(root, "jetstream"), SyncInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Open engine: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	js, err := jetstream.New(eng.Conn())
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	const bucket = "blob_close_bound"
	const key = "blobs/missing-chunks"
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	obj, err := js.CreateObjectStore(ctx, objectStoreConfig(bucket))
	if err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}
	info, err := obj.Put(ctx, jetstream.ObjectMeta{Name: key}, strings.NewReader("chunk payload"))
	if err != nil {
		t.Fatalf("ObjectStore.Put: %v", err)
	}
	stream, err := js.Stream(ctx, "OBJ_"+bucket)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	chunkSubject := "$O." + bucket + ".C." + info.NUID
	if err := stream.Purge(ctx, jetstream.WithPurgeSubject(chunkSubject)); err != nil {
		t.Fatalf("Purge chunks: %v", err)
	}

	store := newBlobStore(newJetStreamObjectSeam(obj, js))
	timeoutReader, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get timeout reader: %v", err)
	}
	timeoutStarted := time.Now()
	_, timeoutErr := timeoutReader.Read(make([]byte, 1))
	timeoutElapsed := time.Since(timeoutStarted)
	_ = timeoutReader.Close()
	if timeoutErr == nil || timeoutElapsed < 4*time.Second || timeoutElapsed > store.BlobReaderCloseBound() {
		t.Fatalf("blocked Read = %v after %v; want terminal error in [4s,%v]", timeoutErr, timeoutElapsed, store.BlobReaderCloseBound())
	}
	reader, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	started := make(chan struct{})
	readDone := make(chan error, 1)
	go func() {
		close(started)
		_, readErr := reader.Read(make([]byte, 1))
		readDone <- readErr
	}()
	<-started
	blockedWindow := time.NewTimer(100 * time.Millisecond)
	select {
	case readErr := <-readDone:
		blockedWindow.Stop()
		t.Fatalf("Read returned before Close against missing chunks: %v", readErr)
	case <-blockedWindow.C:
	}

	startedAt := time.Now()
	closeDone := make(chan error, 1)
	go func() { closeDone <- reader.Close() }()
	bound := store.BlobReaderCloseBound()
	deadline := time.NewTimer(bound)
	defer deadline.Stop()
	var readErr, closeErr error
	for readDone != nil || closeDone != nil {
		select {
		case readErr = <-readDone:
			readDone = nil
		case closeErr = <-closeDone:
			closeDone = nil
		case <-deadline.C:
			t.Fatalf("Read/Close exceeded advertised %v bound", bound)
		}
	}
	elapsed := time.Since(startedAt)
	if elapsed > bound {
		t.Fatalf("Read/Close took %v, exceeded advertised %v bound", elapsed, bound)
	}
	if readErr == nil || closeErr != nil {
		t.Fatalf("Read/Close = %v / %v, want terminal Read error and nil Close", readErr, closeErr)
	}
	for i := 0; i < 3; i++ {
		if n, postErr := reader.Read(make([]byte, 1)); n != 0 || postErr == nil || errors.Is(postErr, io.EOF) {
			t.Fatalf("post-Close Read %d = %d, %v; want non-EOF terminal error", i, n, postErr)
		}
	}
}

// TestBlobReaderAllowsProgressBeyondDefaultReadDeadline proves that the
// provider's per-Read shutdown bound is not an absolute ObjectResult lifetime.
// A reader that keeps consuming chunks may remain open longer than five seconds
// and still complete digest verification successfully.
func TestBlobReaderAllowsProgressBeyondDefaultReadDeadline(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", root)
	eng, err := OpenEngine(EngineOptions{DataDir: filepath.Join(root, "jetstream"), SyncInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Open engine: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	js, err := jetstream.New(eng.Conn())
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	obj, err := js.CreateObjectStore(context.Background(), objectStoreConfig("blob_slow_reader"))
	if err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}

	const chunkSize = 128 * 1024
	want := bytes.Repeat([]byte("slow-reader-data"), 64*1024)
	meta := jetstream.ObjectMeta{Name: "blobs/slow", Opts: &jetstream.ObjectMetaOptions{ChunkSize: chunkSize}}
	if _, err := obj.Put(context.Background(), meta, bytes.NewReader(want)); err != nil {
		t.Fatalf("ObjectStore.Put: %v", err)
	}
	store := newBlobStore(newJetStreamObjectSeam(obj, js))
	reader, err := store.Get(context.Background(), "blobs/slow")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer reader.Close()

	started := time.Now()
	var got bytes.Buffer
	buf := make([]byte, chunkSize)
	for {
		n, readErr := reader.Read(buf)
		got.Write(buf[:n])
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			t.Fatalf("Read after %v: %v", time.Since(started), readErr)
		}
		time.Sleep(750 * time.Millisecond)
	}
	if elapsed := time.Since(started); elapsed <= blobObjectReadTimeout {
		t.Fatalf("read completed in %v; want nonvacuous duration beyond old %v total cap", elapsed, blobObjectReadTimeout)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("read %d bytes, want %d identical bytes", got.Len(), len(want))
	}
}

func TestBlobReaderFollowsObjectLinks(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", root)
	eng, err := OpenEngine(EngineOptions{DataDir: filepath.Join(root, "jetstream"), SyncInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	js, err := jetstream.New(eng.Conn())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rootStore, err := js.CreateObjectStore(ctx, objectStoreConfig("blob_link_root"))
	if err != nil {
		t.Fatal(err)
	}
	targetStore, err := js.CreateObjectStore(ctx, objectStoreConfig("blob_link_target"))
	if err != nil {
		t.Fatal(err)
	}
	target, err := targetStore.PutString(ctx, "target", "linked payload")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rootStore.AddLink(ctx, "cross-link", target); err != nil {
		t.Fatal(err)
	}
	local, err := rootStore.PutString(ctx, "local", "local payload")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rootStore.AddLink(ctx, "same-link", local); err != nil {
		t.Fatal(err)
	}
	if _, err := rootStore.AddBucketLink(ctx, "bucket-link", targetStore); err != nil {
		t.Fatal(err)
	}
	store := newBlobStore(newJetStreamObjectSeam(rootStore, js))
	for key, want := range map[string]string{"cross-link": "linked payload", "same-link": "local payload"} {
		reader, getErr := store.Get(ctx, key)
		if getErr != nil {
			t.Fatalf("Get(%q): %v", key, getErr)
		}
		got, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil || string(got) != want {
			t.Fatalf("Get(%q) = %q, %v, Close %v; want %q", key, got, readErr, closeErr, want)
		}
	}
	if _, err := store.Get(ctx, "bucket-link"); !errors.Is(err, jetstream.ErrCantGetBucket) {
		t.Fatalf("Get(bucket-link) = %v, want ErrCantGetBucket", err)
	}
}

// TestLeaserConformance runs the full storage Leaser conformance suite against the
// JetStream-KV-backed leaser over an embedded, in-process engine (no network). Every
// factory call gets a FRESH engine + bucket on its own StoreDir, so each subtest is
// isolated and the leaser sees the caller's real, unmangled names (the suite asserts on
// LeaseHeldError.Name / InvalidNameError.Name). The default lease TTL is used: it is far
// longer than a suite run, so no heartbeat fires mid-test and Release drives every
// re-acquire.
func TestLeaserConformance(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", root)
	var counter atomic.Uint64

	storetest.TestLeaser(t, func(t *testing.T) storage.Leaser {
		return newLeaserBackend(t, root, &counter, defaultLeaseTTL)
	})
}

// TestLeaserReclaimAfterTTLExpiry is the natsstore-specific per-host liveness proof: a
// holder that dies (its heartbeat stops) without releasing leaves an entry that ages
// past its application-level ExpiresAt, after which a fresh Acquire reclaims the name at
// a strictly higher epoch. It uses a short TTL and real wall-clock (production wires
// time.Now — there is no injected clock on the public Leaser), simulating death by
// stopping A's heartbeat, then waiting past the TTL.
func TestLeaserReclaimAfterTTLExpiry(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", root)
	var counter atomic.Uint64

	const shortTTL = 200 * time.Millisecond
	le := newLeaserBackend(t, root, &counter, shortTTL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const name = "sessions/reclaim"
	a, err := le.Acquire(ctx, name)
	if err != nil {
		t.Fatalf("Acquire A: %v", err)
	}
	epochA := a.Epoch()

	// Simulate A's death: stop its heartbeat so ExpiresAt is no longer renewed, WITHOUT
	// releasing (a released lease would relinquish the entry cleanly — that is not death).
	a.(*kvLease).stopHeartbeatForTest()

	// Wait comfortably past the TTL so the stored ExpiresAt reads expired.
	time.Sleep(shortTTL * 3)

	b, err := le.Acquire(ctx, name)
	if err != nil {
		t.Fatalf("Acquire B after TTL expiry: %v", err)
	}
	defer func() { _ = b.Release(ctx) }()
	if b.Epoch() <= epochA {
		t.Errorf("reclaimed epoch = %d, want strictly > dead holder's epoch %d", b.Epoch(), epochA)
	}

	// Sanity: the reclaimed lease is live (Lost open) and re-holds the name — a second
	// Acquire while B holds must now be refused.
	if isChanClosed(b.Lost()) {
		t.Error("reclaimed lease B.Lost() is closed, want open (live)")
	}
	_, err = le.Acquire(ctx, name)
	var held *storage.LeaseHeldError
	if !errors.As(err, &held) {
		t.Fatalf("Acquire while B holds = %v, want *LeaseHeldError", err)
	}
	if held.HolderEpoch != b.Epoch() {
		t.Errorf("LeaseHeldError.HolderEpoch = %d, want %d", held.HolderEpoch, b.Epoch())
	}
}
