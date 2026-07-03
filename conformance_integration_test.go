//go:build integration

package natsstore

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ciram-co/storekit"
	"github.com/ciram-co/storekit/storetest"
	"github.com/nats-io/nats.go/jetstream"
)

// TestLedgerConformance runs the full storekit ledger conformance suite against the
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

	storetest.TestLedger(t, func(t *testing.T) storekit.Ledger {
		n := counter.Add(1)
		dir := filepath.Join(root, "e"+strconv.FormatUint(n, 10), "jetstream")
		eng, err := Open(EngineOptions{DataDir: dir, SyncInterval: 50 * time.Millisecond})
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
	eng, err := Open(EngineOptions{DataDir: dir, SyncInterval: 50 * time.Millisecond})
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
	eng, err := Open(EngineOptions{DataDir: dir, SyncInterval: 50 * time.Millisecond})
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

// TestKVConformance runs the full storekit KV conformance suite against the JetStream-KV-
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

	storetest.TestKV(t, func(t *testing.T) storekit.KV {
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
	eng, err := Open(EngineOptions{DataDir: dir, SyncInterval: 50 * time.Millisecond})
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
	return newBlobStore(newJetStreamObjectSeam(obj))
}

// TestBlobsConformance runs the full storekit Blobs conformance suite against the
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

	storetest.TestBlobs(t, func(t *testing.T) storekit.Blobs {
		return newBlobsBackend(t, root, &counter)
	})
}

// TestLeaserConformance runs the full storekit Leaser conformance suite against the
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

	storetest.TestLeaser(t, func(t *testing.T) storekit.Leaser {
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
	var held *storekit.LeaseHeldError
	if !errors.As(err, &held) {
		t.Fatalf("Acquire while B holds = %v, want *LeaseHeldError", err)
	}
	if held.HolderEpoch != b.Epoch() {
		t.Errorf("LeaseHeldError.HolderEpoch = %d, want %d", held.HolderEpoch, b.Epoch())
	}
}
