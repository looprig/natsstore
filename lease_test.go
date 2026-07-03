package natsstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/looprig/storekit"
)

// fakeKVSeam is a stateful, in-memory kvLeaseSeam for unit-testing leaserStore with
// no running server. It models JetStream KV's create-if-absent / revision-CAS-update
// semantics exactly (per-key monotonic revision, ErrKeyExists on create-over-present,
// wrong-revision-conflict on update), so a test can drive real acquire->release->acquire
// cycles and assert epoch monotonicity end to end. It is mutex-guarded because Acquire
// launches a heartbeat goroutine that may call update concurrently.
type fakeKVSeam struct {
	mu      sync.Mutex
	store   map[string]fakeKVEntry
	nextRev uint64

	// Call counters (guarded by mu) so a test can assert an operation happened exactly
	// N times — notably that double-Release performs ZERO extra backend writes.
	createCalls int
	updateCalls int
	getCalls    int

	// forceGetErr, when non-nil, makes get return it (a non-not-found backend error)
	// so a test can exercise the fail-closed *LeaseOpError path on an ambiguous read.
	forceGetErr error
	// getVal/getRev override get to return an arbitrary stored value (used to inject a
	// malformed record without going through create).
	getOverride *fakeKVEntry
}

type fakeKVEntry struct {
	val []byte
	rev uint64
}

func newFakeKVSeam() *fakeKVSeam {
	return &fakeKVSeam{store: make(map[string]fakeKVEntry)}
}

func (f *fakeKVSeam) create(_ context.Context, key string, val []byte) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	if _, ok := f.store[key]; ok {
		return 0, errKVKeyExists
	}
	f.nextRev++
	f.store[key] = fakeKVEntry{val: append([]byte(nil), val...), rev: f.nextRev}
	return f.nextRev, nil
}

func (f *fakeKVSeam) get(_ context.Context, key string) ([]byte, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	if f.forceGetErr != nil {
		return nil, 0, f.forceGetErr
	}
	if f.getOverride != nil {
		return f.getOverride.val, f.getOverride.rev, nil
	}
	e, ok := f.store[key]
	if !ok {
		return nil, 0, errKVKeyNotFound
	}
	return e.val, e.rev, nil
}

func (f *fakeKVSeam) update(_ context.Context, key string, val []byte, expectedRev uint64) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateCalls++
	e, ok := f.store[key]
	if !ok {
		return 0, errKVKeyNotFound
	}
	if e.rev != expectedRev {
		return 0, errKVCASConflict
	}
	f.nextRev++
	f.store[key] = fakeKVEntry{val: append([]byte(nil), val...), rev: f.nextRev}
	return f.nextRev, nil
}

// testClock is an advanceable clock for deterministic expiry tests.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// unitTTL is comfortably larger than any unit test's wall-clock duration, so the
// heartbeat ticker (interval ttl/3) never fires mid-test; expiry is driven by the
// injected testClock, not real time. Every acquired lease is Release'd to stop its
// heartbeat goroutine.
const unitTTL = 30 * time.Second

func TestLeaserAcquireEpochAndErrors(t *testing.T) {
	t.Parallel()
	const name = "sessions/lease"
	ctx := context.Background()

	t.Run("absent grants epoch 1", func(t *testing.T) {
		t.Parallel()
		clk := newTestClock()
		s := newLeaserStore(newFakeKVSeam(), unitTTL, clk.Now)
		l, err := s.Acquire(ctx, name)
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		defer func() { _ = l.Release(ctx) }()
		if l.Epoch() != 1 {
			t.Errorf("Epoch() = %d, want 1", l.Epoch())
		}
		if isChanClosed(l.Lost()) {
			t.Error("Lost() closed on a fresh grant")
		}
	})

	t.Run("live holder refuses with LeaseHeldError", func(t *testing.T) {
		t.Parallel()
		clk := newTestClock()
		s := newLeaserStore(newFakeKVSeam(), unitTTL, clk.Now)
		first, err := s.Acquire(ctx, name)
		if err != nil {
			t.Fatalf("first Acquire: %v", err)
		}
		defer func() { _ = first.Release(ctx) }()

		_, err = s.Acquire(ctx, name) // same (fixed) clock -> first is still live
		var held *storekit.LeaseHeldError
		if !errors.As(err, &held) {
			t.Fatalf("second Acquire = %v, want *LeaseHeldError", err)
		}
		if held.Name != name || held.HolderEpoch != first.Epoch() {
			t.Errorf("LeaseHeldError = {Name:%q, HolderEpoch:%d}, want {%q, %d}",
				held.Name, held.HolderEpoch, name, first.Epoch())
		}
	})

	t.Run("expired holder is taken over at higher epoch", func(t *testing.T) {
		t.Parallel()
		clk := newTestClock()
		s := newLeaserStore(newFakeKVSeam(), unitTTL, clk.Now)
		a, err := s.Acquire(ctx, name)
		if err != nil {
			t.Fatalf("Acquire A: %v", err)
		}
		// Simulate A's death: stop its heartbeat so its ExpiresAt is not renewed,
		// then advance the clock past the TTL so the stored entry reads expired.
		a.(*kvLease).stopHeartbeatForTest()
		clk.advance(unitTTL * 2)

		b, err := s.Acquire(ctx, name)
		if err != nil {
			t.Fatalf("Acquire B after expiry: %v", err)
		}
		defer func() { _ = b.Release(ctx) }()
		if b.Epoch() <= a.Epoch() {
			t.Errorf("B.Epoch() = %d, want > A.Epoch() %d", b.Epoch(), a.Epoch())
		}
	})

	t.Run("ambiguous read fails closed with LeaseOpError", func(t *testing.T) {
		t.Parallel()
		boom := errors.New("kv backend unavailable")
		f := newFakeKVSeam()
		f.forceGetErr = boom
		s := newLeaserStore(f, unitTTL, newTestClock().Now)
		_, err := s.Acquire(ctx, name)
		var op *LeaseOpError
		if !errors.As(err, &op) {
			t.Fatalf("Acquire = %v, want *LeaseOpError (fail closed)", err)
		}
		var held *storekit.LeaseHeldError
		if errors.As(err, &held) {
			t.Error("an ambiguous read was mis-classified as LeaseHeldError")
		}
		if !errors.Is(err, boom) {
			t.Error("LeaseOpError does not unwrap to the backend cause")
		}
	})

	t.Run("malformed record fails closed with LeaseOpError", func(t *testing.T) {
		t.Parallel()
		f := newFakeKVSeam()
		f.getOverride = &fakeKVEntry{val: []byte("{not json"), rev: 7}
		s := newLeaserStore(f, unitTTL, newTestClock().Now)
		_, err := s.Acquire(ctx, name)
		var op *LeaseOpError
		if !errors.As(err, &op) {
			t.Fatalf("Acquire = %v, want *LeaseOpError on malformed record", err)
		}
	})
}

func TestLeaserEpochMonotonicAcrossReleaseGrant(t *testing.T) {
	t.Parallel()
	const name = "sessions/mono"
	ctx := context.Background()
	clk := newTestClock() // fixed clock: Release relinquishes as expired-now, so the
	// next Acquire sees an expired entry and bumps the epoch — no clock advance needed.
	s := newLeaserStore(newFakeKVSeam(), unitTTL, clk.Now)

	var last uint64
	for i := 0; i < 4; i++ {
		l, err := s.Acquire(ctx, name)
		if err != nil {
			t.Fatalf("Acquire #%d: %v", i, err)
		}
		if l.Epoch() <= last {
			t.Fatalf("grant #%d Epoch() = %d, want strictly > %d", i, l.Epoch(), last)
		}
		last = l.Epoch()
		if err := l.Release(ctx); err != nil {
			t.Fatalf("Release #%d: %v", i, err)
		}
	}
	if last < 4 {
		t.Errorf("final epoch = %d, want >= 4 across four grant/release cycles", last)
	}
}

func TestLeaserAcquireInvalidName(t *testing.T) {
	t.Parallel()
	badNames := []struct {
		label string
		value string
	}{
		{"empty", ""},
		{"leading slash", "/leading"},
		{"trailing slash", "trailing/"},
		{"doubled slash", "a//b"},
		{"uppercase", "Upper"},
		{"space", "has space"},
		{"dot-dot segment", ".."},
	}
	for _, bad := range badNames {
		t.Run(bad.label, func(t *testing.T) {
			t.Parallel()
			f := newFakeKVSeam()
			s := newLeaserStore(f, unitTTL, newTestClock().Now)
			_, err := s.Acquire(context.Background(), bad.value)
			var ine *storekit.InvalidNameError
			if !errors.As(err, &ine) {
				t.Fatalf("Acquire(%q) = %v, want *InvalidNameError", bad.value, err)
			}
			if ine.Name != bad.value {
				t.Errorf("InvalidNameError.Name = %q, want %q", ine.Name, bad.value)
			}
			f.mu.Lock()
			n := len(f.store)
			f.mu.Unlock()
			if n != 0 {
				t.Errorf("invalid name touched the backend: %d keys, want 0", n)
			}
		})
	}
}

func TestLeaseReleaseClosesLostAndIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFakeKVSeam()
	s := newLeaserStore(f, unitTTL, newTestClock().Now)
	l, err := s.Acquire(ctx, "sessions/release")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if isChanClosed(l.Lost()) {
		t.Fatal("Lost() closed before Release")
	}

	// First Release relinquishes via exactly one update (expired-now, epoch preserved).
	if err := l.Release(ctx); err != nil {
		t.Fatalf("first Release: %v", err)
	}
	f.mu.Lock()
	updatesAfterFirst := f.updateCalls
	f.mu.Unlock()
	if updatesAfterFirst != 1 {
		t.Errorf("update calls after first Release = %d, want 1 (single relinquish)", updatesAfterFirst)
	}

	// Subsequent Releases are pure no-ops: nil, and ZERO additional backend I/O (the
	// released-guard short-circuits before any seam call).
	for i := 1; i < 3; i++ {
		if err := l.Release(ctx); err != nil {
			t.Fatalf("Release call %d = %v, want nil (idempotent)", i, err)
		}
	}
	f.mu.Lock()
	updatesAfterAll := f.updateCalls
	f.mu.Unlock()
	if updatesAfterAll != updatesAfterFirst {
		t.Errorf("update calls after repeated Release = %d, want %d (double-Release must do no backend I/O)",
			updatesAfterAll, updatesAfterFirst)
	}
	if !isChanClosed(l.Lost()) {
		t.Error("Lost() not closed after Release")
	}
}

func TestLeaseReleaseRelinquishesPreservingEpoch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clk := newTestClock()
	f := newFakeKVSeam()
	s := newLeaserStore(f, unitTTL, clk.Now)
	l, err := s.Acquire(ctx, "sessions/relinquish")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := l.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}
	// The relinquished entry must preserve the epoch and be expired-now, so the next
	// acquirer bumps to epoch+1 rather than resetting to 1.
	key, err := leaseKeyForName("sessions/relinquish")
	if err != nil {
		t.Fatalf("leaseKeyForName: %v", err)
	}
	f.mu.Lock()
	e, ok := f.store[key]
	f.mu.Unlock()
	if !ok {
		t.Fatal("relinquished entry was deleted; want preserved (breaks epoch monotonicity)")
	}
	rec, derr := decodeLeaseRecord(e.val)
	if derr != nil {
		t.Fatalf("decode relinquished record: %v", derr)
	}
	if rec.Epoch != l.Epoch() {
		t.Errorf("relinquished epoch = %d, want %d (preserved)", rec.Epoch, l.Epoch())
	}
	if rec.ExpiresAt.After(clk.Now()) {
		t.Errorf("relinquished ExpiresAt = %v, want <= now %v (expired-now)", rec.ExpiresAt, clk.Now())
	}
}

func TestLeaseRenewClassification(t *testing.T) {
	t.Parallel()
	errTransient := errors.New("transport timeout")
	tests := []struct {
		name string
		// updateErr is what the scripted seam returns from update; nil == a clean renew.
		updateErr error
		// advance moves the injected clock forward before renew. validUntil is seeded at
		// base+unitTTL (the ExpiresAt of the last write), so advancing past unitTTL puts
		// the clock beyond validUntil — the self-fence trigger.
		advance    time.Duration
		wantKept   bool // renew returns true (lease retained)
		wantExtend bool // clean renew must push validUntil forward to now+ttl
	}{
		{name: "clean renew keeps and extends the fence", updateErr: nil, advance: 0, wantKept: true, wantExtend: true},
		{name: "cas conflict surrenders (definitive loss)", updateErr: errKVCASConflict, advance: 0, wantKept: false},
		{name: "vanished entry surrenders (definitive loss)", updateErr: errKVKeyNotFound, advance: 0, wantKept: false},
		{name: "transient within TTL window retries", updateErr: errTransient, advance: 0, wantKept: true},
		{name: "transient past validUntil self-fences", updateErr: errTransient, advance: unitTTL + time.Second, wantKept: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clk := newTestClock()
			base := clk.Now()
			validUntil := base.Add(unitTTL) // ExpiresAt of our last successful write
			clk.advance(tt.advance)

			l := &kvLease{
				seam:       &scriptedUpdateSeam{err: tt.updateErr, rev: 99},
				now:        clk.Now,
				ttl:        unitTTL,
				name:       "sessions/renew",
				key:        "sessions_srenew",
				epoch:      5,
				rev:        1,
				validUntil: validUntil,
				lost:       make(chan struct{}),
				stop:       make(chan struct{}),
			}
			kept := l.renew(context.Background())
			if kept != tt.wantKept {
				t.Fatalf("renew kept=%v, want %v", kept, tt.wantKept)
			}
			if tt.wantExtend {
				l.mu.Lock()
				got := l.validUntil
				l.mu.Unlock()
				if want := clk.Now().Add(unitTTL); !got.Equal(want) {
					t.Errorf("validUntil after clean renew = %v, want %v (extended by TTL)", got, want)
				}
			}
			if !tt.wantKept {
				// Surrender: the heartbeat closes Lost() on a false renew, gating the
				// holder's out-of-band work. Mirror that and assert the signal fires.
				l.markLost()
				if !isChanClosed(l.Lost()) {
					t.Error("Lost() not closed after a surrendering renew")
				}
			}
		})
	}
}

// scriptedUpdateSeam returns a fixed outcome for update and errors for create/get; it
// isolates renew()'s classification (which only calls update).
type scriptedUpdateSeam struct {
	err error
	rev uint64
}

func (s *scriptedUpdateSeam) create(context.Context, string, []byte) (uint64, error) {
	return 0, errors.New("unexpected create")
}
func (s *scriptedUpdateSeam) get(context.Context, string) ([]byte, uint64, error) {
	return nil, 0, errors.New("unexpected get")
}
func (s *scriptedUpdateSeam) update(context.Context, string, []byte, uint64) (uint64, error) {
	if s.err != nil {
		return 0, s.err
	}
	return s.rev, nil
}

func TestBackstopBucketTTL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		ttl  time.Duration
		want time.Duration
	}{
		{"short ttl floors at one hour", 200 * time.Millisecond, time.Hour},
		{"exactly-floor multiple keeps floor", 36 * time.Second, time.Hour},
		{"large ttl scales by 100", time.Hour, 100 * time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := backstopBucketTTL(tt.ttl); got != tt.want {
				t.Errorf("backstopBucketTTL(%v) = %v, want %v", tt.ttl, got, tt.want)
			}
		})
	}
}

// stopHeartbeatForTest stops the renewal goroutine WITHOUT relinquishing the KV entry
// or closing Lost — it simulates holder death (a crashed process whose heartbeat
// simply stops, leaving the entry to age out) for the reclaim-after-expiry tests. It
// is a test-only seam, not part of the Lease contract; only in-package tests call it.
func (l *kvLease) stopHeartbeatForTest() {
	l.stopOnce.Do(func() { close(l.stop) })
	l.wg.Wait()
}

// isChanClosed reports whether ch is closed without blocking. Mirrors storetest's
// isClosed for use in these in-package unit tests.
func isChanClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
