package natsstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/looprig/storage"
)

// defaultLeaseTTL is the application-level lease validity window: an entry whose
// ExpiresAt is at or before the manager's clock is treated as expired and eligible
// for CAS takeover. The holder renews ExpiresAt on a heartbeat at a fraction of this.
// It mirrors looprig's LeaseManager default.
const defaultLeaseTTL = 30 * time.Second

// kvLeaseSeam is the narrow set of JetStream KV operations the leaser drives, isolated
// behind an interface (DIP + ISP) so the leaser's epoch/CAS logic can be unit-tested
// with a stateful fake and no running server. It wraps ONLY the three ops the leaser
// uses — create-if-absent, revision-CAS get, and revision-CAS update — and nothing
// more:
//   - There is no delete/relinquish op: Release relinquishes by CAS-updating the entry
//     to an expired-now state that PRESERVES the epoch (so the next acquirer fences
//     prev+1); deleting instead would reset the successor to epoch 1 and break
//     monotonicity. This mirrors looprig kvLease.Release.
//   - There is no watch op: ownership loss is detected on the heartbeat's own renewal
//     CAS — a higher-epoch takeover, a vanished entry, or an expiry-then-takeover all
//     surface as a CAS conflict / not-found on renew, which closes Lost(). This mirrors
//     looprig kvLease.renew; a separate watcher would be a redundant second loss path.
//
// The production binding (jetStreamKVSeam, seam.go) wraps a real nats.KeyValue and
// maps NATS' CAS errors onto the three sentinels below; unit tests fake it directly.
type kvLeaseSeam interface {
	// create writes val at key only if absent, returning the new revision. A key that
	// already exists yields errKVKeyExists (the create lost the single-holder race).
	create(ctx context.Context, key string, val []byte) (rev uint64, err error)
	// get returns the stored value and its current revision at key, or errKVKeyNotFound
	// if absent. Any other error is a genuine backend failure the caller fails closed on.
	get(ctx context.Context, key string) (val []byte, rev uint64, err error)
	// update writes val at key only if its current revision equals expectedRev,
	// returning the new revision. A revision mismatch yields errKVCASConflict; an entry
	// that has since vanished yields errKVKeyNotFound.
	update(ctx context.Context, key string, val []byte, expectedRev uint64) (rev uint64, err error)
}

// The three seam-contract sentinels the leaser classifies on. They are leaf errors
// with no context fields, so package sentinels are permitted (CLAUDE.md). Keeping the
// seam's error contract in package terms — not NATS terms — is what lets the leaser
// logic and its unit fake stay free of any nats.go dependency.
var (
	// errKVKeyExists is a create-if-absent that lost: the key already exists.
	errKVKeyExists = errors.New("natsstore: kv key already exists")
	// errKVKeyNotFound is a get/update against an absent key.
	errKVKeyNotFound = errors.New("natsstore: kv key not found")
	// errKVCASConflict is an update whose expected revision no longer matches.
	errKVCASConflict = errors.New("natsstore: kv revision conflict")
)

// LeaseOpError reports a definite failure of a KV operation the leaser performs
// (get / create / update / decode) that is NOT one of the expected CAS outcomes — an
// ambiguous or malformed read, or a backend fault. It fails closed: an ambiguous read
// never silently grants ownership. It names the lease, the operation, and unwraps to
// the underlying cause. (The storage taxonomy has no read-error type — LeaseHeldError
// and LeaseLostError are the only lease errors it defines — so this is a
// natsstore-specific typed error, analogous to StreamOpError.)
type LeaseOpError struct {
	Name  string
	Op    string
	Cause error
}

func (e *LeaseOpError) Error() string {
	return "natsstore: lease " + strconv.Quote(e.Name) + " " + e.Op + " failed: " + e.Cause.Error()
}
func (e *LeaseOpError) Unwrap() error { return e.Cause }

// leaseClock is the time seam for the leaser: it mints ExpiresAt and decides whether a
// stored entry is expired. Injecting it makes TTL expiry deterministic in unit tests
// (advance the clock past ExpiresAt); production wires time.Now.
type leaseClock func() time.Time

// leaseRecord is the JSON value stored in the KV entry for a name. Epoch is the
// monotonic fencing epoch; Holder is the unique per-acquisition id (observability —
// the revision CAS, not the holder, is what fences ownership); ExpiresAt is the
// wall-clock instant after which the lease is considered expired and may be taken over
// by CAS. Application-level expiry (this field) is the authoritative, clock-injectable
// check; the KV bucket's server-side TTL is only a coarse backstop for a truly dead
// holder.
type leaseRecord struct {
	Epoch     uint64    `json:"epoch"`
	Holder    string    `json:"holder"`
	ExpiresAt time.Time `json:"expires_at"`
}

// leaserStore implements storage.Leaser over one JetStream KV bucket, one entry per
// name keyed by leaseKeyForName(name). Acquire fences a monotonically increasing epoch
// via CAS so only one holder wins and every new owner out-ranks every prior one. It
// holds only the seam (DIP), the TTL, and the clock; it carries no per-name mutable
// state, so it is as safe for concurrent use as the seam beneath it.
type leaserStore struct {
	seam kvLeaseSeam
	ttl  time.Duration
	now  leaseClock
}

var _ storage.Leaser = (*leaserStore)(nil)

// newLeaserStore builds a leaser over seam with the given application-level TTL and
// clock. A non-positive ttl falls back to defaultLeaseTTL; a nil clock falls back to
// time.Now, so the store owns its own invariants.
func newLeaserStore(seam kvLeaseSeam, ttl time.Duration, now leaseClock) *leaserStore {
	if ttl <= 0 {
		ttl = defaultLeaseTTL
	}
	if now == nil {
		now = time.Now
	}
	return &leaserStore{seam: seam, ttl: ttl, now: now}
}

// Acquire grants single-writer ownership of name by fencing a monotonically increasing
// epoch into the lease bucket via CAS:
//
//   - entry absent           → create {Epoch:1, ...}; a losing create → *LeaseHeldError.
//   - entry present, expired  → update(rev) to {Epoch:prev+1, ...}; a losing update →
//     *LeaseHeldError.
//   - entry present and live  → *LeaseHeldError (the holder still owns it).
//
// It validates the name first (returning *InvalidNameError verbatim). On success it
// starts a heartbeat goroutine renewing ExpiresAt and returns a live Lease. The epoch
// is monotonic across acquisitions because each takeover writes prev+1 and Release
// preserves the epoch; only one holder wins a race because create / update(rev) is
// atomic. An ambiguous read fails closed with *LeaseOpError — it never grants.
func (s *leaserStore) Acquire(ctx context.Context, name string) (storage.Lease, error) {
	key, err := leaseKeyForName(name)
	if err != nil {
		return nil, err // *storage.InvalidNameError, verbatim
	}
	holder, err := newHolderID()
	if err != nil {
		return nil, &LeaseOpError{Name: name, Op: "holder-id", Cause: err}
	}

	val, rev, err := s.seam.get(ctx, key)
	switch {
	case errors.Is(err, errKVKeyNotFound):
		return s.createLease(ctx, name, key, holder, 1)
	case err != nil:
		return nil, &LeaseOpError{Name: name, Op: "get", Cause: err}
	}

	rec, derr := decodeLeaseRecord(val)
	if derr != nil {
		return nil, &LeaseOpError{Name: name, Op: "decode", Cause: derr}
	}
	if !s.expired(rec) {
		return nil, &storage.LeaseHeldError{Name: name, HolderEpoch: rec.Epoch}
	}
	return s.updateLease(ctx, name, key, holder, rec.Epoch+1, rev)
}

// createLease CAS-creates a fresh entry at epoch and, on success, returns a started
// lease. A losing create (errKVKeyExists) means a concurrent acquirer won the race →
// *LeaseHeldError.
func (s *leaserStore) createLease(ctx context.Context, name, key, holder string, epoch uint64) (storage.Lease, error) {
	val, expiresAt, err := s.encode(epoch, holder)
	if err != nil {
		return nil, &LeaseOpError{Name: name, Op: "encode", Cause: err}
	}
	rev, err := s.seam.create(ctx, key, val)
	if err != nil {
		if errors.Is(err, errKVKeyExists) {
			return nil, &storage.LeaseHeldError{Name: name, HolderEpoch: epoch}
		}
		return nil, &LeaseOpError{Name: name, Op: "create", Cause: err}
	}
	return s.start(name, key, holder, epoch, rev, expiresAt), nil
}

// updateLease CAS-updates an expired entry at lastRev to the next epoch and, on
// success, returns a started lease. A losing update (errKVCASConflict / not-found)
// means a concurrent acquirer or a stale holder's renew moved the revision →
// *LeaseHeldError naming the epoch that beat us (epoch-1).
func (s *leaserStore) updateLease(ctx context.Context, name, key, holder string, epoch, lastRev uint64) (storage.Lease, error) {
	val, expiresAt, err := s.encode(epoch, holder)
	if err != nil {
		return nil, &LeaseOpError{Name: name, Op: "encode", Cause: err}
	}
	rev, err := s.seam.update(ctx, key, val, lastRev)
	if err != nil {
		if errors.Is(err, errKVCASConflict) || errors.Is(err, errKVKeyNotFound) {
			return nil, &storage.LeaseHeldError{Name: name, HolderEpoch: epoch - 1}
		}
		return nil, &LeaseOpError{Name: name, Op: "update", Cause: err}
	}
	return s.start(name, key, holder, epoch, rev, expiresAt), nil
}

// encode mints a leaseRecord at epoch/holder with ExpiresAt = now + ttl and marshals it,
// returning the ExpiresAt it stamped so the caller can seed the lease's self-fence
// (validUntil) with the exact instant written to the entry.
func (s *leaserStore) encode(epoch uint64, holder string) ([]byte, time.Time, error) {
	expiresAt := s.now().Add(s.ttl)
	val, err := encodeLeaseRecord(leaseRecord{Epoch: epoch, Holder: holder, ExpiresAt: expiresAt})
	return val, expiresAt, err
}

// expired reports whether rec's ExpiresAt is at or before the store's clock — the
// authoritative, clock-injectable expiry check (equal counts as expired).
func (s *leaserStore) expired(rec leaseRecord) bool {
	return !rec.ExpiresAt.After(s.now())
}

// start constructs a live *kvLease at epoch/rev and launches its heartbeat goroutine.
// validUntil is the ExpiresAt just written to the entry — the instant up to which this
// holder can PROVE ownership, seeding the heartbeat's TTL-bounded self-fence.
func (s *leaserStore) start(name, key, holder string, epoch, rev uint64, validUntil time.Time) *kvLease {
	l := &kvLease{
		seam:       s.seam,
		now:        s.now,
		ttl:        s.ttl,
		name:       name,
		key:        key,
		holder:     holder,
		epoch:      epoch,
		rev:        rev,
		validUntil: validUntil,
		lost:       make(chan struct{}),
		stop:       make(chan struct{}),
	}
	l.startHeartbeat()
	return l
}

// kvLease is the concrete storage.Lease: a single holder of a name's KV lease entry.
// It heartbeats its ExpiresAt forward on a background goroutine via CAS on its own
// revision; a failed renew (a higher epoch / different holder took the entry, or it
// vanished) closes Lost(). epoch is immutable (a takeover is a loss, not an epoch
// change), so Epoch() needs no lock; mu guards rev and the release flag, which the
// heartbeat and the holder's Release mutate concurrently.
type kvLease struct {
	seam kvLeaseSeam
	now  leaseClock
	ttl  time.Duration

	name   string
	key    string
	holder string
	epoch  uint64 // immutable after construction

	mu         sync.Mutex
	rev        uint64    // guarded: advanced by each successful renew
	validUntil time.Time // guarded: ExpiresAt of the last write we made — up to here we can PROVE ownership
	released   bool      // guarded: set by Release, guards the idempotent no-op

	lost     chan struct{} // closed once on loss or release
	stop     chan struct{} // closed by Release to stop the heartbeat
	stopOnce sync.Once
	lostOnce sync.Once
	wg       sync.WaitGroup
}

var _ storage.Lease = (*kvLease)(nil)

// Epoch returns the fencing epoch this lease holds. It is fixed at construction.
func (l *kvLease) Epoch() uint64 { return l.epoch }

// Lost returns the channel closed when the lease is lost (takeover / expiry-then-
// takeover / vanished entry) or released. It never carries a value.
func (l *kvLease) Lost() <-chan struct{} { return l.lost }

// Release relinquishes the lease: it stops the heartbeat, waits for the renewal
// goroutine to exit (so rev is final), closes Lost, and best-effort CAS-updates the
// entry to an immediately-expired state that PRESERVES the epoch — so a successor can
// re-acquire at once (the entry reads expired) AND fences a strictly higher epoch (the
// prior epoch survives for prev+1). Deleting the entry instead would reset the next
// acquirer to epoch 1 and break monotonicity. The update fails silently if a higher
// epoch already replaced us (that successor already owns the name). Release is
// idempotent: a second call is a no-op returning nil with no backend I/O.
//
// Note: ctx does NOT bound the KV round-trip. The vendored legacy KV API
// (Get/Create/Update) takes no context; each call is bounded only by the JetStream
// client's default request timeout. ctx is accepted for signature uniformity.
func (l *kvLease) Release(ctx context.Context) error {
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return nil
	}
	l.released = true
	l.mu.Unlock()

	l.stopOnce.Do(func() { close(l.stop) })
	l.wg.Wait() // heartbeat has exited: rev is now final
	l.markLost()

	l.mu.Lock()
	epoch, holder, rev := l.epoch, l.holder, l.rev
	l.mu.Unlock()

	val, err := encodeLeaseRecord(leaseRecord{Epoch: epoch, Holder: holder, ExpiresAt: l.now()})
	if err != nil {
		return &LeaseOpError{Name: l.name, Op: "encode", Cause: err}
	}
	if _, err := l.seam.update(ctx, l.key, val, rev); err != nil {
		if errors.Is(err, errKVCASConflict) || errors.Is(err, errKVKeyNotFound) {
			return nil // a higher epoch already replaced us — nothing to relinquish
		}
		return &LeaseOpError{Name: l.name, Op: "release", Cause: err}
	}
	return nil
}

// markLost closes Lost exactly once. Safe to call from both Release and the heartbeat.
func (l *kvLease) markLost() {
	l.lostOnce.Do(func() { close(l.lost) })
}

// startHeartbeat launches the renewal goroutine. It renews at ttl/3 so two renews fit
// inside one TTL window (tolerating one missed beat). On a renewal that surrenders the
// lease — a higher epoch / different holder took the entry, or it vanished — it closes
// Lost and exits; the holder observes loss via Lost() and stops writing.
func (l *kvLease) startHeartbeat() {
	interval := l.ttl / 3
	if interval <= 0 {
		interval = l.ttl
	}
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-l.stop:
				return
			case <-t.C:
				if !l.renew(context.Background()) {
					l.markLost()
					return
				}
			}
		}
	}()
}

// renew CAS-updates the lease entry to push ExpiresAt forward, keyed on the lease's own
// revision. It returns true if the lease is still ours (renewed), false if it must be
// surrendered. Surrender happens in two cases:
//
//   - Definitive loss: a CAS conflict or a vanished entry — someone CAS-replaced our
//     revision (a higher epoch took over) or the entry is gone.
//   - TTL-bounded self-fence: on a TRANSIENT error (transport/timeout), the lease is
//     kept and retried on the next beat — UNLESS we can no longer PROVE ownership,
//     i.e. now is at or past validUntil (the ExpiresAt of our last successful write).
//     Past that instant our entry has provably expired and a successor may have taken
//     it over unseen (a network partition hides the takeover from our CAS), so a
//     partitioned holder must self-surrender. Because the bound IS the TTL, this never
//     false-surrenders on a sub-TTL reconnect — a blip shorter than the lease window
//     still retries.
//
// Loss closes Lost() (the caller, startHeartbeat, calls markLost), which gates the
// out-of-band, non-ledger-fenced work a holder performs under the lease.
func (l *kvLease) renew(ctx context.Context) bool {
	l.mu.Lock()
	epoch, holder, rev := l.epoch, l.holder, l.rev
	l.mu.Unlock()

	expiresAt := l.now().Add(l.ttl)
	val, err := encodeLeaseRecord(leaseRecord{Epoch: epoch, Holder: holder, ExpiresAt: expiresAt})
	if err != nil {
		return true // encoding a uint64+string+time can't realistically fail; treat as transient
	}
	newRev, err := l.seam.update(ctx, l.key, val, rev)
	if err == nil {
		l.mu.Lock()
		l.rev = newRev
		l.validUntil = expiresAt // renewed: we can now prove ownership for another full TTL
		l.mu.Unlock()
		return true
	}
	if errors.Is(err, errKVCASConflict) || errors.Is(err, errKVKeyNotFound) {
		return false // definitive loss
	}
	// Transient error: keep the lease only while we can still prove ownership.
	l.mu.Lock()
	validUntil := l.validUntil
	l.mu.Unlock()
	if !l.now().Before(validUntil) {
		return false // self-fence: our lease has provably expired; surrender
	}
	return true
}

// leaseKeyForName maps a storage name to its KV key, reusing the D2 stream escaping so
// the key is a single flat JetStream-safe token in [a-z0-9_-] (no '.' — which a KV key
// otherwise turns into subject hierarchy — and no '/'). It returns the
// *storage.InvalidNameError verbatim for an invalid name, so Acquire validates and
// escapes in one call.
func leaseKeyForName(name string) (string, error) {
	return streamForName(name)
}

// newHolderID mints a unique acquirer id from crypto/rand (never math/rand, per
// CLAUDE.md) — 16 random bytes as hex.
func newHolderID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// backstopBucketTTL returns the KV bucket's server-side (wall-clock) TTL: a generous
// multiple of the application lease TTL, floored at one hour so a very short test TTL
// still yields a bucket TTL safely longer than a test run. The bucket TTL must never
// reap an entry before the application-level ExpiresAt check would (that check is the
// deterministic, takeover-driving one); it only eventually reaps entries whose holder
// died without releasing. It mirrors looprig's backstopBucketTTL.
func backstopBucketTTL(ttl time.Duration) time.Duration {
	const floor = time.Hour
	if mult := ttl * 100; mult > floor {
		return mult
	}
	return floor
}

// LeaseEncodeError wraps a failure to marshal a leaseRecord to JSON. A leaseRecord is a
// uint64 + string + time, so this is effectively unreachable, but the codec returns a
// typed error rather than dropping the json.Marshal error, to satisfy the
// errors-are-typed contract.
type LeaseEncodeError struct{ Cause error }

func (e *LeaseEncodeError) Error() string {
	return "natsstore: encode lease record: " + e.Cause.Error()
}
func (e *LeaseEncodeError) Unwrap() error { return e.Cause }

// encodeLeaseRecord marshals a leaseRecord to its JSON value.
func encodeLeaseRecord(rec leaseRecord) ([]byte, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return nil, &LeaseEncodeError{Cause: err}
	}
	return data, nil
}

// decodeLeaseRecord decodes a stored lease entry value, failing closed on malformed
// JSON, an unknown field, or trailing bytes — an ambiguous entry never silently grants
// ownership.
func decodeLeaseRecord(data []byte) (leaseRecord, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var rec leaseRecord
	if err := dec.Decode(&rec); err != nil {
		return leaseRecord{}, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return leaseRecord{}, errTrailingLeaseData
	}
	return rec, nil
}

// errTrailingLeaseData is the leaf cause when a stored lease entry has bytes after its
// JSON object. It carries no context fields, so a sentinel is permitted.
var errTrailingLeaseData = errors.New("natsstore: trailing data after lease record")
