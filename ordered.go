package natsstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/looprig/storage"
)

// orderedCreateAttempts bounds the optimistic retry loop Create runs when it
// loses the per-scope counter race. Every rejection means SOME writer made
// progress, so the scope as a whole never livelocks — but that says nothing
// about THIS caller, which can lose to newly arriving writers indefinitely. The
// bound is therefore a real last resort, not a formality: exhausting it yields a
// retryable *OrderedContentionError. Two mechanisms keep it out of reach in
// practice — the per-scope latch (orderedStore.acquireScope), which stops
// same-process creators from colliding at all, and the jittered backoff below,
// which breaks the lockstep herd across processes.
const orderedCreateAttempts = 64

// The retry backoff. Losers that retry with no delay re-collide immediately, so
// each round costs O(N) round trips and N rounds cost O(N^2); a jittered wait
// spreads the herd instead. The base is small enough to be invisible at low
// contention and the cap keeps the worst case bounded.
const (
	orderedRetryBase     = 100 * time.Microsecond
	orderedRetryMaxShift = 8 // cap the window at orderedRetryBase << 8

	// orderedViewRetryBase is the window a namespace view starts from when its
	// subscription must be re-attached. It is far larger than the create-path
	// base because a detached view is waiting on a stream or a connection, not
	// racing another writer, and a tight re-attach loop would only add load to a
	// server that is already unwell.
	orderedViewRetryBase = 50 * time.Millisecond
)

// The seam reports exactly two conditions the ordered store must distinguish
// from an ordinary failure, and it reports them as leaf sentinels so the store's
// logic stays free of NATS types (and testable with a fake seam).
var (
	// errOrderedPrecondition reports that at least one message in a publish or
	// atomic batch failed its expected-last-subject-sequence fence. Nothing was
	// committed.
	errOrderedPrecondition = errors.New("natsstore: ordered publish precondition failed")

	// errOrderedAmbiguous reports a publish whose acknowledgement was lost, so
	// the write may or may not have committed.
	errOrderedAmbiguous = errors.New("natsstore: ordered publish outcome is unknown")
)

// OrderedContentionError reports a Create that could not win its order scope's
// counter within the bounded retry budget. Nothing was written, and the identity
// is still free, so the operation is RETRYABLE: a caller that retries it later
// gets a normal allocation once the scope quiets down.
type OrderedContentionError struct {
	ID       storage.OrderedID
	Attempts int
}

func (e *OrderedContentionError) Error() string {
	return "natsstore: ordered create for namespace " + strconv.Quote(e.ID.Namespace) +
		" ordering scope " + strconv.Quote(e.ID.OrderingScope) +
		" lost the order counter race " + strconv.Itoa(e.Attempts) + " times"
}

// OrderedOrderExhaustedError reports an order scope that cannot allocate another
// order without overflowing uint64. Order is immutable and never reused, so the
// scope is permanently full; the store leaves it unchanged.
type OrderedOrderExhaustedError struct {
	ID    storage.OrderedID
	Order uint64
}

func (e *OrderedOrderExhaustedError) Error() string {
	return "natsstore: ordered namespace " + strconv.Quote(e.ID.Namespace) +
		" ordering scope " + strconv.Quote(e.ID.OrderingScope) +
		" exhausted its order space at " + strconv.FormatUint(e.Order, 10)
}

// orderedStreamSpec names the stream one OrderedIndex namespace occupies and the
// wildcard subject it binds. Both are pure functions of the namespace.
type orderedStreamSpec struct {
	stream        string
	subjectFilter string
}

// orderedMsg is one fenced message: a subject, its payload, and its OWN expected
// last sequence on that subject. Zero means "this subject must not exist yet",
// which is exactly what JetStream's expected-last-subject-sequence header means,
// so the two agree by construction. Every ordered write is fenced; there is no
// unconditional publish.
type orderedMsg struct {
	subject       string
	data          []byte
	expectLastSeq uint64
}

// orderedSeam is the narrow set of JetStream operations the ordered store drives,
// isolated behind an interface (DIP + ISP) so the store's allocation, CAS, and
// ambiguity logic is server-free and unit-testable. The production binding
// (jetStreamOrderedSeam, ordered_seam.go) is the only ordered code that speaks
// the wire protocol.
type orderedSeam interface {
	// ensureStream idempotently provisions the namespace's stream and verifies
	// that an already-provisioned stream carries this layout version and the
	// configuration the design depends on.
	ensureStream(ctx context.Context, spec orderedStreamSpec) error
	// lastMsgForSubject returns the current message on subject: its stream
	// sequence and payload, or (0, nil, nil) when the subject — or the whole
	// stream — has never been written.
	lastMsgForSubject(ctx context.Context, stream, subject string) (seq uint64, data []byte, err error)
	// publish commits one fenced message. It returns errOrderedPrecondition when
	// the fence fails and errOrderedAmbiguous when the acknowledgement is lost.
	publish(ctx context.Context, stream string, msg orderedMsg) error
	// publishBatch commits every message atomically under its own fence, with the
	// same two sentinels. A rejected batch commits nothing. stream names the
	// stream the subjects live in, so a failure can say which one it was.
	publishBatch(ctx context.Context, stream string, msgs []orderedMsg) error
	// streamTip returns the stream's last stored sequence, or 0 when the stream
	// does not exist. It is the barrier a query captures before reading.
	streamTip(ctx context.Context, stream string) (uint64, error)
	// watchStream delivers the current head of every subject in the stream and
	// then every later commit, to apply, until ctx ends. It returns nil when ctx
	// ended and an error when the subscription failed and must be re-attached.
	watchStream(ctx context.Context, spec orderedStreamSpec, apply func(subject string, seq uint64, data []byte)) error
}

// orderedStore implements the mutating half of storage.OrderedIndex over one
// JetStream stream per namespace. Within that stream an order scope owns a
// counter subject holding its last allocated order, and every record owns a
// subject addressed by the hash of its identity. Both families are configured
// MaxMsgsPerSubject=1, so "the last message on the subject" IS the current
// version and its stream sequence is the fence a compare-and-swap writes under.
//
// It holds only the seam and carries no mutable state, so it is as safe for
// concurrent use as the seam beneath it.
type orderedStore struct {
	seam orderedSeam

	// retryBase is the backoff window Create starts from. It is a field rather
	// than a constant only so tests can drive the retry budget to exhaustion
	// without paying for the waits; production always uses orderedRetryBase.
	retryBase time.Duration

	// viewRetryBase is the backoff window the materialized view starts from when
	// its subscription must be re-attached. It is a field for the same reason
	// retryBase is: tests drive a re-attach without paying for the waits.
	viewRetryBase time.Duration

	// mu guards latches (the per-order-scope in-process queue; see acquireScope),
	// the per-namespace view registry, and the closed flag.
	mu      sync.Mutex
	latches map[string]*orderedScopeLatch
	views   map[string]*orderedView
	closed  bool

	// stats counts the work the query paths perform. See orderedStats.
	stats *orderedStats

	// localPathReporter carries the engine's on-disk paths. It is populated when
	// the store is wired into the composite (see buildComposite in natsstore.go),
	// which is the lifecycle task; an unwired store reports no paths.
	localPathReporter
}

// newOrderedStore builds an ordered index over seam.
func newOrderedStore(seam orderedSeam) *orderedStore {
	return &orderedStore{
		seam:          seam,
		retryBase:     orderedRetryBase,
		viewRetryBase: orderedViewRetryBase,
		latches:       map[string]*orderedScopeLatch{},
		views:         map[string]*orderedView{},
		stats:         &orderedStats{},
	}
}

// orderedScopeLatch serializes this process's creators within one order scope.
// token holds a single permit; refs is how many callers currently want the
// latch, so it can be dropped from the map when the scope goes idle instead of
// accumulating one entry per scope ever written.
type orderedScopeLatch struct {
	token chan struct{}
	refs  int
}

// acquireScope makes same-process Creates in one order scope queue instead of
// colliding. Nothing is lost by doing so: the counter fence already admits
// exactly one allocator at a time, so concurrent attempts within a process were
// never parallelism — only wasted round trips, wasted server-side in-flight
// batch slots, and a herd that re-collides on every retry. Cross-process
// contention is still handled optimistically, by the fence and the backoff.
//
// It returns a release function that must be called exactly once, and honours
// ctx while waiting.
func (s *orderedStore) acquireScope(ctx context.Context, key string) (func(), error) {
	s.mu.Lock()
	latch, ok := s.latches[key]
	if !ok {
		latch = &orderedScopeLatch{token: make(chan struct{}, 1)}
		s.latches[key] = latch
	}
	latch.refs++
	s.mu.Unlock()

	drop := func(held bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if held {
			<-latch.token
		}
		latch.refs--
		if latch.refs == 0 {
			delete(s.latches, key)
		}
	}

	select {
	case latch.token <- struct{}{}:
		return func() { drop(true) }, nil
	case <-ctx.Done():
		drop(false)
		return nil, ctx.Err()
	}
}

// waitBeforeRetry sleeps a uniformly random slice of an exponentially growing
// window ("full jitter"), or returns ctx's error if the caller gives up first.
// The randomness is what breaks the herd, so it is drawn per caller per attempt.
func (s *orderedStore) waitBeforeRetry(ctx context.Context, attempt int) error {
	return orderedBackoff(ctx, s.retryBase, attempt-1, orderedRetryMaxShift)
}

// orderedBackoff sleeps a uniformly random slice of an exponentially growing
// window ("full jitter"), or returns ctx's error if the caller gives up first.
// The randomness is what breaks a herd, so it is drawn per caller per attempt.
//
// A zero or negative base disables the wait entirely — there is nothing to wait
// for, so not even a cancelled context turns the call into a failure — which is
// how tests drive a retry budget to exhaustion without paying for it. shift is
// the number of doublings requested and is clamped into [0, maxShift].
func orderedBackoff(ctx context.Context, base time.Duration, shift int, maxShift int) error {
	if base <= 0 {
		return nil
	}
	shift = min(max(shift, 0), maxShift)
	window := base << shift
	var b [8]byte
	orderedRandomBytes(b[:])
	// #nosec G115 -- window is a positive base shifted by at most maxShift, so it
	// is a small positive Duration; the modulus keeps the result inside it.
	delay := time.Duration(binary.BigEndian.Uint64(b[:]) % uint64(window))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// orderedLocation resolves the stream, counter subject, and record subject for a
// validated identity. It validates id first, so every public method inherits the
// contract's "validate the identity before inspecting anything" rule from here.
func orderedLocation(id storage.OrderedID) (spec orderedStreamSpec, counter, record string, err error) {
	if err := storage.ValidateOrderedID(id); err != nil {
		return orderedStreamSpec{}, "", "", err
	}
	stream, err := orderedStreamName(id.Namespace)
	if err != nil {
		return orderedStreamSpec{}, "", "", err
	}
	filter, err := orderedSubjectFilter(id.Namespace)
	if err != nil {
		return orderedStreamSpec{}, "", "", err
	}
	counter, err = orderedCounterSubject(id.Namespace, id.OrderingScope)
	if err != nil {
		return orderedStreamSpec{}, "", "", err
	}
	record, err = orderedRecordSubject(id)
	if err != nil {
		return orderedStreamSpec{}, "", "", err
	}
	return orderedStreamSpec{stream: stream, subjectFilter: filter}, counter, record, nil
}

// readRecord returns the current record on subject together with the stream
// sequence a compare-and-swap must fence on. A missing subject yields ok=false;
// a present but unreadable or foreign payload is an error, never an absence.
func (s *orderedStore) readRecord(ctx context.Context, stream, subject string) (rec storage.OrderedRecord, seq uint64, ok bool, err error) {
	seq, data, err := s.seam.lastMsgForSubject(ctx, stream, subject)
	if err != nil {
		return storage.OrderedRecord{}, 0, false, err
	}
	if seq == 0 {
		return storage.OrderedRecord{}, 0, false, nil
	}
	rec, err = decodeOrderedRecord(subject, data)
	if err != nil {
		return storage.OrderedRecord{}, 0, false, err
	}
	return rec, seq, true, nil
}

// Get returns the current record for id, including a logical tombstone.
func (s *orderedStore) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	spec, _, recordSubj, err := orderedLocation(id)
	if err != nil {
		return storage.OrderedRecord{}, err
	}
	rec, _, ok, err := s.readRecord(ctx, spec.stream, recordSubj)
	if err != nil {
		return storage.OrderedRecord{}, err
	}
	if !ok {
		return storage.OrderedRecord{}, &storage.OrderedRecordNotFoundError{ID: id}
	}
	return rec, nil
}

// Create is idempotent by identity. It validates id, then inspects the identity
// BEFORE validating any candidate field: an existing record — live or tombstoned
// — is returned verbatim with created=false, whatever the caller passed. Only an
// absent identity validates the candidate, allocates the scope's next order, and
// commits the counter and the record as one atomic batch, each message fenced by
// its own expectation.
func (s *orderedStore) Create(ctx context.Context, id storage.OrderedID, rankingScope string, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, bool, error) {
	spec, counterSubj, recordSubj, err := orderedLocation(id)
	if err != nil {
		return storage.OrderedRecord{}, false, err
	}
	if err := s.seam.ensureStream(ctx, spec); err != nil {
		return storage.OrderedRecord{}, false, err
	}
	existing, _, ok, err := s.readRecord(ctx, spec.stream, recordSubj)
	if err != nil {
		return storage.OrderedRecord{}, false, err
	}
	if ok {
		return existing, false, nil
	}
	if err := storage.ValidateName(rankingScope); err != nil {
		return storage.OrderedRecord{}, false, err
	}
	if err := storage.ValidateOrderedValue(value); err != nil {
		return storage.OrderedRecord{}, false, err
	}
	if err := storage.ValidateDue(due); err != nil {
		return storage.OrderedRecord{}, false, err
	}

	release, err := s.acquireScope(ctx, counterSubj)
	if err != nil {
		return storage.OrderedRecord{}, false, err
	}
	defer release()

	for attempt := 1; attempt <= orderedCreateAttempts; attempt++ {
		if attempt > 1 {
			if err := s.waitBeforeRetry(ctx, attempt-1); err != nil {
				return storage.OrderedRecord{}, false, err
			}
		}
		counterSeq, counterData, err := s.seam.lastMsgForSubject(ctx, spec.stream, counterSubj)
		if err != nil {
			return storage.OrderedRecord{}, false, err
		}
		var last uint64
		if counterSeq != 0 {
			if last, err = decodeOrderedCounter(counterData); err != nil {
				return storage.OrderedRecord{}, false, err
			}
		}
		if last == math.MaxUint64 {
			return storage.OrderedRecord{}, false, &OrderedOrderExhaustedError{ID: id, Order: last}
		}
		rec := storage.OrderedRecord{
			ID:           id,
			RankingScope: rankingScope,
			Revision:     1,
			Order:        last + 1,
			Due:          due,
			Rank:         rank,
			Value:        value,
		}
		payload, err := encodeOrderedRecord(rec)
		if err != nil {
			return storage.OrderedRecord{}, false, err
		}
		// Two subjects, two independent expectations: the counter must still be
		// where we read it, and the record subject must not exist at all. They are
		// necessarily distinct subjects — one batch cannot fence one subject twice.
		err = s.seam.publishBatch(ctx, spec.stream, []orderedMsg{
			{subject: counterSubj, data: encodeOrderedCounter(rec.Order), expectLastSeq: counterSeq},
			{subject: recordSubj, data: payload, expectLastSeq: 0},
		})
		switch {
		case err == nil:
			// The record is retained by the caller from here on, so it must not
			// alias the caller's own input slice.
			rec.Value = bytes.Clone(value)
			return rec, true, nil
		case errors.Is(err, errOrderedPrecondition):
			// Either a concurrent allocator moved the counter or a concurrent
			// creator claimed this identity. The identity settles it.
			winner, _, ok, readErr := s.readRecord(ctx, spec.stream, recordSubj)
			if readErr != nil {
				return storage.OrderedRecord{}, false, readErr
			}
			if ok {
				return winner, false, nil
			}
		case errors.Is(err, errOrderedAmbiguous):
			winner, _, ok, readErr := s.readRecord(ctx, spec.stream, recordSubj)
			if readErr != nil {
				return storage.OrderedRecord{}, false, readErr
			}
			if ok {
				// The identity exists either way, so the canonical record is the
				// answer; what is left to decide is created. A stored record identical
				// to the one this call submitted is proof the submitted write landed,
				// which is exactly how commitRevision resolves the same ambiguity, so
				// report the creation honestly instead of downgrading a successful
				// first Create to a duplicate. The residual imprecision is narrow and
				// accepted: two creators submitting byte-identical records that were
				// also assigned the same order would both report created=true.
				return winner, orderedRecordEqual(winner, rec), nil
			}
			return storage.OrderedRecord{}, false, &storage.OrderedAmbiguousError{
				Operation: storage.OrderedCreateOperation, ID: id, Cause: err,
			}
		default:
			return storage.OrderedRecord{}, false, err
		}
	}
	return storage.OrderedRecord{}, false, &OrderedContentionError{ID: id, Attempts: orderedCreateAttempts}
}

// Update replaces value, rank, and due state in one revision compare-and-swap.
// Precedence follows the contract exactly: absent, then tombstoned, then
// candidate validation, then the revision compare. Identity, ranking scope, and
// immutable order cannot change.
func (s *orderedStore) Update(ctx context.Context, id storage.OrderedID, expectedRevision uint64, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, error) {
	spec, _, recordSubj, err := orderedLocation(id)
	if err != nil {
		return storage.OrderedRecord{}, err
	}
	current, seq, ok, err := s.readRecord(ctx, spec.stream, recordSubj)
	if err != nil {
		return storage.OrderedRecord{}, err
	}
	if !ok {
		return storage.OrderedRecord{}, &storage.OrderedRecordNotFoundError{ID: id}
	}
	if current.Deleted {
		return storage.OrderedRecord{}, &storage.OrderedDeletedError{ID: id}
	}
	if err := storage.ValidateOrderedValue(value); err != nil {
		return storage.OrderedRecord{}, err
	}
	if err := storage.ValidateDue(due); err != nil {
		return storage.OrderedRecord{}, err
	}
	if current.Revision != expectedRevision {
		return storage.OrderedRecord{}, &storage.OrderedRevisionConflictError{
			ID: id, ExpectedRevision: expectedRevision, ActualRevision: current.Revision,
		}
	}
	if current.Revision == math.MaxUint64 {
		return storage.OrderedRecord{}, &storage.OrderedRevisionExhaustedError{ID: id, Revision: current.Revision}
	}
	next := current
	next.Revision = current.Revision + 1
	next.Value = value
	next.Rank = rank
	next.Due = due
	return s.commitRevision(ctx, spec.stream, recordSubj, seq, next, expectedRevision, storage.OrderedUpdateOperation)
}

// Delete writes a logical tombstone: unranked, not due, value and immutable
// order preserved. A tombstone is returned as-is regardless of
// expectedRevision, so a retry of a completed Delete is satisfied by the
// original tombstone rather than by a conflict.
func (s *orderedStore) Delete(ctx context.Context, id storage.OrderedID, expectedRevision uint64) (storage.OrderedRecord, error) {
	spec, _, recordSubj, err := orderedLocation(id)
	if err != nil {
		return storage.OrderedRecord{}, err
	}
	current, seq, ok, err := s.readRecord(ctx, spec.stream, recordSubj)
	if err != nil {
		return storage.OrderedRecord{}, err
	}
	if !ok {
		return storage.OrderedRecord{}, &storage.OrderedRecordNotFoundError{ID: id}
	}
	if current.Deleted {
		return current, nil
	}
	if current.Revision != expectedRevision {
		return storage.OrderedRecord{}, &storage.OrderedRevisionConflictError{
			ID: id, ExpectedRevision: expectedRevision, ActualRevision: current.Revision,
		}
	}
	if current.Revision == math.MaxUint64 {
		return storage.OrderedRecord{}, &storage.OrderedRevisionExhaustedError{ID: id, Revision: current.Revision}
	}
	next := current
	next.Revision = current.Revision + 1
	next.Deleted = true
	next.Rank = storage.Rank{}
	next.Due = storage.Due{State: storage.NotDue, UnixMillis: 0}
	return s.commitRevision(ctx, spec.stream, recordSubj, seq, next, expectedRevision, storage.OrderedDeleteOperation)
}

// commitRevision publishes next under the record subject's observed sequence and
// classifies the outcome. A failed fence means someone else advanced the record,
// which is reported as a revision conflict carrying the observed revision. A lost
// acknowledgement is resolved by reading the record back: if the stored record IS
// next, the write landed and succeeds; otherwise the outcome stays ambiguous.
func (s *orderedStore) commitRevision(ctx context.Context, stream, subject string, expectSeq uint64, next storage.OrderedRecord, expectedRevision uint64, op storage.OrderedOperation) (storage.OrderedRecord, error) {
	payload, err := encodeOrderedRecord(next)
	if err != nil {
		return storage.OrderedRecord{}, err
	}
	err = s.seam.publish(ctx, stream, orderedMsg{subject: subject, data: payload, expectLastSeq: expectSeq})
	switch {
	case err == nil:
		// As in Create: the returned record must not alias the caller's slice.
		next.Value = bytes.Clone(next.Value)
		return next, nil
	case errors.Is(err, errOrderedPrecondition):
		current, _, ok, readErr := s.readRecord(ctx, stream, subject)
		if readErr != nil {
			return storage.OrderedRecord{}, readErr
		}
		if !ok {
			return storage.OrderedRecord{}, &storage.OrderedRecordNotFoundError{ID: next.ID}
		}
		// The fence lost, so the only linearization of this call is AFTER the write
		// that won it. If that winner was a Delete, the contract's precedence
		// applies unchanged: Update reports the tombstone, Delete returns it, and
		// neither is a revision conflict — "regardless of expectedRevision" governs
		// this read exactly as it governs the pre-write one.
		if current.Deleted {
			if op == storage.OrderedDeleteOperation {
				return current, nil
			}
			return storage.OrderedRecord{}, &storage.OrderedDeletedError{ID: next.ID}
		}
		return storage.OrderedRecord{}, &storage.OrderedRevisionConflictError{
			ID: next.ID, ExpectedRevision: expectedRevision, ActualRevision: current.Revision,
		}
	case errors.Is(err, errOrderedAmbiguous):
		current, _, ok, readErr := s.readRecord(ctx, stream, subject)
		if readErr != nil {
			return storage.OrderedRecord{}, readErr
		}
		if ok && orderedRecordEqual(current, next) {
			return current, nil
		}
		return storage.OrderedRecord{}, &storage.OrderedAmbiguousError{Operation: op, ID: next.ID, Cause: err}
	default:
		return storage.OrderedRecord{}, err
	}
}

// orderedRandomBytes fills b with cryptographically secure entropy. crypto/rand
// is the only random source this module uses (per CLAUDE.md), and its Read is
// documented never to return an error — it crashes the program irrecoverably
// instead — so wrapping it in an error return would only add a branch no test
// can reach.
func orderedRandomBytes(b []byte) {
	rand.Read(b)
}

// orderedRecordEqual reports whether two records are the same observable state.
// It is how an ambiguous write is resolved: only a stored record identical to the
// one we tried to write proves the write landed.
func orderedRecordEqual(a, b storage.OrderedRecord) bool {
	return a.ID == b.ID &&
		a.RankingScope == b.RankingScope &&
		a.Revision == b.Revision &&
		a.Order == b.Order &&
		a.Due == b.Due &&
		a.Rank == b.Rank &&
		a.Deleted == b.Deleted &&
		bytes.Equal(a.Value, b.Value)
}

// --- materialized query views ----------------------------------------------
//
// A namespace's authoritative state lives in JetStream: one message per record
// subject, one per order-scope counter subject, MaxMsgsPerSubject=1. That layout
// is exactly right for the mutating half of OrderedIndex — the last message on a
// subject IS the current version, and its stream sequence is the fence a
// compare-and-swap writes under — and it offers nothing at all to a query. There
// is no server-side secondary index on rank or due time, and no subject-listing
// call whose cost is proportional to a page.
//
// So the provider maintains one. The view below is an INTERNAL PROVIDER INDEX
// derived from the authoritative stream, not a second SessionStore and not a
// cache with its own retention policy: it holds no state that JetStream does not
// hold, it is rebuilt from live heads whenever the process or the subscription
// restarts, and losing it costs a re-bootstrap and nothing else. Nothing is ever
// read from it that was not written to JetStream first.
//
// Three properties are load-bearing:
//
//  1. It is built from LIVE HEADS. The subscription is DeliverLastPerSubject
//     plus the tail, never a historical replay — because with
//     MaxMsgsPerSubject=1 there is no history to replay, and because
//     Invariant 15 forbids a read path that enumerates subjects or walks the
//     past.
//  2. A query is CAUGHT UP before it answers. It captures the stream's tip
//     sequence, waits for the view to have applied through at least that tip,
//     and only then pages an index. A reader therefore never observes a state
//     older than the moment its own call began.
//  3. Its indexes are SORTED, so a page costs its own limit plus the binary
//     searches that locate it, independent of how many records the namespace
//     holds.

// orderedViewRetryMaxShift caps the view's re-attach backoff window. It is
// shorter than the create path's cap because a detached view is not competing
// with anything: it is waiting for a stream or a connection to come back, and a
// reader is parked on the barrier while it does.
const orderedViewRetryMaxShift = 6

// orderedIdentity is the comparable in-view key for a record. The namespace is
// fixed by the view, so the remaining identity components are the ordering scope
// and the stable key. It is deliberately NOT the hashed subject token: the view
// indexes identities, and the authoritative identity is the one the payload
// carries and decodeOrderedRecord has already verified against the subject.
type orderedIdentity struct {
	orderingScope string
	stableKey     storage.StableKey
}

// orderedComparator is a strict ordering over two records under one of the two
// frozen sort keys. Each key is defined by EXACTLY ONE function, and both the
// index-maintenance path and the cursor-resume path consult that same function —
// the resume path by searching for a probe record built from the cursor. Stating
// an ordering twice is how a tiebreaker silently disappears from one copy.
type orderedComparator func(left, right storage.OrderedRecord) bool

// orderedRankedBefore is the frozen DESCENDING ranked order:
// (rank, stable_key, ordering_scope), each compared larger-first. Namespace and
// ranking scope are fixed by the index this comparator sorts.
//
// The third component is required, not decorative. Record identity is
// (namespace, ordering_scope, stable_key), so two records in one ranking scope
// may share a rank AND a stable key while remaining distinct records; without
// the ordering-scope tiebreaker they would be indistinguishable to a cursor and
// pagination would either skip one forever or loop on it.
func orderedRankedBefore(left, right storage.OrderedRecord) bool {
	if left.Rank.Value != right.Rank.Value {
		return left.Rank.Value > right.Rank.Value
	}
	if left.ID.StableKey != right.ID.StableKey {
		return left.ID.StableKey > right.ID.StableKey
	}
	return left.ID.OrderingScope > right.ID.OrderingScope
}

// orderedDueBefore is the frozen ASCENDING due order:
// (due_at, stable_key, ordering_scope), each compared smaller-first. The
// ordering-scope tiebreaker is required for the same reason as in
// orderedRankedBefore.
func orderedDueBefore(left, right storage.OrderedRecord) bool {
	if left.Due.UnixMillis != right.Due.UnixMillis {
		return left.Due.UnixMillis < right.Due.UnixMillis
	}
	if left.ID.StableKey != right.ID.StableKey {
		return left.ID.StableKey < right.ID.StableKey
	}
	return left.ID.OrderingScope < right.ID.OrderingScope
}

// orderedStats counts the work the ordered store's query paths perform. It is
// the instrumentation Invariant 15 is asserted against: a warm page must touch
// index entries proportional to its own limit, apply no further stream messages,
// and read the stream tip exactly once.
type orderedStats struct {
	messagesApplied atomic.Uint64
	indexVisited    atomic.Uint64
	recordsCopied   atomic.Uint64
	streamTipReads  atomic.Uint64
}

// orderedQueryStats is a snapshot of orderedStats. The fields are read
// independently, so it is not an atomic snapshot; it does not need to be,
// because the assertions difference two snapshots around one synchronous query.
type orderedQueryStats struct {
	// MessagesApplied counts stream messages the views have applied.
	MessagesApplied uint64
	// IndexVisited counts sorted-index entries compared, whether while serving a
	// read or while applying a change. A read's own share is exact whenever
	// MessagesApplied did not move across the same window.
	IndexVisited uint64
	// RecordsCopied counts records copied out into a page.
	RecordsCopied uint64
	// StreamTipReads counts barrier captures, which is one per listing call.
	StreamTipReads uint64
}

func (s *orderedStats) snapshot() orderedQueryStats {
	return orderedQueryStats{
		MessagesApplied: s.messagesApplied.Load(),
		IndexVisited:    s.indexVisited.Load(),
		RecordsCopied:   s.recordsCopied.Load(),
		StreamTipReads:  s.streamTipReads.Load(),
	}
}

// OrderedStoreClosedError reports a listing issued against an ordered store
// whose namespace views have been stopped. It is a permanent, typed refusal
// rather than a silently empty page: a closed store has no view to be caught up
// with, so it cannot honestly answer a query at all.
type OrderedStoreClosedError struct {
	Namespace string
}

func (e *OrderedStoreClosedError) Error() string {
	return "natsstore: ordered store is closed; namespace " + strconv.Quote(e.Namespace) + " has no view"
}

// OrderedViewStopTimeoutError reports a Close whose context expired before a
// namespace view's goroutine finished. The goroutine is already cancelled and
// will exit, so this reports a slow shutdown, not a leak.
type OrderedViewStopTimeoutError struct {
	Namespace string
	Cause     error
}

func (e *OrderedViewStopTimeoutError) Error() string {
	return "natsstore: ordered view for namespace " + strconv.Quote(e.Namespace) +
		" did not stop in time: " + e.Cause.Error()
}

// Unwrap returns the context error that ended the wait.
func (e *OrderedViewStopTimeoutError) Unwrap() error { return e.Cause }

// orderedView is one namespace's materialized query view: the current record for
// every identity, plus three sorted indexes over them.
//
// Concurrency: mu guards every map and slice below, and changed is a broadcast
// channel that is closed and replaced whenever watermark advances or fatal is
// set, so a barrier waiter can select on it alongside its own context. A
// condition variable cannot do that — sync.Cond has no cancellable wait — which
// is why the broadcast is a channel.
type orderedView struct {
	namespace string
	spec      orderedStreamSpec
	seam      orderedSeam
	stats     *orderedStats
	retryBase time.Duration

	// cancel stops the subscription goroutine and done closes when it has
	// exited. Together they are the shutdown seam Close drives, and the one
	// F4.4's TestStoreCloseStopsOrderedView asserts against.
	cancel context.CancelFunc
	done   chan struct{}

	mu        sync.Mutex
	changed   chan struct{}
	watermark uint64
	// fatal is sticky for this view instance: a stream that currently cannot
	// serve this layout, or a record payload that cannot be decoded. Both make a
	// page from this view a lie by omission, so waiters fail. A stopped
	// config-fatal view may be replaced after operator repair; corrupt payloads
	// remain sticky because rebuilding would deterministically decode the same
	// authoritative bytes.
	fatal error

	// records is the current version of every identity in the namespace,
	// INCLUDING tombstones: ListOrdered is the immutable acceptance-order stream
	// and must return them.
	records map[orderedIdentity]storage.OrderedRecord
	// order indexes each order scope's identities ascending by immutable Order.
	// Order is strictly increasing within its scope but neither contiguous nor
	// 1-based (a JetStream sequence is a legitimate allocator), so this is a
	// sorted slice searched by value, never an array indexed by order.
	order map[string][]orderedIdentity
	// ranked indexes each ranking scope's live, ranked identities under
	// orderedRankedBefore.
	ranked map[string][]orderedIdentity
	// due indexes the namespace's live, DueAt identities under orderedDueBefore.
	due []orderedIdentity
}

func newOrderedView(namespace string, spec orderedStreamSpec, seam orderedSeam, stats *orderedStats, retryBase time.Duration) *orderedView {
	return &orderedView{
		namespace: namespace,
		spec:      spec,
		seam:      seam,
		stats:     stats,
		retryBase: retryBase,
		done:      make(chan struct{}),
		changed:   make(chan struct{}),
		records:   map[orderedIdentity]storage.OrderedRecord{},
		order:     map[string][]orderedIdentity{},
		ranked:    map[string][]orderedIdentity{},
	}
}

// start launches the view's single subscription goroutine. Exactly one goroutine
// exists per open namespace, and stop is the only way it ends.
func (v *orderedView) start() {
	ctx, cancel := context.WithCancel(context.Background())
	v.cancel = cancel
	go v.run(ctx)
}

// stop cancels the subscription and waits for the goroutine to exit, so a caller
// that closes the store can prove no view goroutine outlives it.
func (v *orderedView) stop(ctx context.Context) error {
	v.cancel()
	select {
	case <-v.done:
		return nil
	case <-ctx.Done():
		return &OrderedViewStopTimeoutError{Namespace: v.namespace, Cause: ctx.Err()}
	}
}

// run keeps a subscription attached for the life of the view. A failed attach is
// retried with the same jittered backoff the create path uses; a stream that
// cannot serve this layout is fatal for this view and ends its goroutine. After
// an operator repair, a later query may replace the stopped view rather than
// retrying forever inside this goroutine.
//
// Re-attaching restarts from DeliverLastPerSubject, which is a COMPLETE refresh
// of the namespace's current state, so a gap in the subscription cannot leave
// the view holding a stale record. Redelivery of a head the view already has is
// a no-op (see applyRecord).
func (v *orderedView) run(ctx context.Context) {
	defer close(v.done)
	attempt := 0
	for {
		err := v.seam.watchStream(ctx, v.spec, v.applyMessage)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			var configErr *OrderedStreamConfigError
			if errors.As(err, &configErr) {
				v.setFatal(err)
				return
			}
			attempt++
		} else {
			attempt = 0
		}
		if waitErr := orderedBackoff(ctx, v.retryBase, attempt-1, orderedViewRetryMaxShift); waitErr != nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// applyMessage folds one stream message into the view. It is the ONLY writer of
// view state; every mutation the store performs reaches the view the same way a
// remote process's mutation does, through the stream.
func (v *orderedView) applyMessage(subject string, seq uint64, data []byte) {
	v.stats.messagesApplied.Add(1)
	family, err := classifyOrderedSubject(v.namespace, subject)
	if err != nil {
		v.setFatal(err)
		return
	}
	if family == orderedRecordSubjectFamily {
		record, err := decodeOrderedRecord(subject, data)
		if err != nil {
			// A record subject whose payload will not decode is authoritative
			// state this process cannot represent. Skipping it would serve pages
			// that silently omit a record, which is exactly the failure mode the
			// codec's fail-closed rule exists to prevent, so the view refuses to
			// answer for this namespace until the stream is repaired.
			v.setFatal(err)
			return
		}
		v.applyRecord(record)
	}
	// Counter subjects, and any family this layout does not define, carry no
	// view state — but they DO carry stream sequences a barrier waits for, so
	// they advance the watermark like anything else.
	v.advance(seq)
}

// applyRecord upserts one record and repositions it in every index it belongs
// to. A revision no newer than the one already held is ignored, which is what
// makes redelivery — inherent to re-attaching a subscription — idempotent.
func (v *orderedView) applyRecord(record storage.OrderedRecord) {
	key := orderedIdentity{orderingScope: record.ID.OrderingScope, stableKey: record.ID.StableKey}
	v.mu.Lock()
	defer v.mu.Unlock()
	previous, had := v.records[key]
	if had && record.Revision <= previous.Revision {
		return
	}
	if had {
		// Remove against the PREVIOUS record, while v.records still holds it, so
		// the search runs against the keys the index was actually sorted by.
		v.removeCurrentLocked(key, previous)
	}
	v.records[key] = record
	if !had {
		// Order is immutable, so an existing record never moves in this index.
		v.insertOrderLocked(key, record)
	}
	v.insertCurrentLocked(key, record)
}

// advance raises the watermark and wakes every barrier waiter. The watermark is
// monotonic: a re-attach replays heads with sequences the view has already
// passed, and a barrier that had been satisfied must never become unsatisfied.
func (v *orderedView) advance(seq uint64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if seq <= v.watermark {
		return
	}
	v.watermark = seq
	v.broadcastLocked()
}

func (v *orderedView) setFatal(err error) {
	v.mu.Lock()
	if v.fatal == nil {
		v.fatal = err
		v.broadcastLocked()
	}
	v.mu.Unlock()
	// This view instance can no longer answer, so it must not keep a server-side
	// consumer alive either. Cancelling here is safe outside the lock and
	// idempotent; a repairable replacement starts only after done closes.
	v.cancel()
}

func (v *orderedView) fatalError() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.fatal
}

// broadcastLocked wakes every barrier waiter by closing the current generation
// channel and installing the next one. The caller must hold v.mu.
func (v *orderedView) broadcastLocked() {
	close(v.changed)
	v.changed = make(chan struct{})
}

func (v *orderedView) currentWatermark() uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.watermark
}

func (v *orderedView) recordCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.records)
}

// waitBarrier blocks until the view has applied every message through target.
// This is the "caught up before read" guarantee: target is the stream tip the
// caller captured before the query began, so returning means the view reflects
// at least the state that existed at the start of the call.
//
// It always terminates. MaxMsgsPerSubject=1 removes a message only when a NEWER
// message is published on the same subject, and that replacement carries a
// higher sequence, so the sequence the view can reach is never capped below a
// tip that once existed.
//
// A target of zero — an absent or empty stream — is satisfied immediately,
// without waiting for the view to have attached. That is not a shortcut: a
// stream holding nothing has no record a page could omit, so an empty page is
// the complete and correct answer, and blocking on a subscription to a stream
// that may not even exist yet would turn every read of an unwritten namespace
// into a wait for a deadline.
func (v *orderedView) waitBarrier(ctx context.Context, target uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		v.mu.Lock()
		if v.fatal != nil {
			err := v.fatal
			v.mu.Unlock()
			return err
		}
		if v.watermark >= target {
			v.mu.Unlock()
			return nil
		}
		changed := v.changed
		v.mu.Unlock()

		select {
		case <-changed:
		case <-v.done:
			// The view stopped without reaching the barrier, so the only honest
			// answer is the reason it stopped, or the closure itself.
			v.mu.Lock()
			err := v.fatal
			v.mu.Unlock()
			if err != nil {
				return err
			}
			return &OrderedStoreClosedError{Namespace: v.namespace}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// --- sorted index maintenance and search ------------------------------------

// lowerBoundLocked returns the first index whose record does NOT sort before
// probe: the position probe belongs at. The caller must hold v.mu.
func (v *orderedView) lowerBoundLocked(entries []orderedIdentity, before orderedComparator, probe storage.OrderedRecord) int {
	low, high := 0, len(entries)
	for low < high {
		middle := low + (high-low)/2
		v.stats.indexVisited.Add(1)
		if before(v.records[entries[middle]], probe) {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

// upperBoundLocked returns the first index whose record sorts strictly AFTER
// probe. It is the cursor-resume search: a cursor names a position, the page
// after it starts at the first record beyond that position, and the comparison
// is the same orderedComparator that built the index. The caller must hold v.mu.
func (v *orderedView) upperBoundLocked(entries []orderedIdentity, before orderedComparator, probe storage.OrderedRecord) int {
	low, high := 0, len(entries)
	for low < high {
		middle := low + (high-low)/2
		v.stats.indexVisited.Add(1)
		if before(probe, v.records[entries[middle]]) {
			high = middle
		} else {
			low = middle + 1
		}
	}
	return low
}

// locateLocked finds key's exact position in a sorted index. Both sort keys are
// injective over the identities in one index — (rank, stable_key,
// ordering_scope) and (due_at, stable_key, ordering_scope) each determine the
// identity within a fixed namespace and scope — so the lower bound already IS
// the entry, and the forward scan below can advance only past records that
// compare equal to it, of which there are none. It is written as a scan anyway
// so the code does not depend on that argument being restated correctly at every
// call site. The caller must hold v.mu.
func (v *orderedView) locateLocked(entries []orderedIdentity, before orderedComparator, record storage.OrderedRecord, key orderedIdentity) (int, bool) {
	at := v.lowerBoundLocked(entries, before, record)
	for at < len(entries) && !before(record, v.records[entries[at]]) {
		if entries[at] == key {
			return at, true
		}
		at++
	}
	return 0, false
}

// removeCurrentLocked takes a record out of the two current-state indexes it may
// occupy. The caller must hold v.mu, and v.records must still hold previous.
func (v *orderedView) removeCurrentLocked(key orderedIdentity, previous storage.OrderedRecord) {
	if !previous.Deleted && previous.Rank.Ranked {
		entries := v.ranked[previous.RankingScope]
		if at, ok := v.locateLocked(entries, orderedRankedBefore, previous, key); ok {
			v.ranked[previous.RankingScope] = slices.Delete(entries, at, at+1)
		}
	}
	if !previous.Deleted && previous.Due.State == storage.DueAt {
		if at, ok := v.locateLocked(v.due, orderedDueBefore, previous, key); ok {
			v.due = slices.Delete(v.due, at, at+1)
		}
	}
}

// insertCurrentLocked places a record into the current-state indexes it belongs
// to. A tombstone belongs to neither: it is unranked and canonically not-due, so
// deleting a record removes it from both views while leaving the acceptance-order
// stream untouched. The caller must hold v.mu.
//
// The Deleted guards below are redundant TODAY and kept deliberately. Delete
// clears Rank and Due, and decodeOrderedRecord runs ValidateOrderedRecord, which
// rejects a deleted record carrying either — so no tombstone can reach these
// branches with Ranked or DueAt set, and no test can distinguish their removal.
// They state the rule the indexes depend on at the point that depends on it,
// rather than relying on a validator three call frames away staying that strict.
func (v *orderedView) insertCurrentLocked(key orderedIdentity, record storage.OrderedRecord) {
	if !record.Deleted && record.Rank.Ranked {
		entries := v.ranked[record.RankingScope]
		at := v.lowerBoundLocked(entries, orderedRankedBefore, record)
		v.ranked[record.RankingScope] = slices.Insert(entries, at, key)
	}
	if !record.Deleted && record.Due.State == storage.DueAt {
		at := v.lowerBoundLocked(v.due, orderedDueBefore, record)
		v.due = slices.Insert(v.due, at, key)
	}
}

// insertOrderLocked places a newly seen identity in its order scope's ascending
// acceptance stream. The caller must hold v.mu.
func (v *orderedView) insertOrderLocked(key orderedIdentity, record storage.OrderedRecord) {
	entries := v.order[key.orderingScope]
	at := v.orderLowerBoundLocked(entries, record.Order)
	v.order[key.orderingScope] = slices.Insert(entries, at, key)
}

// orderLowerBoundLocked returns the first index whose immutable order is at
// least order. It compares one scalar rather than a tuple, so it is not a second
// statement of either frozen sort key. The caller must hold v.mu.
func (v *orderedView) orderLowerBoundLocked(entries []orderedIdentity, order uint64) int {
	low, high := 0, len(entries)
	for low < high {
		middle := low + (high-low)/2
		v.stats.indexVisited.Add(1)
		if v.records[entries[middle]].Order < order {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

// dueEndLocked returns the first index whose due time is later than bound, which
// is where an inclusive ListDue page must stop. The caller must hold v.mu.
func (v *orderedView) dueEndLocked(bound int64) int {
	low, high := 0, len(v.due)
	for low < high {
		middle := low + (high-low)/2
		v.stats.indexVisited.Add(1)
		if v.records[v.due[middle]].Due.UnixMillis <= bound {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

// collectLocked copies a run of index entries out as caller-owned records. The
// caller must hold v.mu.
func (v *orderedView) collectLocked(entries []orderedIdentity) []storage.OrderedRecord {
	records := make([]storage.OrderedRecord, 0, len(entries))
	for _, key := range entries {
		v.stats.indexVisited.Add(1)
		v.stats.recordsCopied.Add(1)
		record := v.records[key]
		record.Value = bytes.Clone(record.Value)
		records = append(records, record)
	}
	return records
}

// --- listings ---------------------------------------------------------------

var _ storage.OrderedIndex = (*orderedStore)(nil)

// orderedNamespaceSpec resolves a validated namespace's stream and subject
// filter. It is orderedLocation's namespace-only half, for the read paths, which
// have no identity to resolve.
func orderedNamespaceSpec(namespace string) (orderedStreamSpec, error) {
	stream, err := orderedStreamName(namespace)
	if err != nil {
		return orderedStreamSpec{}, err
	}
	filter, err := orderedSubjectFilter(namespace)
	if err != nil {
		return orderedStreamSpec{}, err
	}
	return orderedStreamSpec{stream: stream, subjectFilter: filter}, nil
}

// view returns the namespace's view, starting it on first use. One view, one
// goroutine, one subscription per open namespace. A healthy or corrupt-fatal
// view stays registered until Close; a stopped config-fatal view may be retired
// after operator repair.
func (s *orderedStore) view(namespace string) (*orderedView, error) {
	view, _, err := s.acquireView(namespace)
	return view, err
}

// acquireView returns the namespace view and whether this call created it.
// Readiness uses that provenance to distinguish a fatal inherited from a prior
// query from one latched immediately by the goroutine this query just started.
func (s *orderedStore) acquireView(namespace string) (*orderedView, bool, error) {
	spec, err := orderedNamespaceSpec(namespace)
	if err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false, &OrderedStoreClosedError{Namespace: namespace}
	}
	if view, ok := s.views[namespace]; ok {
		return view, false, nil
	}
	view := newOrderedView(namespace, spec, s.seam, s.stats, s.viewRetryBase)
	s.views[namespace] = view
	view.start()
	return view, true, nil
}

// readyView returns a view that has applied through the stream tip observed at
// the start of this call. Capturing the tip BEFORE waiting is what makes the
// guarantee meaningful: a tip captured afterwards would only prove the view was
// caught up with some earlier moment.
func (s *orderedStore) readyView(ctx context.Context, namespace string) (*orderedView, error) {
	view, created, err := s.acquireView(namespace)
	if err != nil {
		return nil, err
	}
	return s.readyAcquiredView(ctx, namespace, view, created)
}

// readyAcquiredView makes one acquired view ready. It is split from readyView
// so acquisition provenance remains explicit and scheduling cannot make a new
// view's fast fatal look inherited.
func (s *orderedStore) readyAcquiredView(ctx context.Context, namespace string, view *orderedView, created bool) (*orderedView, error) {
	const maxRetiredViewRestarts = 1
	var err error
	for restarts := 0; ; {
		// A fatal already present when this query acquires the view belongs to a
		// previous query only when acquisition says the view already existed.
		// Retire it and bootstrap once in this same call, so operator repair takes
		// effect on the next query without hiding a newly-created view's error.
		if fatal := view.fatalError(); fatal != nil {
			if !created && restarts < maxRetiredViewRestarts {
				outcome, retireErr := s.evictRepairableFatal(ctx, namespace, view, fatal)
				if retireErr != nil {
					return nil, retireErr
				}
				if outcome == orderedViewRetired || outcome == orderedViewSuperseded {
					restarts++
					view, created, err = s.acquireView(namespace)
					if err != nil {
						return nil, err
					}
					continue
				}
			}
			return nil, fatal
		}
		s.stats.streamTipReads.Add(1)
		tip, err := s.seam.streamTip(ctx, view.spec.stream)
		if err != nil {
			return nil, err
		}
		if err := view.waitBarrier(ctx, tip); err != nil {
			_, retireErr := s.evictRepairableFatal(ctx, namespace, view, err)
			if retireErr != nil {
				return nil, retireErr
			}
			return nil, err
		}
		return view, nil
	}
}

type orderedViewRetireResult uint8

const (
	orderedViewNotRetired orderedViewRetireResult = iota
	orderedViewRetired
	orderedViewSuperseded
)

// evictRepairableFatal drops only a view whose stream configuration can be
// repaired out of band. Corrupt record bytes remain sticky: rebuilding from
// the same authoritative payload would deterministically fail again and should
// not create an unbounded subscription loop.
func (s *orderedStore) evictRepairableFatal(ctx context.Context, namespace string, view *orderedView, err error) (orderedViewRetireResult, error) {
	var configErr *OrderedStreamConfigError
	if !errors.As(err, &configErr) {
		return orderedViewNotRetired, nil
	}
	// setFatal wakes barrier waiters before it cancels the view. Do not remove the
	// last lifecycle handle until run has completed: otherwise a replacement can
	// overlap it and a concurrent Close cannot wait for the retired goroutine.
	// If this query gives up first, retain the view and report its context error.
	select {
	case <-view.done:
	case <-ctx.Done():
		return orderedViewNotRetired, ctx.Err()
	}
	s.mu.Lock()
	if s.views[namespace] == view {
		delete(s.views, namespace)
		s.mu.Unlock()
		return orderedViewRetired, nil
	}
	s.mu.Unlock()
	return orderedViewSuperseded, nil
}

// queryStats reports the query-work counters. It exists for this module's own
// bounded-work assertions and for the counter probe F4.4 supplies to the storage
// conformance suite.
func (s *orderedStore) queryStats() orderedQueryStats { return s.stats.snapshot() }

// Close stops every open namespace view. It is idempotent, and it waits for each
// goroutine to exit so a caller can prove none outlives the store. F4.4 wires it
// into the composite's lifecycle; nothing may start a view afterwards.
func (s *orderedStore) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	views := make([]*orderedView, 0, len(s.views))
	for _, view := range s.views {
		views = append(views, view)
	}
	clear(s.views)
	s.mu.Unlock()

	// Cancellation is a separate pass from waiting: an expired Close context
	// must not let one slow view prevent every later view from being stopped.
	for _, view := range views {
		view.cancel()
	}
	errs := make([]error, 0, len(views))
	for _, view := range views {
		if err := view.stop(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ListOrdered pages the immutable acceptance-order stream of one order scope,
// ascending after the exclusive afterOrder. It INCLUDES tombstones: this is the
// acceptance history, not a current view, and a consumer resuming from an order
// cursor has to see that a record was deleted.
func (s *orderedStore) ListOrdered(ctx context.Context, namespace string, orderingScope string, afterOrder uint64, limit int) (storage.OrderedPage, error) {
	if err := storage.ValidateName(namespace); err != nil {
		return storage.OrderedPage{}, err
	}
	if err := storage.ValidateName(orderingScope); err != nil {
		return storage.OrderedPage{}, err
	}
	if err := storage.ValidateOrderedLimit(limit); err != nil {
		return storage.OrderedPage{}, err
	}
	view, err := s.readyView(ctx, namespace)
	if err != nil {
		return storage.OrderedPage{}, err
	}

	view.mu.Lock()
	defer view.mu.Unlock()
	entries := view.order[orderingScope]
	// afterOrder is exclusive, so the page starts at the first order strictly
	// greater than it: the lower bound of afterOrder+1. The maximal order is
	// handled separately because afterOrder+1 would wrap to zero there, and
	// zero is the "start at the beginning" argument.
	start := len(entries)
	if afterOrder != math.MaxUint64 {
		start = view.orderLowerBoundLocked(entries, afterOrder+1)
	}
	end := min(start+limit, len(entries))
	page := storage.OrderedPage{Records: view.collectLocked(entries[start:end])}
	if len(page.Records) > 0 {
		page.NextAfterOrder = page.Records[len(page.Records)-1].Order
	}
	return page, nil
}

// ListRanked pages one ranking scope's current, nondeleted, ranked records in
// descending (rank, stable_key, ordering_scope) order.
//
// Pagination is keyset, over a view that keeps moving. A record whose rank
// crosses the cursor's frozen position between two pages is skipped or returned
// twice; that is inherent to resuming from a position rather than from a
// snapshot, it is what the contract specifies, and callers reconcile by stable
// identity rather than by page.
func (s *orderedStore) ListRanked(ctx context.Context, namespace string, rankingScope string, after storage.RankedCursor, limit int) (storage.RankedPage, error) {
	if err := storage.ValidateName(namespace); err != nil {
		return storage.RankedPage{}, err
	}
	if err := storage.ValidateName(rankingScope); err != nil {
		return storage.RankedPage{}, err
	}
	if err := storage.ValidateOrderedLimit(limit); err != nil {
		return storage.RankedPage{}, err
	}
	probe, resuming, err := decodeRankedCursor(after, namespace, rankingScope)
	if err != nil {
		return storage.RankedPage{}, err
	}
	view, err := s.readyView(ctx, namespace)
	if err != nil {
		return storage.RankedPage{}, err
	}

	view.mu.Lock()
	defer view.mu.Unlock()
	entries := view.ranked[rankingScope]
	start := 0
	if resuming {
		start = view.upperBoundLocked(entries, orderedRankedBefore, probe)
	}
	end := min(start+limit, len(entries))
	page := storage.RankedPage{Records: view.collectLocked(entries[start:end])}
	if end < len(entries) {
		last := page.Records[len(page.Records)-1]
		token, err := encodeOrderedCursor(orderedCursorPayload{
			Kind:          string(storage.RankedCursorKind),
			Namespace:     namespace,
			RankingScope:  rankingScope,
			Rank:          last.Rank.Value,
			StableKey:     string(last.ID.StableKey),
			OrderingScope: last.ID.OrderingScope,
		})
		if err != nil {
			return storage.RankedPage{}, err
		}
		page.NextCursor = storage.RankedCursor(token)
	}
	return page, nil
}

// ListDue pages the namespace's current, nondeleted, DueAt records with a due
// time no later than dueAtOrBefore, ascending by (due_at, stable_key,
// ordering_scope). The same keyset caveat as ListRanked applies to a record
// whose due time moves across the cursor between pages.
func (s *orderedStore) ListDue(ctx context.Context, namespace string, dueAtOrBefore int64, after storage.DueCursor, limit int) (storage.DuePage, error) {
	if err := storage.ValidateName(namespace); err != nil {
		return storage.DuePage{}, err
	}
	if err := storage.ValidateOrderedLimit(limit); err != nil {
		return storage.DuePage{}, err
	}
	probe, resuming, err := decodeDueCursor(after, namespace, dueAtOrBefore)
	if err != nil {
		return storage.DuePage{}, err
	}
	view, err := s.readyView(ctx, namespace)
	if err != nil {
		return storage.DuePage{}, err
	}

	view.mu.Lock()
	defer view.mu.Unlock()
	start := 0
	if resuming {
		start = view.upperBoundLocked(view.due, orderedDueBefore, probe)
	}
	eligible := view.dueEndLocked(dueAtOrBefore)
	end := min(start+limit, eligible)
	page := storage.DuePage{Records: view.collectLocked(view.due[start:end])}
	if end < eligible {
		last := page.Records[len(page.Records)-1]
		token, err := encodeOrderedCursor(orderedCursorPayload{
			Kind:          string(storage.DueCursorKind),
			Namespace:     namespace,
			DueBound:      dueAtOrBefore,
			DueAt:         last.Due.UnixMillis,
			StableKey:     string(last.ID.StableKey),
			OrderingScope: last.ID.OrderingScope,
		})
		if err != nil {
			return storage.DuePage{}, err
		}
		page.NextCursor = storage.DueCursor(token)
	}
	return page, nil
}

// decodeRankedCursor turns an opaque token into the PROBE RECORD the ranked
// comparator searches for. Returning a record rather than a bespoke position
// type is deliberate: it is what lets the resume path use orderedRankedBefore
// itself, so the descending sort key — including both tiebreakers — is written
// down exactly once in this package.
//
// Every binding is re-checked against the live request. The token is never
// trusted for namespace or ranking scope; it carries a position, not authority.
func decodeRankedCursor(cursor storage.RankedCursor, namespace, rankingScope string) (storage.OrderedRecord, bool, error) {
	if cursor == "" {
		return storage.OrderedRecord{}, false, nil
	}
	payload, err := decodeOrderedCursorPayload(storage.RankedCursorKind, string(cursor))
	if err != nil {
		return storage.OrderedRecord{}, false, err
	}
	if payload.Namespace != namespace || payload.RankingScope != rankingScope {
		return storage.OrderedRecord{}, false, storage.NewInvalidOrderedCursorError(
			storage.RankedCursorKind, string(cursor), storage.OrderedCursorQueryMismatch)
	}
	return storage.OrderedRecord{
		ID: storage.OrderedID{
			Namespace:     payload.Namespace,
			OrderingScope: payload.OrderingScope,
			StableKey:     storage.StableKey(payload.StableKey),
		},
		RankingScope: payload.RankingScope,
		Rank:         storage.Rank{Ranked: true, Value: payload.Rank},
	}, true, nil
}

// decodeDueCursor is decodeRankedCursor's ascending counterpart. The fixed due
// bound is part of the binding: a page issued under one bound resumes only under
// that same bound, because the eligible end of the index moves with it.
func decodeDueCursor(cursor storage.DueCursor, namespace string, dueAtOrBefore int64) (storage.OrderedRecord, bool, error) {
	if cursor == "" {
		return storage.OrderedRecord{}, false, nil
	}
	payload, err := decodeOrderedCursorPayload(storage.DueCursorKind, string(cursor))
	if err != nil {
		return storage.OrderedRecord{}, false, err
	}
	if payload.Namespace != namespace || payload.DueBound != dueAtOrBefore || payload.DueAt > dueAtOrBefore {
		return storage.OrderedRecord{}, false, storage.NewInvalidOrderedCursorError(
			storage.DueCursorKind, string(cursor), storage.OrderedCursorQueryMismatch)
	}
	return storage.OrderedRecord{
		ID: storage.OrderedID{
			Namespace:     payload.Namespace,
			OrderingScope: payload.OrderingScope,
			StableKey:     storage.StableKey(payload.StableKey),
		},
		Due: storage.Due{State: storage.DueAt, UnixMillis: payload.DueAt},
	}, true, nil
}
