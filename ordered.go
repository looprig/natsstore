package natsstore

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strconv"

	"github.com/looprig/storage"
)

// orderedCreateAttempts bounds the optimistic retry loop Create runs when it
// loses the per-scope counter race. Each attempt costs one head read and one
// rejected batch, and every rejection means some other writer made progress, so
// the loop is livelock-free; the bound exists only so a pathologically contended
// scope fails loudly instead of spinning forever.
const orderedCreateAttempts = 64

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
// counter within the bounded retry budget.
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
	publish(ctx context.Context, msg orderedMsg) error
	// publishBatch commits every message atomically under its own fence, with the
	// same two sentinels. A rejected batch commits nothing.
	publishBatch(ctx context.Context, msgs []orderedMsg) error
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
	localPathReporter
}

// newOrderedStore builds an ordered index over seam.
func newOrderedStore(seam orderedSeam) *orderedStore {
	return &orderedStore{seam: seam}
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

	for attempt := 1; attempt <= orderedCreateAttempts; attempt++ {
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
		err = s.seam.publishBatch(ctx, []orderedMsg{
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
				// The identity exists, so a retry of Create is satisfied by the
				// canonical record. Whether this call or a concurrent one wrote it is
				// unknowable, so it is reported as a duplicate.
				return winner, false, nil
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
	err = s.seam.publish(ctx, orderedMsg{subject: subject, data: payload, expectLastSeq: expectSeq})
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
