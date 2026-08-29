package natsstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/storage"
)

// fakeStoredMsg is one subject's current message in the fake seam. Because every
// ordered subject is configured MaxMsgsPerSubject=1, one entry per subject is a
// faithful model of the server.
type fakeStoredMsg struct {
	seq  uint64
	data []byte
}

// fakeOrderedSeam models the JetStream behaviour the ordered store depends on:
// a monotonically increasing stream sequence, one live message per subject, and
// an all-or-nothing publish whose members each carry their own
// expected-last-subject-sequence precondition. It enforces those preconditions
// for real, so a test that expects a conflict gets one from the model rather
// than from a canned error.
type fakeOrderedSeam struct {
	mu      sync.Mutex
	nextSeq uint64
	msgs    map[string]fakeStoredMsg

	ensured   []orderedStreamSpec
	published [][]orderedMsg

	// hook runs before each publish or batch is applied. It may mutate the fake's
	// state (simulating a concurrent writer) and may return a non-nil error, which
	// the seam returns without applying the messages.
	hook func(f *fakeOrderedSeam, msgs []orderedMsg) error

	ensureErr error

	// getErr, once failGetAfter successful reads have been served, is returned by
	// every subsequent lastMsgForSubject. failGetAfter=0 fails from the first
	// read, which is the common case.
	getErr       error
	failGetAfter int
	gets         int
}

func newFakeOrderedSeam() *fakeOrderedSeam {
	return &fakeOrderedSeam{msgs: map[string]fakeStoredMsg{}}
}

func (f *fakeOrderedSeam) ensureStream(_ context.Context, spec orderedStreamSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensured = append(f.ensured, spec)
	return f.ensureErr
}

func (f *fakeOrderedSeam) lastMsgForSubject(_ context.Context, _ string, subject string) (uint64, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if f.getErr != nil && f.gets > f.failGetAfter {
		return 0, nil, f.getErr
	}
	msg, ok := f.msgs[subject]
	if !ok {
		return 0, nil, nil
	}
	return msg.seq, bytes.Clone(msg.data), nil
}

func (f *fakeOrderedSeam) publish(ctx context.Context, stream string, msg orderedMsg) error {
	return f.publishBatch(ctx, stream, []orderedMsg{msg})
}

func (f *fakeOrderedSeam) publishBatch(_ context.Context, _ string, msgs []orderedMsg) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	recorded := make([]orderedMsg, 0, len(msgs))
	for _, m := range msgs {
		recorded = append(recorded, orderedMsg{subject: m.subject, data: bytes.Clone(m.data), expectLastSeq: m.expectLastSeq})
	}
	f.published = append(f.published, recorded)
	if f.hook != nil {
		if err := f.hook(f, msgs); err != nil {
			return err
		}
	}
	return f.applyLocked(msgs)
}

// applyLocked checks every member's precondition and then commits all of them or
// none. The caller must hold f.mu.
func (f *fakeOrderedSeam) applyLocked(msgs []orderedMsg) error {
	for _, m := range msgs {
		if f.msgs[m.subject].seq != m.expectLastSeq {
			return errOrderedPrecondition
		}
	}
	for _, m := range msgs {
		f.nextSeq++
		f.msgs[m.subject] = fakeStoredMsg{seq: f.nextSeq, data: bytes.Clone(m.data)}
	}
	return nil
}

// setLocked writes subject unconditionally, simulating a concurrent writer. The
// caller must hold f.mu (hooks are called with it held).
func (f *fakeOrderedSeam) setLocked(subject string, data []byte) {
	f.nextSeq++
	f.msgs[subject] = fakeStoredMsg{seq: f.nextSeq, data: bytes.Clone(data)}
}

func (f *fakeOrderedSeam) seqOf(subject string) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.msgs[subject].seq
}

func (f *fakeOrderedSeam) dataOf(subject string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return bytes.Clone(f.msgs[subject].data)
}

func (f *fakeOrderedSeam) publishCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published)
}

func (f *fakeOrderedSeam) publishedAt(i int) []orderedMsg {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.published[i])
}

var _ orderedSeam = (*fakeOrderedSeam)(nil)

func orderedTestID() storage.OrderedID {
	return storage.OrderedID{Namespace: "sessions", OrderingScope: "tenant/a", StableKey: "Session/One.v2"}
}

func orderedSubjects(t *testing.T, id storage.OrderedID) (counter, record string) {
	t.Helper()
	counter, err := orderedCounterSubject(id.Namespace, id.OrderingScope)
	if err != nil {
		t.Fatalf("orderedCounterSubject: %v", err)
	}
	record, err = orderedRecordSubject(id)
	if err != nil {
		t.Fatalf("orderedRecordSubject: %v", err)
	}
	return counter, record
}

// seedOrderedRecord writes rec into the fake at its canonical subject and
// returns the subject's stream sequence.
func seedOrderedRecord(t *testing.T, f *fakeOrderedSeam, rec storage.OrderedRecord) uint64 {
	t.Helper()
	data, err := encodeOrderedRecord(rec)
	if err != nil {
		t.Fatalf("encodeOrderedRecord: %v", err)
	}
	subj, err := orderedRecordSubject(rec.ID)
	if err != nil {
		t.Fatalf("orderedRecordSubject: %v", err)
	}
	f.mu.Lock()
	f.setLocked(subj, data)
	f.mu.Unlock()
	return f.seqOf(subj)
}

func liveOrderedRecord(id storage.OrderedID, revision, order uint64) storage.OrderedRecord {
	return storage.OrderedRecord{
		ID:           id,
		RankingScope: "catalog",
		Revision:     revision,
		Order:        order,
		Due:          storage.Due{State: storage.DueAt, UnixMillis: 1000},
		Rank:         storage.Rank{Ranked: true, Value: 42},
		Value:        []byte("original"),
	}
}

func TestOrderedCreateAllocatesAtomically(t *testing.T) {
	t.Parallel()
	f := newFakeOrderedSeam()
	store := newOrderedStore(f)
	id := orderedTestID()
	counterSubj, recordSubj := orderedSubjects(t, id)

	rec, created, err := store.Create(context.Background(), id, "catalog", []byte("v1"),
		storage.Rank{Ranked: true, Value: 7}, storage.Due{State: storage.DueAt, UnixMillis: 99})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !created {
		t.Fatal("Create reported created=false for a fresh identity")
	}
	if rec.Order != 1 || rec.Revision != 1 {
		t.Fatalf("Create returned order=%d revision=%d, want 1/1", rec.Order, rec.Revision)
	}
	if rec.ID != id || rec.RankingScope != "catalog" || rec.Deleted {
		t.Fatalf("Create returned %+v, want identity %+v ranking scope %q and not deleted", rec, id, "catalog")
	}

	// The stream must have been provisioned for this namespace before the write.
	if len(f.ensured) != 1 {
		t.Fatalf("ensureStream called %d times, want 1", len(f.ensured))
	}
	wantStream, err := orderedStreamName(id.Namespace)
	if err != nil {
		t.Fatalf("orderedStreamName: %v", err)
	}
	wantFilter, err := orderedSubjectFilter(id.Namespace)
	if err != nil {
		t.Fatalf("orderedSubjectFilter: %v", err)
	}
	if f.ensured[0].stream != wantStream || f.ensured[0].subjectFilter != wantFilter {
		t.Fatalf("ensured %+v, want stream %q filter %q", f.ensured[0], wantStream, wantFilter)
	}

	// Exactly one atomic batch, of exactly the counter and the record, each with
	// its own precondition. Both expect 0: neither subject existed.
	if got := f.publishCount(); got != 1 {
		t.Fatalf("publish calls = %d, want 1", got)
	}
	batch := f.publishedAt(0)
	if len(batch) != 2 {
		t.Fatalf("batch has %d messages, want 2 (%+v)", len(batch), batch)
	}
	if batch[0].subject != counterSubj || batch[0].expectLastSeq != 0 {
		t.Fatalf("batch[0] = subject %q expect %d, want %q expect 0", batch[0].subject, batch[0].expectLastSeq, counterSubj)
	}
	if string(batch[0].data) != "1" {
		t.Fatalf("counter payload = %q, want %q", batch[0].data, "1")
	}
	if batch[1].subject != recordSubj || batch[1].expectLastSeq != 0 {
		t.Fatalf("batch[1] = subject %q expect %d, want %q expect 0", batch[1].subject, batch[1].expectLastSeq, recordSubj)
	}
	if batch[0].subject == batch[1].subject {
		t.Fatal("both batch members target the same subject; independent expectations are impossible")
	}

	// The stored record payload names the original stable key, verbatim, so a
	// read can verify it against the hashed subject.
	var stored map[string]json.RawMessage
	if err := json.Unmarshal(f.dataOf(recordSubj), &stored); err != nil {
		t.Fatalf("stored record is not JSON: %v", err)
	}
	var key string
	if err := json.Unmarshal(stored["key"], &key); err != nil {
		t.Fatalf("stored record has no decodable key field: %v", err)
	}
	if key != string(id.StableKey) {
		t.Fatalf("stored key = %q, want the original %q", key, id.StableKey)
	}
	// The raw key must not appear in the subject it is stored under.
	if bytes.Contains([]byte(recordSubj), []byte("Session")) {
		t.Fatalf("record subject %q contains raw stable key bytes", recordSubj)
	}

	// Get returns the same record.
	got, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != rec.ID || got.Order != rec.Order || got.Revision != rec.Revision ||
		got.Rank != rec.Rank || got.Due != rec.Due || !bytes.Equal(got.Value, []byte("v1")) {
		t.Fatalf("Get returned %+v, want %+v", got, rec)
	}
}

func TestOrderedCreateAllocatesSuccessiveOrders(t *testing.T) {
	t.Parallel()
	f := newFakeOrderedSeam()
	store := newOrderedStore(f)
	first := orderedTestID()
	second := first
	second.StableKey = "Session/Two.v2"
	counterSubj, _ := orderedSubjects(t, first)

	if _, _, err := store.Create(context.Background(), first, "catalog", nil, storage.Rank{}, storage.Due{}); err != nil {
		t.Fatalf("Create first: %v", err)
	}
	counterSeq := f.seqOf(counterSubj)
	rec, created, err := store.Create(context.Background(), second, "catalog", nil, storage.Rank{}, storage.Due{})
	if err != nil {
		t.Fatalf("Create second: %v", err)
	}
	if !created || rec.Order != 2 {
		t.Fatalf("second Create: created=%v order=%d, want true/2", created, rec.Order)
	}
	batch := f.publishedAt(1)
	if batch[0].expectLastSeq != counterSeq {
		t.Fatalf("counter expectation = %d, want the observed head %d", batch[0].expectLastSeq, counterSeq)
	}
	if batch[1].expectLastSeq != 0 {
		t.Fatalf("record expectation = %d, want 0 (must not exist)", batch[1].expectLastSeq)
	}
	if string(f.dataOf(counterSubj)) != "2" {
		t.Fatalf("counter = %q, want %q", f.dataOf(counterSubj), "2")
	}
}

func TestOrderedCreateIsIdempotentByIdentity(t *testing.T) {
	t.Parallel()
	f := newFakeOrderedSeam()
	store := newOrderedStore(f)
	id := orderedTestID()
	want := liveOrderedRecord(id, 4, 11)
	seedOrderedRecord(t, f, want)

	// The candidate fields are deliberately invalid: an existing identity is
	// returned without validating them.
	got, created, err := store.Create(context.Background(), id, "NOT A NAME",
		make([]byte, storage.MaxOrderedValueBytes+1),
		storage.Rank{}, storage.Due{State: storage.NotDue, UnixMillis: 5})
	if err != nil {
		t.Fatalf("Create on an existing identity: %v", err)
	}
	if created {
		t.Fatal("Create reported created=true for an existing identity")
	}
	if got.Revision != want.Revision || got.Order != want.Order || got.RankingScope != want.RankingScope ||
		got.Rank != want.Rank || got.Due != want.Due || !bytes.Equal(got.Value, want.Value) {
		t.Fatalf("Create returned %+v, want the stored %+v", got, want)
	}
	if n := f.publishCount(); n != 0 {
		t.Fatalf("Create published %d times for an existing identity, want 0", n)
	}
}

func TestOrderedCreateRetriesAfterCounterConflict(t *testing.T) {
	t.Parallel()
	f := newFakeOrderedSeam()
	store := newOrderedStore(f)
	id := orderedTestID()
	counterSubj, _ := orderedSubjects(t, id)

	var raced bool
	f.hook = func(f *fakeOrderedSeam, _ []orderedMsg) error {
		if raced {
			return nil
		}
		raced = true
		// A concurrent allocator wins order 1 between our read and our commit.
		f.setLocked(counterSubj, []byte("1"))
		return nil
	}

	rec, created, err := store.Create(context.Background(), id, "catalog", []byte("v"), storage.Rank{}, storage.Due{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !created {
		t.Fatal("Create reported created=false after a counter conflict")
	}
	if rec.Order != 2 {
		t.Fatalf("order after losing the race for 1 = %d, want 2", rec.Order)
	}
	if n := f.publishCount(); n != 2 {
		t.Fatalf("publish attempts = %d, want 2 (one rejected, one committed)", n)
	}
	if got := f.publishedAt(1)[0].expectLastSeq; got != f.seqOf(counterSubj)-1 {
		t.Fatalf("retry re-read the counter head as %d; it must re-read after the conflict", got)
	}
	if string(f.dataOf(counterSubj)) != "2" {
		t.Fatalf("counter = %q, want %q", f.dataOf(counterSubj), "2")
	}
}

func TestOrderedCreateReturnsDuplicateAfterRecordConflict(t *testing.T) {
	t.Parallel()
	f := newFakeOrderedSeam()
	store := newOrderedStore(f)
	id := orderedTestID()
	winner := liveOrderedRecord(id, 1, 5)

	f.hook = func(f *fakeOrderedSeam, msgs []orderedMsg) error {
		f.hook = nil
		// A concurrent creator commits the same identity first.
		data, err := encodeOrderedRecord(winner)
		if err != nil {
			return err
		}
		f.setLocked(msgs[1].subject, data)
		return nil
	}

	got, created, err := store.Create(context.Background(), id, "catalog", []byte("mine"),
		storage.Rank{Ranked: true, Value: 1}, storage.Due{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created {
		t.Fatal("Create reported created=true after losing the record race")
	}
	if got.Order != winner.Order || got.Revision != winner.Revision || !bytes.Equal(got.Value, winner.Value) {
		t.Fatalf("Create returned %+v, want the winner %+v", got, winner)
	}
}

func TestOrderedCreateAmbiguity(t *testing.T) {
	t.Parallel()
	id := orderedTestID()

	t.Run("record landed", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		winner := liveOrderedRecord(id, 1, 9)
		f.hook = func(f *fakeOrderedSeam, msgs []orderedMsg) error {
			f.hook = nil
			data, err := encodeOrderedRecord(winner)
			if err != nil {
				return err
			}
			f.setLocked(msgs[1].subject, data)
			return errOrderedAmbiguous
		}
		got, created, err := store.Create(context.Background(), id, "catalog", []byte("v"), storage.Rank{}, storage.Due{})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if created {
			t.Fatal("created=true after an ambiguous commit resolved to an existing record")
		}
		if got.Order != winner.Order {
			t.Fatalf("Create returned order %d, want the resolved %d", got.Order, winner.Order)
		}
	})

	t.Run("record absent", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		f.hook = func(f *fakeOrderedSeam, _ []orderedMsg) error { return errOrderedAmbiguous }
		_, created, err := store.Create(context.Background(), id, "catalog", []byte("v"), storage.Rank{}, storage.Due{})
		if created {
			t.Fatal("created=true after an unresolvable ambiguous commit")
		}
		var ambiguous *storage.OrderedAmbiguousError
		if !errors.As(err, &ambiguous) {
			t.Fatalf("error = %v (%T), want *storage.OrderedAmbiguousError", err, err)
		}
		if ambiguous.Operation != storage.OrderedCreateOperation || ambiguous.ID != id {
			t.Fatalf("ambiguous error = %+v, want operation %q and id %+v", ambiguous, storage.OrderedCreateOperation, id)
		}
	})
}

func TestOrderedCreateValidatesInputs(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		id           storage.OrderedID
		rankingScope string
		value        []byte
		due          storage.Due
	}{
		{name: "invalid namespace", id: storage.OrderedID{Namespace: "NS", OrderingScope: "s", StableKey: "k"}, rankingScope: "r"},
		{name: "invalid ordering scope", id: storage.OrderedID{Namespace: "n", OrderingScope: "", StableKey: "k"}, rankingScope: "r"},
		{name: "invalid stable key", id: storage.OrderedID{Namespace: "n", OrderingScope: "s", StableKey: ""}, rankingScope: "r"},
		{name: "invalid ranking scope", id: orderedTestID(), rankingScope: "Ranks"},
		{name: "oversize value", id: orderedTestID(), rankingScope: "r", value: make([]byte, storage.MaxOrderedValueBytes+1)},
		{name: "invalid due", id: orderedTestID(), rankingScope: "r", due: storage.Due{State: storage.NotDue, UnixMillis: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFakeOrderedSeam()
			store := newOrderedStore(f)
			if _, _, err := store.Create(context.Background(), tc.id, tc.rankingScope, tc.value, storage.Rank{}, tc.due); err == nil {
				t.Fatal("Create accepted an invalid input")
			}
			if n := f.publishCount(); n != 0 {
				t.Fatalf("Create published %d times after rejecting an input, want 0", n)
			}
		})
	}
}

func TestOrderedGet(t *testing.T) {
	t.Parallel()
	id := orderedTestID()

	t.Run("absent", func(t *testing.T) {
		t.Parallel()
		store := newOrderedStore(newFakeOrderedSeam())
		_, err := store.Get(context.Background(), id)
		var notFound *storage.OrderedRecordNotFoundError
		if !errors.As(err, &notFound) {
			t.Fatalf("error = %v (%T), want *storage.OrderedRecordNotFoundError", err, err)
		}
	})

	t.Run("tombstone is returned", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		tomb := liveOrderedRecord(id, 2, 3)
		tomb.Deleted = true
		tomb.Rank = storage.Rank{}
		tomb.Due = storage.Due{State: storage.NotDue}
		seedOrderedRecord(t, f, tomb)
		got, err := store.Get(context.Background(), id)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !got.Deleted || got.Revision != 2 || got.Order != 3 {
			t.Fatalf("Get returned %+v, want the tombstone %+v", got, tomb)
		}
	})

	// A payload whose own identity differs from the hashed subject it is stored
	// under must fail closed rather than be returned.
	t.Run("forged payload fails closed", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		forged := liveOrderedRecord(id, 1, 1)
		forged.ID.StableKey = "someone-elses-key"
		data, err := encodeOrderedRecord(forged)
		if err != nil {
			t.Fatalf("encodeOrderedRecord: %v", err)
		}
		_, recordSubj := orderedSubjects(t, id)
		f.mu.Lock()
		f.setLocked(recordSubj, data)
		f.mu.Unlock()

		got, err := store.Get(context.Background(), id)
		if err == nil {
			t.Fatalf("Get returned a forged record: %+v", got)
		}
		var mismatch *OrderedIdentityMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("error = %v (%T), want *OrderedIdentityMismatchError", err, err)
		}
	})

	t.Run("invalid identity", func(t *testing.T) {
		t.Parallel()
		store := newOrderedStore(newFakeOrderedSeam())
		if _, err := store.Get(context.Background(), storage.OrderedID{Namespace: "n", OrderingScope: "s"}); err == nil {
			t.Fatal("Get accepted an empty stable key")
		}
	})
}

func TestOrderedUpdate(t *testing.T) {
	t.Parallel()
	id := orderedTestID()

	t.Run("advances one revision under the record fence", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		before := liveOrderedRecord(id, 3, 11)
		seq := seedOrderedRecord(t, f, before)

		got, err := store.Update(context.Background(), id, 3, []byte("next"),
			storage.Rank{Ranked: true, Value: -8}, storage.Due{State: storage.NotDue})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		if got.Revision != 4 {
			t.Fatalf("revision = %d, want 4", got.Revision)
		}
		if got.Order != before.Order || got.RankingScope != before.RankingScope || got.ID != before.ID {
			t.Fatalf("Update changed immutable state: %+v, want order/scope/id from %+v", got, before)
		}
		if got.Rank != (storage.Rank{Ranked: true, Value: -8}) || got.Due != (storage.Due{State: storage.NotDue}) {
			t.Fatalf("Update returned rank %+v due %+v, want the candidates", got.Rank, got.Due)
		}
		if !bytes.Equal(got.Value, []byte("next")) {
			t.Fatalf("value = %q, want %q", got.Value, "next")
		}
		if n := f.publishCount(); n != 1 {
			t.Fatalf("publish calls = %d, want 1", n)
		}
		batch := f.publishedAt(0)
		if len(batch) != 1 {
			t.Fatalf("Update published %d messages, want 1", len(batch))
		}
		_, recordSubj := orderedSubjects(t, id)
		if batch[0].subject != recordSubj || batch[0].expectLastSeq != seq {
			t.Fatalf("Update published to %q expect %d, want %q expect the observed head %d",
				batch[0].subject, batch[0].expectLastSeq, recordSubj, seq)
		}
	})

	t.Run("stale revision does not mutate", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		before := liveOrderedRecord(id, 3, 11)
		seq := seedOrderedRecord(t, f, before)
		_, recordSubj := orderedSubjects(t, id)
		stored := f.dataOf(recordSubj)

		_, err := store.Update(context.Background(), id, 2, []byte("next"), storage.Rank{}, storage.Due{})
		var conflict *storage.OrderedRevisionConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("error = %v (%T), want *storage.OrderedRevisionConflictError", err, err)
		}
		if conflict.ExpectedRevision != 2 || conflict.ActualRevision != 3 || conflict.ID != id {
			t.Fatalf("conflict = %+v, want expected 2 actual 3 for %+v", conflict, id)
		}
		if n := f.publishCount(); n != 0 {
			t.Fatalf("Update published %d times on a stale revision, want 0", n)
		}
		if f.seqOf(recordSubj) != seq || !bytes.Equal(f.dataOf(recordSubj), stored) {
			t.Fatal("the stored record changed after a rejected Update")
		}
	})

	t.Run("candidate is validated before the revision compare", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		seedOrderedRecord(t, f, liveOrderedRecord(id, 3, 11))
		// Stale revision AND an invalid due state: the contract requires the
		// validation error, not the conflict.
		_, err := store.Update(context.Background(), id, 1, nil, storage.Rank{},
			storage.Due{State: storage.NotDue, UnixMillis: 7})
		var conflict *storage.OrderedRevisionConflictError
		if errors.As(err, &conflict) {
			t.Fatalf("error = %v, want the candidate validation error, not a conflict", err)
		}
		var invalidDue *storage.InvalidDueError
		if !errors.As(err, &invalidDue) {
			t.Fatalf("error = %v (%T), want *storage.InvalidDueError", err, err)
		}
	})

	t.Run("absent record", func(t *testing.T) {
		t.Parallel()
		store := newOrderedStore(newFakeOrderedSeam())
		_, err := store.Update(context.Background(), id, 1, nil, storage.Rank{}, storage.Due{})
		var notFound *storage.OrderedRecordNotFoundError
		if !errors.As(err, &notFound) {
			t.Fatalf("error = %v (%T), want *storage.OrderedRecordNotFoundError", err, err)
		}
	})

	t.Run("tombstone cannot be resurrected", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		tomb := liveOrderedRecord(id, 5, 11)
		tomb.Deleted = true
		tomb.Rank = storage.Rank{}
		tomb.Due = storage.Due{State: storage.NotDue}
		seedOrderedRecord(t, f, tomb)
		_, err := store.Update(context.Background(), id, 5, []byte("back"), storage.Rank{}, storage.Due{})
		var deleted *storage.OrderedDeletedError
		if !errors.As(err, &deleted) {
			t.Fatalf("error = %v (%T), want *storage.OrderedDeletedError", err, err)
		}
		if n := f.publishCount(); n != 0 {
			t.Fatalf("Update published %d times against a tombstone, want 0", n)
		}
	})

	t.Run("revision exhausted", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		seedOrderedRecord(t, f, liveOrderedRecord(id, math.MaxUint64, 11))
		_, err := store.Update(context.Background(), id, math.MaxUint64, nil, storage.Rank{}, storage.Due{})
		var exhausted *storage.OrderedRevisionExhaustedError
		if !errors.As(err, &exhausted) {
			t.Fatalf("error = %v (%T), want *storage.OrderedRevisionExhaustedError", err, err)
		}
		if n := f.publishCount(); n != 0 {
			t.Fatalf("Update published %d times with an exhausted revision, want 0", n)
		}
	})

	t.Run("concurrent write reported as a conflict", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		seedOrderedRecord(t, f, liveOrderedRecord(id, 3, 11))
		f.hook = func(f *fakeOrderedSeam, msgs []orderedMsg) error {
			f.hook = nil
			winner := liveOrderedRecord(id, 4, 11)
			data, err := encodeOrderedRecord(winner)
			if err != nil {
				return err
			}
			f.setLocked(msgs[0].subject, data)
			return nil
		}
		_, err := store.Update(context.Background(), id, 3, []byte("mine"), storage.Rank{}, storage.Due{})
		var conflict *storage.OrderedRevisionConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("error = %v (%T), want *storage.OrderedRevisionConflictError", err, err)
		}
		if conflict.ActualRevision != 4 {
			t.Fatalf("conflict.ActualRevision = %d, want 4", conflict.ActualRevision)
		}
	})

	t.Run("ambiguity resolves by reading back", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		seedOrderedRecord(t, f, liveOrderedRecord(id, 3, 11))
		f.hook = func(f *fakeOrderedSeam, msgs []orderedMsg) error {
			f.hook = nil
			f.setLocked(msgs[0].subject, msgs[0].data)
			return errOrderedAmbiguous
		}
		got, err := store.Update(context.Background(), id, 3, []byte("landed"), storage.Rank{}, storage.Due{})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		if got.Revision != 4 || !bytes.Equal(got.Value, []byte("landed")) {
			t.Fatalf("Update returned %+v, want the landed revision 4", got)
		}
	})

	t.Run("unresolvable ambiguity is typed", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		seedOrderedRecord(t, f, liveOrderedRecord(id, 3, 11))
		f.hook = func(*fakeOrderedSeam, []orderedMsg) error { return errOrderedAmbiguous }
		_, err := store.Update(context.Background(), id, 3, []byte("lost"), storage.Rank{}, storage.Due{})
		var ambiguous *storage.OrderedAmbiguousError
		if !errors.As(err, &ambiguous) {
			t.Fatalf("error = %v (%T), want *storage.OrderedAmbiguousError", err, err)
		}
		if ambiguous.Operation != storage.OrderedUpdateOperation {
			t.Fatalf("operation = %q, want %q", ambiguous.Operation, storage.OrderedUpdateOperation)
		}
	})
}

func TestOrderedDelete(t *testing.T) {
	t.Parallel()
	id := orderedTestID()

	t.Run("tombstones a live record", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		before := liveOrderedRecord(id, 2, 6)
		seq := seedOrderedRecord(t, f, before)

		got, err := store.Delete(context.Background(), id, 2)
		if err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if !got.Deleted || got.Revision != 3 {
			t.Fatalf("Delete returned %+v, want deleted at revision 3", got)
		}
		if got.Rank != (storage.Rank{}) || got.Due != (storage.Due{State: storage.NotDue}) {
			t.Fatalf("tombstone kept rank %+v due %+v, want both cleared", got.Rank, got.Due)
		}
		if got.Order != before.Order || got.RankingScope != before.RankingScope || !bytes.Equal(got.Value, before.Value) {
			t.Fatalf("tombstone lost order/scope/value: %+v, want them from %+v", got, before)
		}
		batch := f.publishedAt(0)
		if batch[0].expectLastSeq != seq {
			t.Fatalf("Delete fenced on %d, want the observed head %d", batch[0].expectLastSeq, seq)
		}
	})

	t.Run("retry returns the original tombstone", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		seedOrderedRecord(t, f, liveOrderedRecord(id, 2, 6))
		first, err := store.Delete(context.Background(), id, 2)
		if err != nil {
			t.Fatalf("Delete: %v", err)
		}
		// The pre-delete revision, retried: the same tombstone comes back.
		again, err := store.Delete(context.Background(), id, 2)
		if err != nil {
			t.Fatalf("Delete retry: %v", err)
		}
		if again.Revision != first.Revision || !again.Deleted {
			t.Fatalf("retry returned %+v, want the original tombstone %+v", again, first)
		}
		if n := f.publishCount(); n != 1 {
			t.Fatalf("Delete published %d times across the retry, want 1", n)
		}
	})

	t.Run("absent record", func(t *testing.T) {
		t.Parallel()
		store := newOrderedStore(newFakeOrderedSeam())
		_, err := store.Delete(context.Background(), id, 1)
		var notFound *storage.OrderedRecordNotFoundError
		if !errors.As(err, &notFound) {
			t.Fatalf("error = %v (%T), want *storage.OrderedRecordNotFoundError", err, err)
		}
	})

	t.Run("stale revision does not mutate", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		seedOrderedRecord(t, f, liveOrderedRecord(id, 3, 6))
		_, err := store.Delete(context.Background(), id, 2)
		var conflict *storage.OrderedRevisionConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("error = %v (%T), want *storage.OrderedRevisionConflictError", err, err)
		}
		if n := f.publishCount(); n != 0 {
			t.Fatalf("Delete published %d times on a stale revision, want 0", n)
		}
	})
}

func TestOrderedValuesAreCallerOwned(t *testing.T) {
	t.Parallel()
	f := newFakeOrderedSeam()
	store := newOrderedStore(f)
	id := orderedTestID()

	value := []byte("mutable")
	rec, _, err := store.Create(context.Background(), id, "catalog", value, storage.Rank{}, storage.Due{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// The returned record is caller-owned and must not alias the caller's input:
	// mutating the input afterwards cannot change what Create handed back.
	copy(value, "ZZZZZZZ")
	if !bytes.Equal(rec.Value, []byte("mutable")) {
		t.Fatalf("the record Create returned aliases the caller's input slice: %q", rec.Value)
	}
	copy(value, "mutable")

	updated, err := store.Update(context.Background(), id, 1, value, storage.Rank{}, storage.Due{})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	copy(value, "ZZZZZZZ")
	if !bytes.Equal(updated.Value, []byte("mutable")) {
		t.Fatalf("the record Update returned aliases the caller's input slice: %q", updated.Value)
	}
	copy(value, "mutable")
	// Mutating a returned slice must not reach the store.
	copy(rec.Value, "YYYYYYY")

	got, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got.Value, []byte("mutable")) {
		t.Fatalf("stored value = %q, want %q", got.Value, "mutable")
	}
	// Two reads hand out independent slices.
	other, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get again: %v", err)
	}
	copy(got.Value, "XXXXXXX")
	if !bytes.Equal(other.Value, []byte("mutable")) {
		t.Fatalf("reads share a backing array: %q", other.Value)
	}
}

// deletedOrderedRecord is the canonical tombstone the contract requires Update
// and Delete to report regardless of expectedRevision.
func deletedOrderedRecord(id storage.OrderedID, revision, order uint64) storage.OrderedRecord {
	rec := liveOrderedRecord(id, revision, order)
	rec.Deleted = true
	rec.Rank = storage.Rank{}
	rec.Due = storage.Due{State: storage.NotDue}
	return rec
}

// TestOrderedLostCASAgainstConcurrentDelete pins the contract on the ONE path
// that reads the record back after losing its compare-and-swap. The contract
// says an absent record and a tombstone outrank the revision compare in both
// Update and Delete, "regardless of expectedRevision". The pre-write reads
// already honour that; the post-loss read must too, because the only
// linearization of a lost fence is AFTER the write that won it.
func TestOrderedLostCASAgainstConcurrentDelete(t *testing.T) {
	t.Parallel()
	id := orderedTestID()

	// tombstoneOnPublish makes the first publish lose its fence to a concurrent
	// Delete that has already tombstoned the record.
	tombstoneOnPublish := func(tomb storage.OrderedRecord) func(*fakeOrderedSeam, []orderedMsg) error {
		return func(f *fakeOrderedSeam, msgs []orderedMsg) error {
			f.hook = nil
			data, err := encodeOrderedRecord(tomb)
			if err != nil {
				return err
			}
			f.setLocked(msgs[0].subject, data)
			return nil
		}
	}

	t.Run("update reports the tombstone", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		seedOrderedRecord(t, f, liveOrderedRecord(id, 3, 11))
		f.hook = tombstoneOnPublish(deletedOrderedRecord(id, 4, 11))
		_, err := store.Update(context.Background(), id, 3, []byte("mine"), storage.Rank{}, storage.Due{})
		var deleted *storage.OrderedDeletedError
		if !errors.As(err, &deleted) {
			t.Fatalf("error = %v (%T), want *storage.OrderedDeletedError", err, err)
		}
		if deleted.ID != id {
			t.Fatalf("deleted.ID = %+v, want %+v", deleted.ID, id)
		}
	})

	t.Run("delete reports the tombstone", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		seedOrderedRecord(t, f, liveOrderedRecord(id, 3, 11))
		tomb := deletedOrderedRecord(id, 4, 11)
		f.hook = tombstoneOnPublish(tomb)
		got, err := store.Delete(context.Background(), id, 3)
		if err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if !got.Deleted || got.Revision != tomb.Revision {
			t.Fatalf("Delete returned %+v, want the canonical tombstone %+v", got, tomb)
		}
	})

	t.Run("update still conflicts against a live winner", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		seedOrderedRecord(t, f, liveOrderedRecord(id, 3, 11))
		f.hook = tombstoneOnPublish(liveOrderedRecord(id, 4, 11))
		_, err := store.Update(context.Background(), id, 3, []byte("mine"), storage.Rank{}, storage.Due{})
		var conflict *storage.OrderedRevisionConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("error = %v (%T), want *storage.OrderedRevisionConflictError", err, err)
		}
	})
}

// TestOrderedCreateAmbiguityProvesOwnWrite pins the honest answer to a FIRST
// Create whose acknowledgement was lost: the record read back is byte-identical
// to the one this call tried to write, which proves the write landed, so
// created=true. Reporting created=false there fails any conformance assertion
// that exactly one of N concurrent creators sees created=true.
func TestOrderedCreateAmbiguityProvesOwnWrite(t *testing.T) {
	t.Parallel()
	f := newFakeOrderedSeam()
	store := newOrderedStore(f)
	id := orderedTestID()
	f.hook = func(f *fakeOrderedSeam, msgs []orderedMsg) error {
		f.hook = nil
		// The batch DID commit; only the acknowledgement was lost.
		if err := f.applyLocked(msgs); err != nil {
			return err
		}
		return errOrderedAmbiguous
	}
	got, created, err := store.Create(context.Background(), id, "catalog", []byte("v"),
		storage.Rank{Ranked: true, Value: 3}, storage.Due{State: storage.DueAt, UnixMillis: 7})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !created {
		t.Fatal("created=false although the record read back is the one this call wrote")
	}
	if got.Order != 1 || got.Revision != 1 {
		t.Fatalf("Create returned order=%d revision=%d, want 1/1", got.Order, got.Revision)
	}
}

// TestOrderedCreateContention pins the bounded retry budget. It is the only test
// that constructs an *OrderedContentionError, and it proves the store gives up
// with a typed, retryable error rather than spinning forever.
func TestOrderedCreateContention(t *testing.T) {
	t.Parallel()
	f := newFakeOrderedSeam()
	store := newOrderedStore(f)
	store.retryBase = 0
	id := orderedTestID()
	counterSubj, _ := orderedSubjects(t, id)
	// A concurrent allocator wins the counter on EVERY attempt, so no attempt can
	// ever commit and the record subject stays absent.
	f.hook = func(f *fakeOrderedSeam, _ []orderedMsg) error {
		last, err := decodeOrderedCounter(f.msgs[counterSubj].data)
		if err != nil && f.msgs[counterSubj].seq != 0 {
			return err
		}
		f.setLocked(counterSubj, encodeOrderedCounter(last+1))
		return nil
	}
	_, created, err := store.Create(context.Background(), id, "catalog", []byte("v"), storage.Rank{}, storage.Due{})
	if created {
		t.Fatal("created=true although every attempt lost the counter race")
	}
	var contention *OrderedContentionError
	if !errors.As(err, &contention) {
		t.Fatalf("error = %v (%T), want *OrderedContentionError", err, err)
	}
	if contention.Attempts != orderedCreateAttempts || contention.ID != id {
		t.Fatalf("contention = %+v, want %d attempts for %+v", contention, orderedCreateAttempts, id)
	}
	if msg := contention.Error(); !strings.Contains(msg, strconv.Itoa(orderedCreateAttempts)) ||
		!strings.Contains(msg, id.OrderingScope) {
		t.Fatalf("Error() = %q, want the scope and the attempt count", msg)
	}
	if n := f.publishCount(); n != orderedCreateAttempts {
		t.Fatalf("Create attempted %d batches, want %d", n, orderedCreateAttempts)
	}
}

// TestOrderedCreateOrderExhausted pins the boundary of the order space. Order is
// immutable and never reused, so a scope whose counter reached MaxUint64 is
// permanently full and must fail loudly without writing anything.
func TestOrderedCreateOrderExhausted(t *testing.T) {
	t.Parallel()
	f := newFakeOrderedSeam()
	store := newOrderedStore(f)
	id := orderedTestID()
	counterSubj, _ := orderedSubjects(t, id)
	f.mu.Lock()
	f.setLocked(counterSubj, encodeOrderedCounter(math.MaxUint64))
	f.mu.Unlock()

	_, created, err := store.Create(context.Background(), id, "catalog", []byte("v"), storage.Rank{}, storage.Due{})
	if created {
		t.Fatal("created=true against an exhausted order space")
	}
	var exhausted *OrderedOrderExhaustedError
	if !errors.As(err, &exhausted) {
		t.Fatalf("error = %v (%T), want *OrderedOrderExhaustedError", err, err)
	}
	if exhausted.Order != math.MaxUint64 || exhausted.ID != id {
		t.Fatalf("exhausted = %+v, want MaxUint64 for %+v", exhausted, id)
	}
	if msg := exhausted.Error(); !strings.Contains(msg, strconv.FormatUint(math.MaxUint64, 10)) {
		t.Fatalf("Error() = %q, want the exhausted order", msg)
	}
	if n := f.publishCount(); n != 0 {
		t.Fatalf("Create published %d times against an exhausted scope, want 0", n)
	}
}

// TestOrderedSeamFailuresPropagate covers the I/O failure branches every method
// inherits from the seam: provisioning and every record or counter read. None of
// them may be mistaken for an absence.
func TestOrderedSeamFailuresPropagate(t *testing.T) {
	t.Parallel()
	id := orderedTestID()
	boom := errors.New("seam is down")

	t.Run("ensure stream", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		f.ensureErr = boom
		store := newOrderedStore(f)
		_, created, err := store.Create(context.Background(), id, "catalog", nil, storage.Rank{}, storage.Due{})
		if created || !errors.Is(err, boom) {
			t.Fatalf("Create = (created=%v, %v), want the ensureStream failure", created, err)
		}
		if n := f.publishCount(); n != 0 {
			t.Fatalf("Create published %d times without a stream, want 0", n)
		}
	})

	t.Run("reads", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name string
			call func(*orderedStore) error
		}{
			{name: "get", call: func(s *orderedStore) error {
				_, err := s.Get(context.Background(), id)
				return err
			}},
			{name: "create", call: func(s *orderedStore) error {
				_, _, err := s.Create(context.Background(), id, "catalog", nil, storage.Rank{}, storage.Due{})
				return err
			}},
			{name: "update", call: func(s *orderedStore) error {
				_, err := s.Update(context.Background(), id, 1, nil, storage.Rank{}, storage.Due{})
				return err
			}},
			{name: "delete", call: func(s *orderedStore) error {
				_, err := s.Delete(context.Background(), id, 1)
				return err
			}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				f := newFakeOrderedSeam()
				f.getErr = boom
				store := newOrderedStore(f)
				if err := tc.call(store); !errors.Is(err, boom) {
					t.Fatalf("error = %v, want the read failure %v", err, boom)
				}
				if n := f.publishCount(); n != 0 {
					t.Fatalf("published %d times after a failed read, want 0", n)
				}
			})
		}
	})

	t.Run("counter read after the identity read succeeds", func(t *testing.T) {
		t.Parallel()
		f := newFakeOrderedSeam()
		store := newOrderedStore(f)
		f.failGetAfter = 1
		f.getErr = boom
		_, _, err := store.Create(context.Background(), id, "catalog", nil, storage.Rank{}, storage.Due{})
		if !errors.Is(err, boom) {
			t.Fatalf("error = %v, want the counter read failure %v", err, boom)
		}
		if n := f.publishCount(); n != 0 {
			t.Fatalf("Create published %d times after a failed counter read, want 0", n)
		}
	})
}

// TestOrderedWaitBeforeRetry pins the backoff's three obligations: it is
// disableable (tests drive the retry budget without paying for waits), it stays
// inside its exponential window so the bounded loop stays bounded, and it honours
// the caller's context rather than blocking past a cancellation.
func TestOrderedWaitBeforeRetry(t *testing.T) {
	t.Parallel()

	t.Run("disabled", func(t *testing.T) {
		t.Parallel()
		store := newOrderedStore(newFakeOrderedSeam())
		store.retryBase = 0
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		// With no base there is nothing to wait for, so not even a cancelled
		// context can turn the call into a failure.
		if err := store.waitBeforeRetry(ctx, 1); err != nil {
			t.Fatalf("waitBeforeRetry with no base: %v", err)
		}
	})

	t.Run("stays inside its window", func(t *testing.T) {
		t.Parallel()
		store := newOrderedStore(newFakeOrderedSeam())
		store.retryBase = time.Millisecond
		for attempt := 1; attempt <= orderedRetryMaxShift+3; attempt++ {
			window := store.retryBase << min(attempt-1, orderedRetryMaxShift)
			began := time.Now()
			if err := store.waitBeforeRetry(context.Background(), attempt); err != nil {
				t.Fatalf("waitBeforeRetry(%d): %v", attempt, err)
			}
			// Timer granularity is generous, so only the exponential's cap is
			// asserted: the window must stop doubling at orderedRetryMaxShift.
			if elapsed := time.Since(began); elapsed > window+time.Second {
				t.Fatalf("attempt %d waited %s, past its %s window", attempt, elapsed, window)
			}
		}
	})

	t.Run("honours cancellation", func(t *testing.T) {
		t.Parallel()
		store := newOrderedStore(newFakeOrderedSeam())
		store.retryBase = time.Hour
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := store.waitBeforeRetry(ctx, orderedRetryMaxShift); !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	})
}

// TestOrderedCreateHonoursContextWhileQueued proves the per-scope latch never
// blocks a caller past its context: a creator waiting behind another creator in
// the same scope gives up when its own context does.
func TestOrderedCreateHonoursContextWhileQueued(t *testing.T) {
	t.Parallel()
	f := newFakeOrderedSeam()
	store := newOrderedStore(f)
	id := orderedTestID()
	counterSubj, _ := orderedSubjects(t, id)

	release, err := store.acquireScope(context.Background(), counterSubj)
	if err != nil {
		t.Fatalf("acquireScope: %v", err)
	}
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	other := storage.OrderedID{Namespace: id.Namespace, OrderingScope: id.OrderingScope, StableKey: "queued"}
	_, created, err := store.Create(ctx, other, "catalog", nil, storage.Rank{}, storage.Due{})
	if created || !errors.Is(err, context.Canceled) {
		t.Fatalf("Create = (created=%v, %v), want context.Canceled", created, err)
	}
	if n := f.publishCount(); n != 0 {
		t.Fatalf("Create published %d times while queued, want 0", n)
	}
}

// TestOrderedScopeLatchIsReleased proves the latch map does not grow one entry
// per order scope ever written: a scope that goes idle is dropped.
func TestOrderedScopeLatchIsReleased(t *testing.T) {
	t.Parallel()
	store := newOrderedStore(newFakeOrderedSeam())
	release, err := store.acquireScope(context.Background(), "oi.sessions.c.tenant")
	if err != nil {
		t.Fatalf("acquireScope: %v", err)
	}
	store.mu.Lock()
	held := len(store.latches)
	store.mu.Unlock()
	if held != 1 {
		t.Fatalf("held latches = %d, want 1", held)
	}
	release()
	store.mu.Lock()
	idle := len(store.latches)
	store.mu.Unlock()
	if idle != 0 {
		t.Fatalf("idle latches = %d, want the scope dropped", idle)
	}
}
