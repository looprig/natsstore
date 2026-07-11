package natsstore

import (
	"context"
	"errors"
	"io"
	"strconv"

	"github.com/looprig/storage"
	"github.com/nats-io/nats.go"
)

// ledgerMaxMsgSize is the per-message ceiling configured on every ledger stream. It
// sits generously above the storage 1 MiB payload floor (plus JetStream
// header/framing headroom) so the conformance 1 MiB-payload append round-trips. The
// embedded server's connection-level MaxPayload (see embedded.go) is set higher
// still, so THIS stream cap — not the connection — is the effective per-record limit.
const ledgerMaxMsgSize int32 = 4 << 20

// Compile-time guard on that invariant: the ledger's per-message stream ceiling must
// never exceed the embedded server's connection-level MaxPayload (embedded.go), or a
// floor-sized append would be rejected at the connection with a confusing
// ErrMaxPayload before the stream cap is ever consulted. Converting a negative
// constant to an unsigned type is a compile error, so bumping ledgerMaxMsgSize past
// maxPayload fails the build here rather than at the connection layer at runtime.
const _ = uint(maxPayload - ledgerMaxMsgSize)

// jsSeam is the narrow set of JetStream operations the ledger drives, isolated
// behind an interface (DIP + ISP) so the ledger's CAS/classification logic can be
// unit-tested with a scripted fake and no running server. The production binding
// (jetStreamSeam, seam.go) wraps a real nats.JetStreamContext.
type jsSeam interface {
	// ensureStream idempotently creates the ledger's stream bound to subject with the
	// given per-message ceiling; an already-provisioned stream is a success.
	ensureStream(ctx context.Context, stream, subject string, maxMsgSize int32) error
	// publish appends data to subject under an expected-last-sequence fence and
	// returns the assigned sequence. It returns the RAW backend error so the ledger
	// can classify it (wrong-last-sequence vs ambiguous vs definite).
	publish(ctx context.Context, subject string, data []byte, expectedLastSeq uint64) (seq uint64, err error)
	// getMsg returns the stored payload (body) at seq in stream.
	getMsg(ctx context.Context, stream string, seq uint64) (data []byte, err error)
	// lastSeq returns the stream's tip (last committed sequence), or 0 if absent.
	lastSeq(ctx context.Context, stream string) (uint64, error)
	// deleteStream removes the stream; an absent stream is a no-op success.
	deleteStream(ctx context.Context, stream string) error
}

// RecordReadError reports that a ledger cursor could not read the record at Seq from
// the backend. It fails closed — a caller must NOT treat it as end-of-ledger — and
// unwraps to the underlying backend error.
type RecordReadError struct {
	Name  string
	Seq   uint64
	Cause error
}

func (e *RecordReadError) Error() string {
	return "natsstore: ledger " + strconv.Quote(e.Name) + " read of seq " +
		strconv.FormatUint(e.Seq, 10) + " failed: " + e.Cause.Error()
}
func (e *RecordReadError) Unwrap() error { return e.Cause }

// ledgerStore implements storage.Ledger over one JetStream stream per named ledger:
// stream = streamForName(name), subject = subjectForName(name). It holds only the
// seam (DIP) — the composition root injects the production binding; tests inject a
// fake — and carries no mutable state, so it is as safe for concurrent use as the
// seam beneath it.
type ledgerStore struct {
	seam jsSeam
}

var _ storage.Ledger = (*ledgerStore)(nil)

// newLedgerStore builds a ledger over seam.
func newLedgerStore(seam jsSeam) *ledgerStore {
	return &ledgerStore{seam: seam}
}

// Append commits payload as the record immediately after `expected` (CAS on the
// tip; expected == 0 requires the ledger to be empty). It validates name, ensures
// the stream exists, then publishes under an expected-last-sequence fence and
// classifies the outcome:
//   - success             → nil (the committed seq is expected+1 by construction).
//   - wrong-last-sequence → *storage.ConflictError (definite: the record did not land).
//   - lost ack (timeout /
//     no-response / ctx
//     deadline)           → *storage.AmbiguousError (outcome unknown).
//   - anything else       → the raw error, fail closed.
//
// It NEVER retries: storage.AppendDefinite owns ambiguity resolution.
func (s *ledgerStore) Append(ctx context.Context, name string, expected uint64, payload []byte) error {
	subject, err := subjectForName(name)
	if err != nil {
		return err
	}
	stream, err := streamForName(name)
	if err != nil {
		return err
	}
	if err := s.seam.ensureStream(ctx, stream, subject, ledgerMaxMsgSize); err != nil {
		return err
	}
	if _, err := s.seam.publish(ctx, subject, payload, expected); err != nil {
		if isWrongLastSequence(err) {
			return &storage.ConflictError{Name: name, Expected: expected}
		}
		if isAmbiguous(err) {
			return &storage.AmbiguousError{Name: name, Expected: expected, Cause: err}
		}
		return err
	}
	return nil
}

// Read captures the tip as of now and returns a bounded cursor over the records with
// Seq in [max(from,1), tip]. The cursor never tails appends made after Read
// (storage's bounded-cursor contract); an absent stream or from > tip yields an
// immediately-drained cursor.
func (s *ledgerStore) Read(ctx context.Context, name string, from uint64) (storage.Cursor, error) {
	stream, err := streamForName(name)
	if err != nil {
		return nil, err
	}
	tip, err := s.seam.lastSeq(ctx, stream)
	if err != nil {
		return nil, err
	}
	next := from
	if next < 1 {
		next = 1
	}
	return &ledgerCursor{seam: s.seam, name: name, stream: stream, next: next, tip: tip}, nil
}

// Tip returns the ledger's last committed sequence, or 0 if absent (absent == empty).
func (s *ledgerStore) Tip(ctx context.Context, name string) (uint64, error) {
	stream, err := streamForName(name)
	if err != nil {
		return 0, err
	}
	return s.seam.lastSeq(ctx, stream)
}

// Delete removes the ledger's stream. Deleting an absent ledger is a no-op success
// (idempotent), so a deleted ledger is indistinguishable from a never-written one.
func (s *ledgerStore) Delete(ctx context.Context, name string) error {
	stream, err := streamForName(name)
	if err != nil {
		return err
	}
	return s.seam.deleteStream(ctx, stream)
}

// ledgerCursor is a bounded forward cursor over a ledger's records. next is the next
// sequence to fetch; tip is the bound captured at Read. It yields
// storage.Record{Seq, Payload} in ascending order and io.EOF once next passes tip.
// It is single-consumer (the storage Cursor contract): not safe for concurrent Next.
type ledgerCursor struct {
	seam   jsSeam
	name   string
	stream string
	next   uint64
	tip    uint64
}

var _ storage.Cursor = (*ledgerCursor)(nil)

// Next returns the next record in [next, tip], advancing the cursor, or io.EOF once
// the bound is passed. A backend read failure is a typed *RecordReadError (fail
// closed) — never conflated with EOF.
func (c *ledgerCursor) Next(ctx context.Context) (storage.Record, error) {
	if c.next > c.tip {
		return storage.Record{}, io.EOF
	}
	data, err := c.seam.getMsg(ctx, c.stream, c.next)
	if err != nil {
		return storage.Record{}, &RecordReadError{Name: c.name, Seq: c.next, Cause: err}
	}
	rec := storage.Record{Seq: c.next, Payload: data}
	c.next++
	return rec, nil
}

// Close releases the cursor. Each Next is an independent GetMsg, so the cursor holds
// no backend resources and Close is a no-op success.
func (c *ledgerCursor) Close() error { return nil }

// isWrongLastSequence reports whether err is JetStream's expected-last-sequence
// rejection: a *nats.APIError carrying JSErrCodeStreamWrongLastSequence (10071). It
// is a DEFINITE conflict — the stream advanced past the fenced tip, so the record did
// not land. Matching the typed code (not the message) keeps the classification robust
// against description changes.
func isWrongLastSequence(err error) bool {
	var apiErr *nats.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode == nats.JSErrCodeStreamWrongLastSequence
}

// isAmbiguous reports whether err means the publish ACK was LOST — a NATS timeout, a
// no-response-from-stream, or a context deadline — so the record may or may not have
// committed. These are the ONLY ambiguous outcomes; a wrong-last-sequence rejection
// is definite and is classified first.
//
// context.Canceled is deliberately NOT in this set. A cancel that fires after the PUB
// is already on the wire could still commit server-side, so it is a genuine lost-ack
// case in principle — but we treat it as fail-closed BY CHOICE, not because the record
// definitely did not commit: a context.Canceled here means the caller is aborting, and
// on restart it re-observes the CAS-guarded, contiguous tip, so a
// reported-failure-that-actually-committed self-heals (the caller simply appends after
// the record it did not know had landed).
//
// This three-sentinel set is tuned for the IN-PROCESS embedded transport (no network,
// no reconnect), where these are the whole lost-ack surface. A remote NATS URL backend
// (now supported — natsstore.Open accepts a URL) widens that surface —
// ErrConnectionClosed / ErrReconnecting / a drain timeout, plus cancel-after-send — but
// for v1 those wider remote lost-ack outcomes are DELIBERATELY left out of this set and
// so fall through to a definite error: fail-closed is safe here because a caller that
// re-observes the CAS-guarded, contiguous tip on restart self-heals a
// reported-failure-that-actually-committed (it simply appends after the record it did not
// know had landed), exactly as the context.Canceled note above. Widening the set to
// classify those remote outcomes as *AmbiguousError (so AppendDefinite resolves them in
// band, without a restart) is a possible future refinement, not a correctness gap.
func isAmbiguous(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrNoStreamResponse)
}
