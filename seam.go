package natsstore

import (
	"context"
	"errors"
	"strconv"

	"github.com/nats-io/nats.go"
)

// StreamOpError reports a definite failure of a JetStream stream-management
// operation the ledger seam performs (ensure / info / delete) — as opposed to a
// publish, whose outcome the ledger classifies itself. It names the stream and the
// operation and unwraps to the underlying NATS error.
type StreamOpError struct {
	Stream string
	Op     string
	Cause  error
}

func (e *StreamOpError) Error() string {
	return "natsstore: ledger stream " + strconv.Quote(e.Stream) + " " + e.Op + " failed: " + e.Cause.Error()
}
func (e *StreamOpError) Unwrap() error { return e.Cause }

// jetStreamSeam is the production jsSeam: it binds the ledger's operations to a real
// nats.JetStreamContext. It is the ONLY ledger code that talks to NATS directly; the
// logic above it (ledgerStore) is server-free and unit-testable. Every call carries
// the caller's context via nats.Context, so no operation blocks unbounded.
type jetStreamSeam struct {
	js nats.JetStreamContext
}

var _ jsSeam = (*jetStreamSeam)(nil)

// newJetStreamSeam wraps js as a production ledger seam.
func newJetStreamSeam(js nats.JetStreamContext) *jetStreamSeam {
	return &jetStreamSeam{js: js}
}

// ensureStream creates the keep-everything ledger stream (Limits retention, nothing
// discarded, one replica) bound to exactly subject, with maxMsgSize as the
// per-message ceiling. It is idempotent: an already-provisioned stream — identical
// config yields success, a divergent config yields ErrStreamNameAlreadyInUse — is
// treated as success, so concurrent first-appends never race each other into an
// error. (The stream name is a pure function of the ledger name, so the config is
// deterministic; a divergent stream under our name is not an expected condition.)
func (s *jetStreamSeam) ensureStream(ctx context.Context, stream, subject string, maxMsgSize int32) error {
	cfg := &nats.StreamConfig{
		Name:              stream,
		Subjects:          []string{subject},
		Retention:         nats.LimitsPolicy,
		MaxMsgs:           -1,
		MaxBytes:          -1,
		MaxAge:            0,
		MaxMsgsPerSubject: -1,
		Replicas:          1,
		MaxMsgSize:        maxMsgSize,
	}
	_, err := s.js.AddStream(cfg, nats.Context(ctx))
	if err == nil || errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		return nil
	}
	return &StreamOpError{Stream: stream, Op: "ensure", Cause: err}
}

// publish appends data to subject under an expected-last-sequence fence and returns
// the assigned sequence. It returns the RAW publish error (unwrapped) so the ledger
// can classify wrong-last-sequence vs ambiguous vs definite; wrapping it here would
// hide the *nats.APIError / timeout sentinels the classification matches on.
func (s *jetStreamSeam) publish(ctx context.Context, subject string, data []byte, expectedLastSeq uint64) (uint64, error) {
	msg := &nats.Msg{Subject: subject, Data: data}
	ack, err := s.js.PublishMsg(msg, nats.Context(ctx), nats.ExpectLastSequence(expectedLastSeq))
	if err != nil {
		return 0, err
	}
	return ack.Sequence, nil
}

// getMsg returns the stored payload (body only; headers are not part of the record)
// at seq in stream. The raw backend error is returned unwrapped so the cursor can
// attribute it to the exact sequence via *RecordReadError.
func (s *jetStreamSeam) getMsg(ctx context.Context, stream string, seq uint64) ([]byte, error) {
	raw, err := s.js.GetMsg(stream, seq, nats.Context(ctx))
	if err != nil {
		return nil, err
	}
	return raw.Data, nil
}

// lastSeq returns the stream's tip (State.LastSeq). An absent stream is reported as
// tip 0 (absent == empty), not an error; any other failure is a typed StreamOpError.
func (s *jetStreamSeam) lastSeq(ctx context.Context, stream string) (uint64, error) {
	info, err := s.js.StreamInfo(stream, nats.Context(ctx))
	if err != nil {
		if errors.Is(err, nats.ErrStreamNotFound) {
			return 0, nil
		}
		return 0, &StreamOpError{Stream: stream, Op: "info", Cause: err}
	}
	return info.State.LastSeq, nil
}

// deleteStream removes the stream. An absent stream is a no-op success (idempotent);
// any other failure is a typed StreamOpError.
func (s *jetStreamSeam) deleteStream(ctx context.Context, stream string) error {
	err := s.js.DeleteStream(stream, nats.Context(ctx))
	if err == nil || errors.Is(err, nats.ErrStreamNotFound) {
		return nil
	}
	return &StreamOpError{Stream: stream, Op: "delete", Cause: err}
}
