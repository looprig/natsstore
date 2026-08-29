package natsstore

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// orderedMaxMsgSize is the per-message ceiling configured on every ordered
// stream. It sits generously above the storage 1 MiB value floor (plus the JSON
// envelope, base64 value expansion, and JetStream framing) so a floor-sized
// value round-trips. As with the ledger, the embedded server's connection-level
// MaxPayload is higher still, so THIS cap is the effective limit.
const orderedMaxMsgSize int32 = 4 << 20

// Compile-time guard: the ordered stream's per-message ceiling must never exceed
// the embedded server's connection-level MaxPayload (embedded.go). Converting a
// negative constant to an unsigned type is a compile error, so raising
// orderedMaxMsgSize past maxPayload fails the build here rather than surfacing as
// a confusing ErrMaxPayload at the connection at runtime.
const _ = uint(maxPayload - orderedMaxMsgSize)

// The atomic-batch wire headers. nats.go v1.53.1 exposes AllowAtomicPublish on
// jetstream.StreamConfig but ships no client-side batch publisher, and its PubAck
// carries neither the batch id nor the batch size, so the protocol is spoken here
// with raw headers and a hand-decoded commit acknowledgement. This file is the
// only place that does so.
const (
	orderedHdrBatchID     = "Nats-Batch-Id"
	orderedHdrBatchSeq    = "Nats-Batch-Sequence"
	orderedHdrBatchCommit = "Nats-Batch-Commit"
)

// orderedBatchNonceBytes is the amount of crypto/rand entropy each seam mixes
// into its batch ids. The server keys in-flight atomic-batch state by batch id
// WITHIN THE STREAM and records no client identity, so two writers that emit the
// same id abort each other's staged messages — and an interleaving that happens
// to satisfy the server's batch-sequence gap check commits one writer's message
// together with another's as a single "atomic" batch. A per-seam counter alone
// cannot prevent that: it restarts at 1 in every seam and every process. 128
// bits of per-seam entropy makes a collision between seams not worth reasoning
// about, and the counter keeps ids distinct within a seam.
const orderedBatchNonceBytes = 16

// orderedMaxBatchIDLen is the server's ceiling on a batch id
// (nats-server/v2 server/stream.go). orderedBatchID stays under it by
// construction; orderedBatchIDPrefix is deliberately terse to leave room.
const orderedMaxBatchIDLen = 64

// OrderedStreamOpError reports a definite failure of a stream-management
// operation the ordered seam performs (lookup, create, info, or a batch member
// publish). It names the stream and the operation and unwraps to the underlying
// NATS error.
type OrderedStreamOpError struct {
	Stream string
	Op     string
	Cause  error
}

func (e *OrderedStreamOpError) Error() string {
	return "natsstore: ordered stream " + strconv.Quote(e.Stream) + " " + e.Op + " failed: " + e.Cause.Error()
}

// Unwrap returns the underlying cause.
func (e *OrderedStreamOpError) Unwrap() error { return e.Cause }

// OrderedStreamConfigError reports a stream that already exists under an ordered
// namespace's name but was not provisioned by this layout — a different schema
// version, or a configuration this design's atomicity depends on. The seam
// refuses to write into it rather than silently adopting it.
type OrderedStreamConfigError struct {
	Stream string
	Reason string
}

func (e *OrderedStreamConfigError) Error() string {
	return "natsstore: ordered stream " + strconv.Quote(e.Stream) + " is not usable: " + e.Reason
}

// OrderedBatchRejectedError reports an atomic batch the server rejected for a
// reason other than a failed sequence fence, or one this seam refused to send at
// all. No message in the batch committed — that is the whole meaning of the
// type, and it is why an acknowledged-but-unexpected commit is NOT reported with
// it (see OrderedBatchAckMismatchError).
type OrderedBatchRejectedError struct {
	BatchID     string
	Code        int
	ErrCode     uint16
	Description string
}

func (e *OrderedBatchRejectedError) Error() string {
	return "natsstore: ordered batch " + strconv.Quote(e.BatchID) + " rejected: code=" +
		strconv.Itoa(e.Code) + " err_code=" + strconv.FormatUint(uint64(e.ErrCode), 10) +
		" " + strconv.Quote(e.Description)
}

// OrderedBatchAckMismatchError reports a batch the server ACKNOWLEDGED — the
// acknowledgement carries no error, so something committed — whose reported
// member count is not the one this seam sent. The batch is neither cleanly
// committed nor cleanly rejected, so the seam joins this with errOrderedAmbiguous
// and lets the caller's read-back settle it. Reporting it as a rejection would
// invert the fact and turn a write that landed into a definite failure, which is
// the single misclassification this whole design exists to avoid.
type OrderedBatchAckMismatchError struct {
	BatchID   string
	Committed uint64
	Sent      uint64
}

func (e *OrderedBatchAckMismatchError) Error() string {
	return "natsstore: ordered batch " + strconv.Quote(e.BatchID) +
		" was acknowledged with " + strconv.FormatUint(e.Committed, 10) +
		" of " + strconv.FormatUint(e.Sent, 10) + " messages; the outcome is unknown"
}

// OrderedBatchSubjectsError reports a batch naming one subject twice. Each
// member carries its OWN expected-last-subject-sequence, so two members on one
// subject cannot both have a meaningful fence: the second would be evaluated
// against a sequence the first is about to change. The seam refuses to send such
// a batch rather than let a future caller discover it as a silent mis-commit.
type OrderedBatchSubjectsError struct {
	Subject string
}

func (e *OrderedBatchSubjectsError) Error() string {
	return "natsstore: ordered batch names subject " + strconv.Quote(e.Subject) + " more than once"
}

// orderedAPIError mirrors the server's ApiError as it appears inside a publish
// acknowledgement. It is the narrow serialization boundary this package's typing
// rules allow.
type orderedAPIError struct {
	Code        int    `json:"code"`
	ErrCode     uint16 `json:"err_code"`
	Description string `json:"description"`
}

// orderedBatchAck mirrors the server's PubAck response to a batch commit.
type orderedBatchAck struct {
	Stream    string           `json:"stream"`
	Sequence  uint64           `json:"seq"`
	BatchID   string           `json:"batch"`
	BatchSize uint64           `json:"count"`
	Error     *orderedAPIError `json:"error"`
}

// jetStreamOrderedSeam is the production orderedSeam: it binds the ordered
// store's four operations to a real jetstream.JetStream plus the raw connection
// the batch protocol needs. It caches the stream handle for each provisioned
// namespace, so the lazy provisioning check costs one round trip per namespace
// per process rather than one per mutation.
type jetStreamOrderedSeam struct {
	nc *nats.Conn
	js jetstream.JetStream

	// batchNonce is this seam's process-and-instance-unique batch-id prefix, and
	// batchID keeps ids distinct within the seam. See orderedBatchNonceBytes.
	batchNonce string
	batchID    atomic.Uint64

	mu      sync.RWMutex
	streams map[string]jetstream.Stream
}

var _ orderedSeam = (*jetStreamOrderedSeam)(nil)

// newJetStreamOrderedSeam wraps a connection and its jetstream context as a
// production ordered seam. The new jetstream package is required, not optional:
// the legacy nats.JetStreamContext cannot create an AllowAtomicPublish stream.
func newJetStreamOrderedSeam(nc *nats.Conn, js jetstream.JetStream) *jetStreamOrderedSeam {
	return &jetStreamOrderedSeam{
		nc:         nc,
		js:         js,
		batchNonce: newOrderedBatchNonce(),
		streams:    map[string]jetstream.Stream{},
	}
}

// orderedBatchIDPrefix marks this package's batch ids in server logs. It is kept
// short because the id must fit orderedMaxBatchIDLen alongside the nonce and the
// counter.
const orderedBatchIDPrefix = "ns-"

// newOrderedBatchNonce draws this seam's batch-id entropy. The standard library
// supplies everything a globally unique prefix needs, so no third-party id
// package (nuid, uuid) is added to this module's direct dependencies for it.
func newOrderedBatchNonce() string {
	var nonce [orderedBatchNonceBytes]byte
	orderedRandomBytes(nonce[:])
	return hex.EncodeToString(nonce[:])
}

// nextBatchID returns an id no other seam, in this process or any other, will
// emit.
func (s *jetStreamOrderedSeam) nextBatchID() string {
	return orderedBatchIDPrefix + s.batchNonce + "-" + strconv.FormatUint(s.batchID.Add(1), 10)
}

// orderedStreamConfig is the configuration every ordered namespace stream must
// carry. MaxMsgsPerSubject=1 is load-bearing twice over: it keeps exactly the
// current counter and record versions, and it makes "the last message on the
// subject" the value a compare-and-swap fences on.
func orderedStreamConfig(spec orderedStreamSpec) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:               spec.stream,
		Description:        orderedStreamDescription,
		Subjects:           []string{spec.subjectFilter},
		Retention:          jetstream.LimitsPolicy,
		Discard:            jetstream.DiscardOld,
		Storage:            jetstream.FileStorage,
		MaxMsgs:            -1,
		MaxBytes:           -1,
		MaxAge:             0,
		MaxMsgsPerSubject:  1,
		MaxMsgSize:         orderedMaxMsgSize,
		Replicas:           1,
		AllowAtomicPublish: true,
	}
}

// ensureStream lazily provisions the namespace's stream and verifies its layout.
// A stream created by an older or foreign layout is refused with a typed error
// instead of being written into; a stream this process has already verified is
// served from the handle cache.
func (s *jetStreamOrderedSeam) ensureStream(ctx context.Context, spec orderedStreamSpec) error {
	if _, ok := s.cachedStream(spec.stream); ok {
		return nil
	}
	stream, err := s.js.Stream(ctx, spec.stream)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		stream, err = s.js.CreateStream(ctx, orderedStreamConfig(spec))
		if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			// A concurrent creator won; adopt and verify whatever is there.
			stream, err = s.js.Stream(ctx, spec.stream)
		}
	}
	if err != nil {
		return &OrderedStreamOpError{Stream: spec.stream, Op: "ensure", Cause: err}
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return &OrderedStreamOpError{Stream: spec.stream, Op: "info", Cause: err}
	}
	if err := verifyOrderedStreamConfig(spec, info.Config); err != nil {
		return err
	}
	s.cacheStream(spec.stream, stream)
	return nil
}

// verifyOrderedStreamConfig checks the properties the ordered design depends on.
// Anything else about the stream is the operator's business.
func verifyOrderedStreamConfig(spec orderedStreamSpec, cfg jetstream.StreamConfig) error {
	if cfg.Description != orderedStreamDescription {
		return &OrderedStreamConfigError{
			Stream: spec.stream,
			Reason: "layout is " + strconv.Quote(cfg.Description) + ", want " + strconv.Quote(orderedStreamDescription),
		}
	}
	if !cfg.AllowAtomicPublish {
		return &OrderedStreamConfigError{Stream: spec.stream, Reason: "atomic publish is disabled"}
	}
	if cfg.MaxMsgsPerSubject != 1 {
		return &OrderedStreamConfigError{
			Stream: spec.stream,
			Reason: "max messages per subject is " + strconv.FormatInt(cfg.MaxMsgsPerSubject, 10) + ", want 1",
		}
	}
	if len(cfg.Subjects) != 1 || cfg.Subjects[0] != spec.subjectFilter {
		return &OrderedStreamConfigError{
			Stream: spec.stream,
			Reason: "subjects are not exactly " + strconv.Quote(spec.subjectFilter),
		}
	}
	// Every limit that could delete a record behind the store's back. An ordered
	// record is the authority for its identity and its order is never reused, so
	// silent expiry is not a capacity policy — it is corruption: the identity
	// subject would read absent and the next Create would reallocate an order that
	// has already been handed out.
	want := orderedStreamConfig(spec)
	for _, limit := range []struct {
		name string
		got  int64
		want int64
	}{
		{name: "max age", got: int64(cfg.MaxAge), want: int64(want.MaxAge)},
		{name: "max bytes", got: cfg.MaxBytes, want: want.MaxBytes},
		{name: "max messages", got: cfg.MaxMsgs, want: want.MaxMsgs},
	} {
		if limit.got != limit.want {
			return &OrderedStreamConfigError{
				Stream: spec.stream,
				Reason: limit.name + " is " + strconv.FormatInt(limit.got, 10) +
					", want " + strconv.FormatInt(limit.want, 10) + " (records must never expire)",
			}
		}
	}
	if cfg.Discard != want.Discard {
		return &OrderedStreamConfigError{
			Stream: spec.stream,
			Reason: "discard policy is not the provisioned one",
		}
	}
	return nil
}

func (s *jetStreamOrderedSeam) cachedStream(name string) (jetstream.Stream, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stream, ok := s.streams[name]
	return stream, ok
}

func (s *jetStreamOrderedSeam) cacheStream(name string, stream jetstream.Stream) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streams[name] = stream
}

// lastMsgForSubject returns the current message on subject. An absent stream and
// an absent subject are both reported as sequence 0 with no error, which is
// exactly the value the expected-last-subject-sequence fence uses to mean "must
// not exist", so a read and the following write agree by construction.
func (s *jetStreamOrderedSeam) lastMsgForSubject(ctx context.Context, streamName, subject string) (uint64, []byte, error) {
	stream, ok := s.cachedStream(streamName)
	if !ok {
		var err error
		stream, err = s.js.Stream(ctx, streamName)
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return 0, nil, nil
		}
		if err != nil {
			return 0, nil, &OrderedStreamOpError{Stream: streamName, Op: "lookup", Cause: err}
		}
		// Cache the handle. A read-only process never calls ensureStream, so
		// without this every Get pays a lookup round trip forever — and the view
		// hydration this store exists to feed is exactly that workload. It is as
		// safe as the uncached path: the handle is a name binding, and a stream
		// replaced underneath it fails the same way either way.
		s.cacheStream(streamName, stream)
	}
	msg, err := stream.GetLastMsgForSubject(ctx, subject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return 0, nil, nil
	}
	if err != nil {
		return 0, nil, &OrderedStreamOpError{Stream: streamName, Op: "get last message", Cause: err}
	}
	return msg.Sequence, msg.Data, nil
}

// publish commits one fenced message, translating the server's rejection onto
// errOrderedPrecondition and a lost acknowledgement onto errOrderedAmbiguous.
func (s *jetStreamOrderedSeam) publish(ctx context.Context, streamName string, msg orderedMsg) error {
	m := nats.NewMsg(msg.subject)
	m.Data = msg.data
	if _, err := s.js.PublishMsg(ctx, m, jetstream.WithExpectLastSequencePerSubject(msg.expectLastSeq)); err != nil {
		if isOrderedWrongLastSeq(err) {
			return errOrderedPrecondition
		}
		if isOrderedAmbiguous(err) {
			return errors.Join(errOrderedAmbiguous, err)
		}
		return &OrderedStreamOpError{Stream: streamName, Op: "publish", Cause: err}
	}
	return nil
}

// publishBatch commits msgs as one atomic batch. Every member but the last is a
// fire-and-forget publish carrying the batch id and its sequence within the
// batch; the last also carries the commit header and is a request whose reply is
// the batch's single acknowledgement. Each member keeps its OWN
// expected-last-subject-sequence, which is why the counter and the record can be
// fenced independently — and why no batch may ever name one subject twice.
func (s *jetStreamOrderedSeam) publishBatch(ctx context.Context, streamName string, msgs []orderedMsg) error {
	if len(msgs) == 0 {
		return &OrderedBatchRejectedError{Description: "batch has no messages"}
	}
	seen := make(map[string]struct{}, len(msgs))
	for _, m := range msgs {
		if _, dup := seen[m.subject]; dup {
			return &OrderedBatchSubjectsError{Subject: m.subject}
		}
		seen[m.subject] = struct{}{}
	}
	id := s.nextBatchID()
	for i, m := range msgs {
		msg := nats.NewMsg(m.subject)
		msg.Data = m.data
		msg.Header.Set(orderedHdrBatchID, id)
		msg.Header.Set(orderedHdrBatchSeq, strconv.Itoa(i+1))
		msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(m.expectLastSeq, 10))
		if i < len(msgs)-1 {
			// One connection, so these reach the server in order, ahead of the commit.
			// A local publish failure means the commit below never happens, so the
			// batch cannot have committed: it is definite, not ambiguous.
			if err := s.nc.PublishMsg(msg); err != nil {
				return &OrderedStreamOpError{Stream: streamName, Op: "batch member publish", Cause: err}
			}
			continue
		}
		msg.Header.Set(orderedHdrBatchCommit, "1")
		reply, err := s.nc.RequestMsgWithContext(ctx, msg)
		if err != nil {
			if isOrderedAmbiguous(err) {
				return errors.Join(errOrderedAmbiguous, err)
			}
			// The members published above are already staged on the server under
			// this batch id and hold one of its MaxBatchInflightPerStream slots
			// until its cleanup timer fires. That is bounded and self-healing, but
			// it is real budget: a caller that retries a failing commit in a tight
			// loop can exhaust the per-stream slots, which the server then reports
			// to LATER batches. It is one more reason Create waits between attempts.
			return &OrderedStreamOpError{Stream: streamName, Op: "batch commit", Cause: err}
		}
		var ack orderedBatchAck
		if err := json.Unmarshal(reply.Data, &ack); err != nil {
			return &OrderedStreamOpError{Stream: streamName, Op: "batch commit ack decode", Cause: err}
		}
		return classifyOrderedBatchAck(id, uint64(len(msgs)), ack)
	}
	return nil
}

// classifyOrderedBatchAck maps a decoded commit acknowledgement onto the seam's
// three outcomes. It is a pure function so the classification — the part that
// decides whether a caller sees a definite failure or reads the record back — is
// testable without a server.
func classifyOrderedBatchAck(id string, sent uint64, ack orderedBatchAck) error {
	if ack.Error != nil {
		if isOrderedWrongLastSeqCode(ack.Error.ErrCode) {
			return errOrderedPrecondition
		}
		return &OrderedBatchRejectedError{
			BatchID:     id,
			Code:        ack.Error.Code,
			ErrCode:     ack.Error.ErrCode,
			Description: ack.Error.Description,
		}
	}
	// No error means the server committed. A count that is not the one we sent is
	// therefore NOT a rejection: some prefix of the batch may be durable. Ambiguity
	// is the only honest classification, and the caller resolves it by reading the
	// record back — which also makes this the safe direction to be wrong in.
	if ack.BatchSize != sent {
		return errors.Join(errOrderedAmbiguous, &OrderedBatchAckMismatchError{
			BatchID: id, Committed: ack.BatchSize, Sent: sent,
		})
	}
	return nil
}

// isOrderedWrongLastSeqCode reports whether an ApiError code is the server's
// expected-last-sequence rejection. Both spellings are matched: R1 returns
// JSStreamWrongLastSequence and a replicated stream returns the "Constant"
// variant, and the design must classify a CAS loss identically under either.
func isOrderedWrongLastSeqCode(code uint16) bool {
	return jetstream.ErrorCode(code) == jetstream.JSErrCodeStreamWrongLastSequence ||
		jetstream.ErrorCode(code) == jetstream.JSErrCodeStreamWrongLastSequenceConstant
}

// isOrderedWrongLastSeq reports whether err is that rejection delivered as a
// typed client error.
func isOrderedWrongLastSeq(err error) bool {
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
		apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant
}

// isOrderedAmbiguous reports whether err leaves the outcome of a write unknown:
// the request was sent but no acknowledgement came back.
//
// context.Canceled is deliberately NOT in the set, which is an asymmetry with
// DeadlineExceeded worth spelling out: a cancelled request may well have
// committed, so the classification is not "nothing happened". It is that
// cancellation is caller-initiated — the caller chose to stop waiting and is the
// one party able to decide what to do about a possibly-committed write, whereas
// a deadline expiry is a fact about the server the caller must be told. Every
// ordered mutation is idempotent by identity or fenced by revision, so a
// cancelled caller that retries converges either way.
func isOrderedAmbiguous(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, nats.ErrNoStreamResponse) ||
		errors.Is(err, jetstream.ErrNoStreamResponse)
}
