package natsstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

// jetStreamKVSeam is the production kvLeaseSeam: it binds the leaser's three KV
// operations to a real, context-aware jetstream.KeyValue bucket, translating the
// jetstream package's CAS errors onto the package-level seam sentinels (errKVKeyExists
// / errKVKeyNotFound / errKVCASConflict) so the leaser logic above it stays free of any
// nats.go dependency and unit-testable with a plain fake. It is the ONLY leaser code
// that talks to NATS directly.
//
// Unlike the ledger's legacy JetStreamContext seam, this uses the nats.go v1.52.0
// `jetstream` package, whose Create/Get/Update accept a context.Context — so the
// caller's ctx genuinely bounds the Acquire and Release round-trips (honoring the
// storage.Lease "releasing may cross the network; ctx bounds it" contract). (The
// ledger seam remains on the legacy API for now; unifying it is deferred to D5.)
type jetStreamKVSeam struct {
	kv jetstream.KeyValue
}

var _ kvLeaseSeam = (*jetStreamKVSeam)(nil)

// newJetStreamKVSeam wraps kv as a production leaser seam.
func newJetStreamKVSeam(kv jetstream.KeyValue) *jetStreamKVSeam {
	return &jetStreamKVSeam{kv: kv}
}

// create writes val at key only if absent, bounded by ctx. A key that already exists
// surfaces as jetstream.ErrKeyExists (which wraps the wrong-last-sequence API code) →
// errKVKeyExists.
func (s *jetStreamKVSeam) create(ctx context.Context, key string, val []byte) (uint64, error) {
	rev, err := s.kv.Create(ctx, key, val)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) || isJetStreamWrongLastSeq(err) {
			return 0, errKVKeyExists
		}
		return 0, err
	}
	return rev, nil
}

// get returns the stored value and revision at key, bounded by ctx, or errKVKeyNotFound
// if absent.
func (s *jetStreamKVSeam) get(ctx context.Context, key string) ([]byte, uint64, error) {
	entry, err := s.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, 0, errKVKeyNotFound
		}
		return nil, 0, err
	}
	return entry.Value(), entry.Revision(), nil
}

// update writes val at key only if its current revision equals expectedRev, bounded by
// ctx. A revision mismatch (including an update whose key has since vanished, which
// JetStream KV also reports as a wrong-last-sequence rejection) → errKVCASConflict; an
// explicit not-found → errKVKeyNotFound.
func (s *jetStreamKVSeam) update(ctx context.Context, key string, val []byte, expectedRev uint64) (uint64, error) {
	rev, err := s.kv.Update(ctx, key, val, expectedRev)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return 0, errKVKeyNotFound
		}
		if isJetStreamWrongLastSeq(err) {
			return 0, errKVCASConflict
		}
		return 0, err
	}
	return rev, nil
}

// jetStreamKVStoreSeam is the production kvSeam: it binds the KV store's five operations
// to a real, context-aware jetstream.KeyValue bucket (DISTINCT from the lease bucket). It
// embeds jetStreamKVSeam to reuse the identical create/get/update translation the leaser
// already validated against the embedded server — the CAS mapping is written once — and
// adds the two ops the leaser has no use for: del (idempotent) and keys (list-all). It is
// the ONLY kv-store code that talks to NATS directly.
type jetStreamKVStoreSeam struct {
	*jetStreamKVSeam
}

var _ kvSeam = (*jetStreamKVStoreSeam)(nil)

// newJetStreamKVStoreSeam wraps kv as a production KV-store seam.
func newJetStreamKVStoreSeam(kv jetstream.KeyValue) *jetStreamKVStoreSeam {
	return &jetStreamKVStoreSeam{jetStreamKVSeam: newJetStreamKVSeam(kv)}
}

// del places a delete marker at key, bounded by ctx. JetStream KV's delete is
// unconditional (no revision fence here), so deleting an absent key still succeeds —
// exactly the idempotent contract the store relies on.
func (s *jetStreamKVStoreSeam) del(ctx context.Context, key string) error {
	return s.kv.Delete(ctx, key)
}

// keys returns every live key in the bucket, bounded by ctx. An empty bucket surfaces as
// jetstream.ErrNoKeysFound, which is mapped to an empty slice (absent == empty), not an
// error; the store sorts/dedups/prefix-filters the result.
func (s *jetStreamKVStoreSeam) keys(ctx context.Context) ([]string, error) {
	ks, err := s.kv.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, err
	}
	return ks, nil
}

// kvBucketConfig returns the jetstream KV bucket configuration for the KV store's bucket:
// file storage, one replica (single-node embedded), and History 1 (the KV store is a
// pure revision-CAS latest-value store — no historical values are read). MaxValueSize is
// left at the default (unlimited, bounded only by the connection's MaxPayload), so the
// storage 1 MiB value floor round-trips. It is DISTINCT from leaseBucketConfig's bucket.
func kvBucketConfig(bucket string) jetstream.KeyValueConfig {
	return jetstream.KeyValueConfig{
		Bucket:   bucket,
		Storage:  jetstream.FileStorage,
		History:  1,
		Replicas: 1,
	}
}

// isJetStreamWrongLastSeq reports whether err is the jetstream package's
// expected-last-sequence rejection: a *jetstream.APIError carrying
// JSErrCodeStreamWrongLastSequence (10071). It is the jetstream-package analogue of
// isWrongLastSequence (which matches *nats.APIError for the legacy ledger seam); the
// two APIError types are distinct, so the leaser needs its own matcher.
func isJetStreamWrongLastSeq(err error) bool {
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
}

// leaseBucketConfig returns the jetstream KV bucket configuration for a lease bucket:
// file storage, one replica (single-node embedded), History 1 (a lease entry has no
// useful history), and a server-side TTL that is a generous backstop multiple of the
// application lease TTL (see backstopBucketTTL). Application-level ExpiresAt — not this
// bucket TTL — is the authoritative, takeover-driving expiry check; the bucket TTL only
// eventually reaps an entry whose holder died without releasing.
func leaseBucketConfig(bucket string, ttl time.Duration) jetstream.KeyValueConfig {
	return jetstream.KeyValueConfig{
		Bucket:   bucket,
		TTL:      backstopBucketTTL(ttl),
		Storage:  jetstream.FileStorage,
		History:  1,
		Replicas: 1,
	}
}

// jetStreamObjectSeam is the production objSeam: it binds the blob store's five operations
// to a real, context-aware jetstream.ObjectStore, translating ObjectStore's absence
// sentinels (ErrObjectNotFound / ErrNoObjectsFound) onto the store's package terms
// (errObjNotFound / empty listing) and decoding the stored "SHA-256=<base64>" digest to
// raw bytes for the store's content compare. It is the ONLY blob code that talks to NATS
// directly.
type jetStreamObjectSeam struct {
	obj jetstream.ObjectStore
}

var _ objSeam = (*jetStreamObjectSeam)(nil)

// newJetStreamObjectSeam wraps obj as a production blob-store seam.
func newJetStreamObjectSeam(obj jetstream.ObjectStore) *jetStreamObjectSeam {
	return &jetStreamObjectSeam{obj: obj}
}

// put stores data under key (ObjectStore computes and records the SHA-256 digest itself),
// bounded by ctx. ObjectStore.Put replaces any prior object at the name — the store only
// calls put for a new object, so no live content is clobbered.
func (s *jetStreamObjectSeam) put(ctx context.Context, key string, data []byte) error {
	_, err := s.obj.Put(ctx, jetstream.ObjectMeta{Name: key}, bytes.NewReader(data))
	return err
}

// get returns an ObjectStore result (an io.ReadCloser that verifies the digest on read),
// bounded by ctx, or errObjNotFound if absent.
func (s *jetStreamObjectSeam) get(ctx context.Context, key string) (io.ReadCloser, error) {
	res, err := s.obj.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil, errObjNotFound
		}
		return nil, err
	}
	return res, nil
}

// delete removes key, bounded by ctx. An absent object surfaces as
// jetstream.ErrObjectNotFound, which is mapped to success — the idempotent Delete contract.
func (s *jetStreamObjectSeam) delete(ctx context.Context, key string) error {
	err := s.obj.Delete(ctx, key)
	if err == nil || errors.Is(err, jetstream.ErrObjectNotFound) {
		return nil
	}
	return err
}

// list returns every live object name, bounded by ctx. An empty store surfaces as
// jetstream.ErrNoObjectsFound, mapped to an empty slice (absent == empty), not an error.
func (s *jetStreamObjectSeam) list(ctx context.Context) ([]string, error) {
	infos, err := s.obj.List(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
			return nil, nil
		}
		return nil, err
	}
	names := make([]string, 0, len(infos))
	for _, info := range infos {
		names = append(names, info.Name)
	}
	return names, nil
}

// getInfo returns the RAW SHA-256 digest of the object at key, bounded by ctx, or
// errObjNotFound if absent. ObjectStore stores the digest as "SHA-256=<base64url>";
// jetstream.DecodeObjectDigest reverses that to the 32 raw bytes the store compares. A
// malformed stored digest is a genuine backend fault (returned as-is, fail closed).
func (s *jetStreamObjectSeam) getInfo(ctx context.Context, key string) ([]byte, error) {
	info, err := s.obj.GetInfo(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil, errObjNotFound
		}
		return nil, err
	}
	digest, err := jetstream.DecodeObjectDigest(info.Digest)
	if err != nil {
		return nil, err
	}
	return digest, nil
}

// objectStoreConfig returns the jetstream ObjectStore configuration for the blob store's
// bucket: file storage and one replica (single-node embedded). MaxBytes is left at the
// default (unlimited); ObjectStore chunks large objects, so the storage 1 MiB blob floor
// round-trips without special sizing.
func objectStoreConfig(bucket string) jetstream.ObjectStoreConfig {
	return jetstream.ObjectStoreConfig{
		Bucket:   bucket,
		Storage:  jetstream.FileStorage,
		Replicas: 1,
	}
}
