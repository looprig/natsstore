package natsstore

// This file is natsstore's composition root: it wires the four JetStream backends
// (ledger, leaser, KV, blobs) over a single NATS connection into one storage
// field-bundle and exposes lifecycle (Open/Close). It is the module's public entry
// point; a consumer opens a Store here and hands its *storage.Composite to a facade
// such as sessionstore.Open.
//
// # Two backends behind one door
//
// A Store is backed either by an EMBEDDED, in-process JetStream server (the Store owns
// the Engine and its DontListen server under a caller-provided dir) or by a REMOTE NATS
// URL (the Store owns only the connection). Exactly one is chosen at Open; both wire the
// identical four-primitive Composite over the resulting connection.
//
// # Why a field-bundle, not an all-four interface
//
// storage's four primitives collide on method names (each of Ledger, KV, and Blobs has
// its own Delete), so NO single Go type can implement all four at once. storage solves
// this with Composite, a struct that embeds one provider per primitive. Store follows the
// same shape (as fsstore does): it embeds *storage.Composite rather than pretending to
// satisfy Ledger+Leaser+KV+Blobs itself. A consumer that wants the whole bundle is handed
// store.Composite (or Backend()).

import (
	"context"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/looprig/storage"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// leaseBucketName is the KV bucket holding the leaser's fencing entries. It is
	// DISTINCT from kvBucketName so the leaser's short-TTL, epoch-CAS entries never share
	// a bucket with the application KV store's latest-value entries.
	leaseBucketName = "natsstore_leases"
	// kvBucketName is the KV bucket backing the application KV store (the session
	// catalog). DISTINCT from the lease bucket.
	kvBucketName = "natsstore_kv"
	// blobsBucketName is the ObjectStore bucket backing the blob store.
	blobsBucketName = "natsstore_blobs"

	// remoteConnectTimeout bounds the initial dial of a remote NATS URL. It is set
	// EXPLICITLY (the nats.go default is 2s) so a slow or unreachable broker fails closed
	// within a predictable window rather than at the transport's own discretion.
	remoteConnectTimeout = 10 * time.Second
	// remoteDrainTimeout bounds Close's graceful drain of a remote connection: pending
	// publishes are flushed and subscriptions unsubscribed within this window before the
	// connection is force-closed. It is the remote analogue of the embedded engine's
	// drain-before-shutdown ordering.
	remoteDrainTimeout = 30 * time.Second
)

// OptionsError reports an invalid or unusable Open option: neither or both of
// URL/EmbeddedDir set, a non-absolute EmbeddedDir, or a MaxPayload below the ledger
// stream's per-message ceiling. Field names the offending option and Reason explains the
// fault. It is a pure validation failure (no underlying cause), so it does not Unwrap.
type OptionsError struct {
	Field  string
	Reason string
}

func (e *OptionsError) Error() string {
	return "natsstore: invalid option " + strconv.Quote(e.Field) + ": " + e.Reason
}

// ConnectError reports that Open could not dial a remote NATS URL. URL is REDACTED (any
// userinfo password is stripped — a NATS URL may embed credentials, which must never
// reach a log), and Cause unwraps to the underlying nats.go dial error.
type ConnectError struct {
	URL   string
	Cause error
}

func (e *ConnectError) Error() string {
	return "natsstore: connect to " + strconv.Quote(e.URL) + " failed: " + e.Cause.Error()
}
func (e *ConnectError) Unwrap() error { return e.Cause }

// WiringError reports that Open connected but could not assemble the four-primitive
// bundle: a JetStream context could not be bound, a KV/object bucket could not be
// provisioned, or the composite rejected a nil primitive. Component names the failed
// step; Cause unwraps to the underlying error. On a WiringError Open tears the
// connection/engine down and returns no Store.
type WiringError struct {
	Component string
	Cause     error
}

func (e *WiringError) Error() string {
	return "natsstore: wiring " + strconv.Quote(e.Component) + " failed: " + e.Cause.Error()
}
func (e *WiringError) Unwrap() error { return e.Cause }

// Options configures Open. Exactly one of URL or EmbeddedDir must be set (else an
// *OptionsError): URL selects a remote NATS backend, EmbeddedDir selects an embedded
// in-process JetStream server that the Store owns.
type Options struct {
	// URL is a remote NATS server URL (e.g. "nats://host:4222"). XOR EmbeddedDir. When
	// set, Open dials it with explicit, secure defaults (no InsecureSkipVerify, an
	// explicit connect timeout).
	URL string
	// EmbeddedDir is the StoreDir for an embedded, in-process JetStream server the Store
	// owns. XOR URL. It is used DIRECTLY as the engine StoreDir (no ~/.looprig default,
	// no home/XDG confinement — the caller explicitly owns the path); it is
	// filepath.Clean'd and must be absolute (a relative/empty dir is an *OptionsError).
	// It is created 0700 if absent.
	EmbeddedDir string
	// MaxPayload is the connection-level maximum message size for the EMBEDDED server.
	// In embedded mode a zero/negative value applies the 8 MiB default and a positive
	// value must be >= the ledger stream's per-message ceiling (4 MiB) or Open returns an
	// *OptionsError. In remote mode it is ignored entirely (neither applied nor
	// validated): the remote broker owns its own MaxPayload.
	MaxPayload int32
}

// storeMode is the backend a resolved Options selects.
type storeMode int

const (
	modeInvalid storeMode = iota
	modeEmbedded
	modeRemote
)

// resolvedOptions is the validated, normalized form of Options: exactly one mode, a
// cleaned absolute embeddedDir (embedded mode) or a trimmed url (remote mode), and a
// resolved maxPayload (default applied, floor checked). Producing it is server-free, so
// the validation is unit-testable without starting a backend.
type resolvedOptions struct {
	mode        storeMode
	url         string
	embeddedDir string
	maxPayload  int32
}

// resolve validates and normalizes o. It rejects neither/both of URL/EmbeddedDir and a
// non-absolute EmbeddedDir (each a typed *OptionsError), and — in embedded mode only,
// where MaxPayload actually configures the owned server — applies the MaxPayload default
// and enforces its floor. In remote mode MaxPayload plays no role (the remote broker owns
// its own limit), so it is neither applied nor validated. resolve never touches the
// filesystem or network.
func (o Options) resolve() (resolvedOptions, error) {
	hasURL := strings.TrimSpace(o.URL) != ""
	hasDir := strings.TrimSpace(o.EmbeddedDir) != ""
	switch {
	case hasURL && hasDir:
		return resolvedOptions{}, &OptionsError{Field: "URL/EmbeddedDir", Reason: "exactly one of URL or EmbeddedDir must be set, not both"}
	case !hasURL && !hasDir:
		return resolvedOptions{}, &OptionsError{Field: "URL/EmbeddedDir", Reason: "exactly one of URL or EmbeddedDir must be set"}
	}
	if hasDir {
		dir := filepath.Clean(o.EmbeddedDir)
		if !filepath.IsAbs(dir) {
			return resolvedOptions{}, &OptionsError{Field: "EmbeddedDir", Reason: "must be an absolute path"}
		}
		maxPay, err := resolveMaxPayload(o.MaxPayload)
		if err != nil {
			return resolvedOptions{}, err
		}
		return resolvedOptions{mode: modeEmbedded, embeddedDir: dir, maxPayload: maxPay}, nil
	}
	return resolvedOptions{mode: modeRemote, url: strings.TrimSpace(o.URL)}, nil
}

// resolveMaxPayload applies the 8 MiB default for a non-positive request and enforces the
// invariant that the connection-level MaxPayload is >= the ledger stream's per-message
// ceiling (ledgerMaxMsgSize) — a smaller value would reject a floor-sized append at the
// connection with a confusing ErrMaxPayload before the stream cap is ever consulted.
func resolveMaxPayload(requested int32) (int32, error) {
	if requested <= 0 {
		return maxPayload, nil
	}
	if requested < ledgerMaxMsgSize {
		return 0, &OptionsError{
			Field:  "MaxPayload",
			Reason: "must be >= the ledger stream ceiling (" + strconv.FormatInt(int64(ledgerMaxMsgSize), 10) + " bytes)",
		}
	}
	return requested, nil
}

// Store is an open natsstore over one NATS backend — an owned embedded engine, or a
// remote connection. It embeds *storage.Composite, so a caller reaches each primitive as
// a promoted field (store.Ledger, store.Leaser, store.KV, store.Blobs) and hands the whole
// bundle to a consumer as store.Composite or via Backend. The four primitives collide on
// method names, so no single type can implement all four (see the file-level comment);
// embedding the field-bundle is how Store sidesteps that.
type Store struct {
	*storage.Composite

	// engine is the owned embedded engine (embedded mode); conn is the owned remote
	// connection (remote mode). Exactly one is non-nil, decided at Open, and it is what
	// Close tears down.
	engine *Engine
	conn   *nats.Conn

	mu     sync.Mutex
	closed bool
}

// Open assembles a JetStream-backed storage bundle over a single NATS backend chosen by
// opts: an embedded in-process server the Store owns (Options.EmbeddedDir) or a remote URL
// (Options.URL). It validates opts (*OptionsError on a bad combination), stands the
// backend up, provisions the ledger stream lazily plus the lease/kv/object buckets
// idempotently (so a reopen of the same embedded dir rebinds rather than fails), and wires
// the four primitives with storage.NewComposite.
//
// ctx bounds the bucket-provisioning round-trips; pass a ctx with a deadline. On any
// wiring failure Open tears the backend down (drains the connection, shuts an embedded
// engine down) and returns the typed error — never a half-open Store.
//
// Embedded mode takes NO cross-process store lock (it drives the engine directly rather
// than via LockedEngine), so the caller must guarantee a single open per EmbeddedDir —
// two Stores or processes over the same dir would corrupt the JetStream file store.
func Open(ctx context.Context, opts Options) (*Store, error) {
	res, err := opts.resolve()
	if err != nil {
		return nil, err
	}
	switch res.mode {
	case modeEmbedded:
		return openEmbedded(ctx, res)
	case modeRemote:
		return openRemote(ctx, res)
	default:
		// Unreachable: resolve returns a mode or an error.
		return nil, &OptionsError{Field: "URL/EmbeddedDir", Reason: "no store mode resolved"}
	}
}

// openEmbedded starts an owned embedded engine on the caller's absolute dir (no home/XDG
// confinement — see openEngineAt) and wires the composite over its in-process connection.
// A wiring failure shuts the engine down before returning.
func openEmbedded(ctx context.Context, res resolvedOptions) (*Store, error) {
	eng, err := openEngineAt(res.embeddedDir, 0, res.maxPayload)
	if err != nil {
		return nil, err
	}
	comp, err := buildComposite(ctx, eng.Conn())
	if err != nil {
		_ = eng.Close()
		return nil, err
	}
	return &Store{Composite: comp, engine: eng}, nil
}

// openRemote dials the remote URL with explicit, secure defaults (no InsecureSkipVerify;
// explicit connect + drain timeouts) and wires the composite over the connection. A dial
// failure is a *ConnectError with a redacted URL; a wiring failure closes the connection
// before returning.
func openRemote(ctx context.Context, res resolvedOptions) (*Store, error) {
	conn, err := nats.Connect(res.url,
		nats.Name("natsstore"),
		nats.Timeout(remoteConnectTimeout),
		nats.DrainTimeout(remoteDrainTimeout),
	)
	if err != nil {
		return nil, &ConnectError{URL: redactURL(res.url), Cause: err}
	}
	comp, err := buildComposite(ctx, conn)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &Store{Composite: comp, conn: conn}, nil
}

// backingBuckets are the three provisioned JetStream buckets a Store's leaser, KV, and
// blob stores bind to. The ledger provisions its stream lazily on first append, so it is
// not part of this bundle.
type backingBuckets struct {
	lease jetstream.KeyValue
	kv    jetstream.KeyValue
	obj   jetstream.ObjectStore
}

// buildComposite wires the four backing stores over conn: the ledger on the legacy
// JetStreamContext (its expected-last-sequence publish fence lives on that API), and the
// leaser/kv/blobs on the context-aware jetstream package (their round-trips honor ctx). It
// returns a wiring failure as a typed *WiringError; the caller owns tearing conn down.
func buildComposite(ctx context.Context, conn *nats.Conn) (*storage.Composite, error) {
	jsLegacy, err := conn.JetStream()
	if err != nil {
		return nil, &WiringError{Component: "jetstream-context", Cause: err}
	}
	jsx, err := jetstream.New(conn)
	if err != nil {
		return nil, &WiringError{Component: "jetstream", Cause: err}
	}
	buckets, err := provisionBuckets(ctx, jsx)
	if err != nil {
		return nil, err
	}
	ledger := newLedgerStore(newJetStreamSeam(jsLegacy))
	leaser := newLeaserStore(newJetStreamKVSeam(buckets.lease), defaultLeaseTTL, time.Now)
	kv := newKVStore(newJetStreamKVStoreSeam(buckets.kv))
	blobs := newBlobStore(newJetStreamObjectSeam(buckets.obj))
	comp, err := storage.NewComposite(ledger, leaser, kv, blobs)
	if err != nil {
		return nil, &WiringError{Component: "composite", Cause: err}
	}
	return comp, nil
}

// provisionBuckets idempotently creates (or rebinds, on reopen) the lease KV bucket, the
// application KV bucket, and the blob object store, bounded by ctx. CreateOrUpdate* makes
// a reopen of a durable StoreDir rebind the existing buckets rather than fail with
// ErrBucketExists. Each failure is a typed *WiringError naming the bucket.
func provisionBuckets(ctx context.Context, jsx jetstream.JetStream) (backingBuckets, error) {
	lease, err := jsx.CreateOrUpdateKeyValue(ctx, leaseBucketConfig(leaseBucketName, defaultLeaseTTL))
	if err != nil {
		return backingBuckets{}, &WiringError{Component: "lease-bucket", Cause: err}
	}
	kv, err := jsx.CreateOrUpdateKeyValue(ctx, kvBucketConfig(kvBucketName))
	if err != nil {
		return backingBuckets{}, &WiringError{Component: "kv-bucket", Cause: err}
	}
	obj, err := jsx.CreateOrUpdateObjectStore(ctx, objectStoreConfig(blobsBucketName))
	if err != nil {
		return backingBuckets{}, &WiringError{Component: "blobs-bucket", Cause: err}
	}
	return backingBuckets{lease: lease, kv: kv, obj: obj}, nil
}

// Backend returns the assembled four-primitive bundle to hand to a consumer such as
// sessionstore.Open. It is the embedded *storage.Composite; callers may read
// store.Composite directly instead.
func (s *Store) Backend() *storage.Composite { return s.Composite }

// Close tears the Store's owned backend down: it drains the connection (flushing an
// in-flight append) and, in embedded mode, shuts the in-process server down afterwards —
// the drain-before-shutdown ordering ported from the embedded engine's Close. It surfaces
// the first error and is idempotent: a second call is a no-op returning nil. After Close
// the Store must not be reused.
//
// ctx is accepted for contract uniformity; the drain itself is bounded by the connection's
// drain timeout (remote: remoteDrainTimeout; embedded: the engine's default), as the
// underlying nats.go drain is not context-aware.
func (s *Store) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	switch {
	case s.engine != nil:
		return s.engine.Close()
	case s.conn != nil:
		return s.conn.Drain()
	default:
		return nil
	}
}

// redactURL strips any userinfo password from a NATS URL (or comma-separated URL list) so
// a *ConnectError never carries credentials into a log. Each comma-separated entry is
// parsed and rendered via url.URL.Redacted (password → "xxxxx"); an unparseable entry
// collapses to "[redacted]" rather than risk echoing a secret.
func redactURL(raw string) string {
	parts := strings.Split(raw, ",")
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if u, err := url.Parse(p); err == nil {
			parts[i] = u.Redacted()
		} else {
			parts[i] = "[redacted]"
		}
	}
	return strings.Join(parts, ",")
}
