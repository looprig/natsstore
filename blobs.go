package natsstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/looprig/storage"
)

// objSeam is the narrow set of JetStream ObjectStore operations blobStore drives,
// isolated behind an interface (DIP + ISP) so the store's content-addressed conflict
// logic can be unit-tested with a stateful fake and no running server. The production
// binding (jetStreamObjectSeam, seam.go) wraps a real, context-aware
// jetstream.ObjectStore. It is the ONLY blob code that talks to NATS directly.
//
// getInfo returns the object's stored SHA-256 digest as RAW bytes (the production seam
// decodes ObjectStore's "SHA-256=<base64>" form via jetstream.DecodeObjectDigest), so the
// store compares digests with a plain bytes.Equal and never has to reason about the wire
// encoding — keeping blobs.go free of any nats.go dependency.
type objSeam interface {
	// put stores data under key, overwriting any prior object there. The store only calls
	// put once it has confirmed key is absent (a new object) — an existing object is
	// resolved by the digest compare, never overwritten — so put never clobbers content.
	put(ctx context.Context, key string, data []byte) error
	// get returns an independent reader over the object at key, or errObjNotFound if
	// absent. The caller owns closing the reader.
	get(ctx context.Context, key string) (io.ReadCloser, error)
	// delete removes key. It is idempotent: deleting an absent key is a success.
	delete(ctx context.Context, key string) error
	// list returns every live object name in the bucket, in no particular order and
	// without a prefix filter; an empty bucket yields an empty slice, never an error. The
	// store sorts, dedups, and prefix-filters above it.
	list(ctx context.Context) (names []string, err error)
	// getInfo returns the RAW SHA-256 digest (32 bytes) of the object at key, or
	// errObjNotFound if absent.
	getInfo(ctx context.Context, key string) (digest []byte, err error)
}

// errObjNotFound is the seam-contract sentinel for an absent object (get / getInfo). It is
// a leaf error with no context fields, so a package sentinel is permitted (CLAUDE.md).
// Keeping the seam's absence signal in package terms — not NATS terms — is what lets the
// store logic and its unit fake stay free of any nats.go dependency.
var errObjNotFound = errors.New("natsstore: object not found")

// errNilObjectReader is the seam-contract cause for a successful get that did
// not return a usable reader. blobStore exposes it only through *BlobOpError.
var errNilObjectReader = errors.New("natsstore: object seam returned nil reader")

// BlobOpError reports a definite failure of a blob operation blobStore performs (read /
// put / get / delete / list / getInfo) that is NOT one of the expected outcomes (absence,
// content conflict) — a source-reader fault or a backend fault. It fails closed and names
// the key (or, for list, the prefix), the operation, and unwraps to the underlying cause.
// (The storage taxonomy's only blob errors are BlobNotFoundError and BlobConflictError —
// neither fits a reader/backend fault — so this is a natsstore-specific typed error.)
type BlobOpError struct {
	Key   string
	Op    string
	Cause error
}

func (e *BlobOpError) Error() string {
	return "natsstore: blob " + strconv.Quote(e.Key) + " " + e.Op + " failed: " + e.Cause.Error()
}
func (e *BlobOpError) Unwrap() error { return e.Cause }

// blobStore implements storage.Blobs over one JetStream ObjectStore, one object per key.
// It holds only the seam (DIP) and carries no mutable state, so it is as safe for
// concurrent use as the seam beneath it. Keys map DIRECTLY to ObjectStore object names
// (see blobKeyForName). Writes are content-addressed and immutable per key: an existing
// object with byte-identical content is a no-op success, and an existing object with
// different content is a conflict that leaves the original untouched.
type blobStore struct {
	seam objSeam
	localPathReporter
}

const (
	blobObjectReadTimeout   = 5 * time.Second
	blobReaderCloseOverhead = time.Second
	blobReaderCloseBound    = blobObjectReadTimeout + blobReaderCloseOverhead
)

// BlobReaderCloseBound reports the maximum time a reader returned by Get may
// take to stop an active provider-controlled Read and return from Close. The
// provider bounds each ephemeral pull-consumer chunk fetch at five seconds; the extra
// second covers cancellation and local close completion.
func (*blobStore) BlobReaderCloseBound() time.Duration {
	return blobReaderCloseBound
}

var _ storage.Blobs = (*blobStore)(nil)

// newBlobStore builds a blob store over seam.
func newBlobStore(seam objSeam) *blobStore {
	return &blobStore{seam: seam}
}

// Put stores the bytes read from r under key, enforcing content-addressed immutability. It
// reads r fully (required both to compute the SHA-256 content digest and to write the
// object), then:
//   - object absent            → put the new content.
//   - object present, identical → no-op success (no rewrite of the stored bytes).
//   - object present, different → *storage.BlobConflictError, original left untouched.
//
// A source-reader fault surfaces as a typed *BlobOpError (fail closed) before any backend
// write is attempted.
func (s *blobStore) Put(ctx context.Context, key string, r io.Reader) error {
	k, err := blobKeyForName(key)
	if err != nil {
		return err // *storage.InvalidNameError, verbatim
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return &BlobOpError{Key: key, Op: "read", Cause: err}
	}
	sum := sha256.Sum256(data)

	existing, err := s.seam.getInfo(ctx, k)
	switch {
	case errors.Is(err, errObjNotFound):
		if perr := s.seam.put(ctx, k, data); perr != nil {
			return &BlobOpError{Key: key, Op: "put", Cause: perr}
		}
		return nil
	case err != nil:
		return &BlobOpError{Key: key, Op: "getInfo", Cause: err}
	}
	if bytes.Equal(existing, sum[:]) {
		return nil // byte-identical: content-addressed no-op
	}
	return &storage.BlobConflictError{Key: key}
}

// Get returns an independent lifecycle reader over the object at key, or
// *storage.BlobNotFoundError if absent. The caller owns closing the reader; Close
// is concurrent-safe and stable, and later Reads fail with fs.ErrClosed. A backend
// fault, including a nil reader returned by the seam, is a typed *BlobOpError
// (fail closed) — never conflated with absence.
func (s *blobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	k, err := blobKeyForName(key)
	if err != nil {
		return nil, err // *storage.InvalidNameError, verbatim
	}
	rc, err := s.seam.get(ctx, k)
	if err != nil {
		if errors.Is(err, errObjNotFound) {
			return nil, &storage.BlobNotFoundError{Key: key}
		}
		return nil, &BlobOpError{Key: key, Op: "get", Cause: err}
	}
	if isNilReadCloser(rc) {
		return nil, &BlobOpError{Key: key, Op: "get", Cause: errNilObjectReader}
	}
	return newBlobLifecycleReader(rc), nil
}

func isNilReadCloser(reader io.ReadCloser) bool {
	return isNilInterface(reader)
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

// blobLifecycleReader adapts nats.go's streaming ObjectResult to the Blob reader
// lifecycle. Closure is published before touching the underlying reader, while
// sync.Once makes concurrent Close calls wait for and return one stable result.
type blobLifecycleReader struct {
	underlying io.ReadCloser
	closed     atomic.Bool
	closeOnce  sync.Once
	closeDone  chan struct{}
	closeErr   error
}

func newBlobLifecycleReader(underlying io.ReadCloser) *blobLifecycleReader {
	return &blobLifecycleReader{underlying: underlying, closeDone: make(chan struct{})}
}

func (r *blobLifecycleReader) Read(p []byte) (int, error) {
	if r.closed.Load() {
		return 0, fs.ErrClosed
	}
	return r.underlying.Read(p)
}

func (r *blobLifecycleReader) Close() error {
	r.closed.Store(true)
	r.closeOnce.Do(func() {
		r.closeErr = r.underlying.Close()
		close(r.closeDone)
	})
	<-r.closeDone
	return r.closeErr
}

// Delete removes the object at key. It is idempotent: deleting an absent key is a success,
// so a deleted key is indistinguishable from a never-written one.
func (s *blobStore) Delete(ctx context.Context, key string) error {
	k, err := blobKeyForName(key)
	if err != nil {
		return err // *storage.InvalidNameError, verbatim
	}
	if err := s.seam.delete(ctx, k); err != nil {
		return &BlobOpError{Key: key, Op: "delete", Cause: err}
	}
	return nil
}

// List returns the object keys that begin with prefix, lexicographically ascending and
// duplicate-free. The prefix is a literal string filter over the direct key mapping and is
// NOT validated (a caller may list by any prefix). Sorting and dedup happen locally.
func (s *blobStore) List(ctx context.Context, prefix string) ([]string, error) {
	all, err := s.seam.list(ctx)
	if err != nil {
		return nil, &BlobOpError{Key: prefix, Op: "list", Cause: err}
	}
	return sortedDedupFiltered(all, prefix), nil
}

// blobKeyForName validates a storage key and returns it UNCHANGED as the ObjectStore
// object name. ObjectStore base64-encodes the name internally for its meta/chunk subjects,
// so it imposes no charset restriction of its own; the identity mapping therefore
// round-trips every valid storage key verbatim (List returns the original names) and
// List(prefix) is a literal substring filter. It returns the *storage.InvalidNameError
// verbatim for an invalid key.
func blobKeyForName(key string) (string, error) {
	if err := storage.ValidateName(key); err != nil {
		return "", err
	}
	return key, nil
}
