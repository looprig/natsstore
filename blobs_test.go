package natsstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/storage"
)

// fakeObjSeam is a stateful, in-memory objSeam for unit-testing blobStore with no running
// server. It models ObjectStore closely enough to drive the store's full contract: put
// overwrites, get/getInfo report absence via errObjNotFound, getInfo returns the SHA-256
// of the stored bytes (as a real ObjectStore records the digest of stored content), delete
// is an idempotent tombstone, and list yields live names. It counts put calls so a test
// can prove a byte-identical re-Put performs ZERO rewrite.
type fakeObjSeam struct {
	mu       sync.Mutex
	store    map[string][]byte
	putCalls int

	// forceErr, when non-nil, makes every op return it (a non-sentinel backend fault) so a
	// test can exercise blobStore's fail-closed *BlobOpError path.
	forceErr error
	// listOverride, when non-nil, is returned verbatim by list() — including duplicates and
	// out-of-order entries a real bucket never yields — to prove blobStore sorts, dedups,
	// and prefix-filters above the seam.
	listOverride []string
}

func newFakeObjSeam() *fakeObjSeam {
	return &fakeObjSeam{store: make(map[string][]byte)}
}

func (f *fakeObjSeam) put(_ context.Context, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forceErr != nil {
		return f.forceErr
	}
	f.putCalls++
	f.store[key] = append([]byte(nil), data...)
	return nil
}

func (f *fakeObjSeam) get(_ context.Context, key string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forceErr != nil {
		return nil, f.forceErr
	}
	data, ok := f.store[key]
	if !ok {
		return nil, errObjNotFound
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), data...))), nil
}

func (f *fakeObjSeam) delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forceErr != nil {
		return f.forceErr
	}
	delete(f.store, key)
	return nil // idempotent even when the key was never present
}

func (f *fakeObjSeam) list(_ context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forceErr != nil {
		return nil, f.forceErr
	}
	if f.listOverride != nil {
		return append([]string(nil), f.listOverride...), nil
	}
	var out []string
	for k := range f.store {
		out = append(out, k)
	}
	return out, nil // deliberately unsorted: blobStore.List must sort
}

func (f *fakeObjSeam) getInfo(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forceErr != nil {
		return nil, f.forceErr
	}
	data, ok := f.store[key]
	if !ok {
		return nil, errObjNotFound
	}
	sum := sha256.Sum256(data)
	return sum[:], nil
}

// errReader is an io.Reader that always fails, to exercise the source-reader fault path.
type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

func readClose(t *testing.T, rc io.ReadCloser) []byte {
	t.Helper()
	defer func() {
		if err := rc.Close(); err != nil {
			t.Errorf("reader Close: %v, want nil", err)
		}
	}()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return data
}

func TestBlobsPutGetRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name    string
		content []byte
	}{
		{name: "non-empty", content: []byte("some bytes")},
		{name: "empty", content: []byte{}},
		{name: "binary", content: []byte{0x00, 0xff, 0x10, 0x00}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			const key = "blobs/roundtrip"
			s := newBlobStore(newFakeObjSeam())
			if err := s.Put(ctx, key, bytes.NewReader(tt.content)); err != nil {
				t.Fatalf("Put: %v", err)
			}
			rc, err := s.Get(ctx, key)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got := readClose(t, rc); !bytes.Equal(got, tt.content) {
				t.Errorf("Get = %q, want %q", got, tt.content)
			}
		})
	}
}

func TestBlobsIdenticalRePutIsNoOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const key = "blobs/identical"
	content := []byte("content-addressed bytes")
	f := newFakeObjSeam()
	s := newBlobStore(f)

	if err := s.Put(ctx, key, bytes.NewReader(content)); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if err := s.Put(ctx, key, bytes.NewReader(content)); err != nil {
		t.Fatalf("re-Put(identical) = %v, want nil (no-op)", err)
	}
	// The identical re-Put must NOT rewrite the object: exactly one put reached the seam.
	f.mu.Lock()
	puts := f.putCalls
	f.mu.Unlock()
	if puts != 1 {
		t.Errorf("put calls after identical re-Put = %d, want 1 (no rewrite)", puts)
	}
	rc, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := readClose(t, rc); !bytes.Equal(got, content) {
		t.Errorf("Get after identical re-Put = %q, want %q", got, content)
	}
}

func TestBlobsDifferentContentConflictLeavesOriginal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name     string
		original []byte
		second   []byte
	}{
		{name: "different bytes same length", original: []byte("aaaa"), second: []byte("bbbb")},
		{name: "shorter is different", original: []byte("aaaa"), second: []byte("aa")},
		{name: "longer is different", original: []byte("aa"), second: []byte("aaaa")},
		{name: "non-empty vs empty", original: []byte("aa"), second: []byte{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			const key = "blobs/conflict"
			f := newFakeObjSeam()
			s := newBlobStore(f)
			if err := s.Put(ctx, key, bytes.NewReader(tt.original)); err != nil {
				t.Fatalf("first Put: %v", err)
			}
			err := s.Put(ctx, key, bytes.NewReader(tt.second))
			var bc *storage.BlobConflictError
			if !errors.As(err, &bc) {
				t.Fatalf("re-Put(different) = %v, want *BlobConflictError", err)
			}
			if bc.Key != key {
				t.Errorf("BlobConflictError.Key = %q, want %q", bc.Key, key)
			}
			// Exactly one put reached the seam: the conflicting Put never wrote.
			f.mu.Lock()
			puts := f.putCalls
			f.mu.Unlock()
			if puts != 1 {
				t.Errorf("put calls after rejected Put = %d, want 1 (original untouched)", puts)
			}
			rc, gerr := s.Get(ctx, key)
			if gerr != nil {
				t.Fatalf("Get: %v", gerr)
			}
			if got := readClose(t, rc); !bytes.Equal(got, tt.original) {
				t.Errorf("Get after rejected Put = %q, want original %q", got, tt.original)
			}
		})
	}
}

func TestBlobsGetAbsentReturnsBlobNotFound(t *testing.T) {
	t.Parallel()
	const key = "blobs/missing"
	s := newBlobStore(newFakeObjSeam())
	_, err := s.Get(context.Background(), key)
	var nf *storage.BlobNotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("Get(absent) = %v, want *BlobNotFoundError", err)
	}
	if nf.Key != key {
		t.Errorf("BlobNotFoundError.Key = %q, want %q", nf.Key, key)
	}
}

func TestBlobsListSortedDedupPrefixFiltered(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		override []string
		prefix   string
		want     []string
	}{
		{
			name:     "empty prefix returns all sorted deduped",
			override: []string{"blobs/c", "blobs/a", "blobs/a", "snaps/z", "blobs/b"},
			prefix:   "",
			want:     []string{"blobs/a", "blobs/b", "blobs/c", "snaps/z"},
		},
		{
			name:     "prefix filters and sorts",
			override: []string{"blobs/c", "blobs/a", "snaps/z", "blobs/b"},
			prefix:   "blobs/",
			want:     []string{"blobs/a", "blobs/b", "blobs/c"},
		},
		{
			name:     "prefix matching nothing is empty",
			override: []string{"blobs/a"},
			prefix:   "snaps/",
			want:     nil,
		},
		{
			name:     "empty store is empty",
			override: nil,
			prefix:   "",
			want:     nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newFakeObjSeam()
			f.listOverride = tt.override
			s := newBlobStore(f)
			got, err := s.List(context.Background(), tt.prefix)
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if !equalStrings(got, tt.want) {
				t.Errorf("List(%q) = %v, want %v", tt.prefix, got, tt.want)
			}
		})
	}
}

func TestBlobsDeleteIdempotentAndFreesKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name    string
		seed    bool
		deletes int
	}{
		{name: "delete absent is nil", seed: false, deletes: 1},
		{name: "delete existing removes", seed: true, deletes: 1},
		{name: "double delete is nil", seed: true, deletes: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			const key = "blobs/del"
			s := newBlobStore(newFakeObjSeam())
			if tt.seed {
				if err := s.Put(ctx, key, bytes.NewReader([]byte("v"))); err != nil {
					t.Fatalf("seed Put: %v", err)
				}
			}
			for i := 0; i < tt.deletes; i++ {
				if err := s.Delete(ctx, key); err != nil {
					t.Fatalf("Delete call %d = %v, want nil (idempotent)", i, err)
				}
			}
			if _, err := s.Get(ctx, key); !errors.As(err, new(*storage.BlobNotFoundError)) {
				t.Errorf("Get after delete = %v, want *BlobNotFoundError", err)
			}
			// A deleted key is free: a fresh Put of new content succeeds with no lingering
			// conflict against the deleted bytes.
			if err := s.Put(ctx, key, bytes.NewReader([]byte("fresh"))); err != nil {
				t.Errorf("Put after delete = %v, want nil (key free)", err)
			}
		})
	}
}

func TestBlobsInvalidKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	methods := []struct {
		method string
		call   func(s *blobStore, key string) error
	}{
		{"Put", func(s *blobStore, key string) error { return s.Put(ctx, key, bytes.NewReader([]byte("x"))) }},
		{"Get", func(s *blobStore, key string) error { _, err := s.Get(ctx, key); return err }},
		{"Delete", func(s *blobStore, key string) error { return s.Delete(ctx, key) }},
	}
	badNames := []struct {
		label string
		value string
	}{
		{"empty", ""},
		{"leading slash", "/leading"},
		{"trailing slash", "trailing/"},
		{"doubled slash", "a//b"},
		{"uppercase", "Upper"},
		{"space", "has space"},
		{"dot-dot segment", ".."},
	}
	for _, m := range methods {
		for _, bad := range badNames {
			t.Run(m.method+"/"+bad.label, func(t *testing.T) {
				t.Parallel()
				f := newFakeObjSeam()
				s := newBlobStore(f)
				err := m.call(s, bad.value)
				var ine *storage.InvalidNameError
				if !errors.As(err, &ine) {
					t.Fatalf("%s(%q) = %v, want *InvalidNameError", m.method, bad.value, err)
				}
				if ine.Name != bad.value {
					t.Errorf("InvalidNameError.Name = %q, want %q", ine.Name, bad.value)
				}
				f.mu.Lock()
				n := len(f.store)
				f.mu.Unlock()
				if n != 0 {
					t.Errorf("invalid key touched the backend: %d objects, want 0", n)
				}
			})
		}
	}
}

func TestBlobsReaderFaultFailsClosed(t *testing.T) {
	t.Parallel()
	boom := errors.New("source read failed")
	f := newFakeObjSeam()
	s := newBlobStore(f)
	err := s.Put(context.Background(), "blobs/x", errReader{err: boom})
	var be *BlobOpError
	if !errors.As(err, &be) {
		t.Fatalf("Put(errReader) = %v, want *BlobOpError (fail closed)", err)
	}
	if !errors.Is(err, boom) {
		t.Error("BlobOpError does not unwrap to the reader cause")
	}
	// A reader fault must never write to the backend.
	f.mu.Lock()
	puts, n := f.putCalls, len(f.store)
	f.mu.Unlock()
	if puts != 0 || n != 0 {
		t.Errorf("reader fault touched the backend: putCalls=%d objects=%d, want 0/0", puts, n)
	}
}

func TestBlobsBackendFaultFailsClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	boom := errors.New("object store unavailable")
	ops := []struct {
		name string
		call func(s *blobStore) error
	}{
		{"Put", func(s *blobStore) error { return s.Put(ctx, "blobs/x", bytes.NewReader([]byte("v"))) }},
		{"Get", func(s *blobStore) error { _, err := s.Get(ctx, "blobs/x"); return err }},
		{"Delete", func(s *blobStore) error { return s.Delete(ctx, "blobs/x") }},
		{"List", func(s *blobStore) error { _, err := s.List(ctx, ""); return err }},
	}
	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			t.Parallel()
			f := newFakeObjSeam()
			f.forceErr = boom
			s := newBlobStore(f)
			err := op.call(s)
			var be *BlobOpError
			if !errors.As(err, &be) {
				t.Fatalf("%s = %v, want *BlobOpError (fail closed)", op.name, err)
			}
			if !errors.Is(err, boom) {
				t.Error("BlobOpError does not unwrap to the backend cause")
			}
			// A backend fault must never be mis-typed as a storage blob outcome.
			if errors.As(err, new(*storage.BlobConflictError)) || errors.As(err, new(*storage.BlobNotFoundError)) {
				t.Error("backend fault mis-classified as a storage blob outcome")
			}
		})
	}
}

func TestBlobsGetRejectsNilSeamReader(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		reader io.ReadCloser
	}{
		{name: "literal nil"},
		{name: "typed nil", reader: (*serializedReadCloser)(nil)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			seam := &scriptedGetObjSeam{fakeObjSeam: newFakeObjSeam(), reader: tt.reader}
			store := newBlobStore(seam)
			reader, err := store.Get(context.Background(), "blobs/nil-reader")
			var opErr *BlobOpError
			if reader != nil || !errors.As(err, &opErr) || opErr.Op != "get" {
				t.Fatalf("Get = %v, %T %v; want nil, *BlobOpError(get)", reader, err, err)
			}
		})
	}
}

func TestBlobReaderLifecycleCloseIsConcurrentAndStable(t *testing.T) {
	t.Parallel()

	readCause := errors.New("read stopped")
	closeCause := errors.New("close failed")
	underlying := newSerializedReadCloser(readCause, closeCause)
	store := newBlobStore(&scriptedGetObjSeam{fakeObjSeam: newFakeObjSeam(), reader: underlying})
	reader, err := store.Get(context.Background(), "blobs/lifecycle")
	if err != nil {
		t.Fatal(err)
	}

	readDone := make(chan error, 1)
	go func() {
		_, readErr := reader.Read(make([]byte, 1))
		readDone <- readErr
	}()
	select {
	case <-underlying.readStarted:
	case <-time.After(time.Second):
		t.Fatal("Read did not enter the underlying reader")
	}

	const closeCallers = 8
	closeResults := make(chan error, closeCallers)
	for range closeCallers {
		go func() { closeResults <- reader.Close() }()
	}
	select {
	case <-underlying.closeStarted:
		t.Fatal("underlying Close passed the active Read before it was released")
	case <-time.After(20 * time.Millisecond):
	}
	close(underlying.releaseRead)

	select {
	case readErr := <-readDone:
		if !errors.Is(readErr, readCause) {
			t.Fatalf("active Read = %v, want read cause", readErr)
		}
	case <-time.After(time.Second):
		t.Fatal("active Read did not return")
	}
	for range closeCallers {
		select {
		case closeErr := <-closeResults:
			if !errors.Is(closeErr, closeCause) {
				t.Fatalf("Close = %v, want stable close cause", closeErr)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent Close did not return")
		}
	}
	if calls := underlying.closeCalls.Load(); calls != 1 {
		t.Fatalf("underlying Close calls = %d, want 1", calls)
	}
	for i := 0; i < 3; i++ {
		if n, readErr := reader.Read(make([]byte, 1)); n != 0 || !errors.Is(readErr, fs.ErrClosed) || errors.Is(readErr, io.EOF) {
			t.Fatalf("post-Close Read %d = %d, %v; want 0, fs.ErrClosed, non-EOF", i, n, readErr)
		}
	}
}

func TestBlobReaderLifecyclePublishesClosedBeforeUnderlyingCloseReturns(t *testing.T) {
	t.Parallel()

	underlying := newSerializedReadCloser(nil, nil)
	underlying.blockClose = make(chan struct{})
	store := newBlobStore(&scriptedGetObjSeam{fakeObjSeam: newFakeObjSeam(), reader: underlying})
	reader, err := store.Get(context.Background(), "blobs/publish-close")
	if err != nil {
		t.Fatal(err)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- reader.Close() }()
	select {
	case <-underlying.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("underlying Close did not start")
	}
	readDone := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		n, readErr := reader.Read(make([]byte, 1))
		readDone <- struct {
			n   int
			err error
		}{n: n, err: readErr}
	}()
	select {
	case result := <-readDone:
		if result.n != 0 || !errors.Is(result.err, fs.ErrClosed) {
			t.Fatalf("Read during Close = %d, %v; want 0, fs.ErrClosed", result.n, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("closed state was not published before underlying Close returned")
	}
	if calls := underlying.readCalls.Load(); calls != 0 {
		t.Fatalf("underlying Read calls after closure = %d, want 0", calls)
	}
	close(underlying.blockClose)
	if closeErr := <-closeDone; closeErr != nil {
		t.Fatalf("Close = %v", closeErr)
	}
}

func TestBlobReaderCloseBoundAdvertised(t *testing.T) {
	t.Parallel()

	store := newBlobStore(newFakeObjSeam())
	if got := store.BlobReaderCloseBound(); got != 6*time.Second {
		t.Fatalf("BlobReaderCloseBound = %v, want 6s", got)
	}
}

type scriptedGetObjSeam struct {
	*fakeObjSeam
	reader io.ReadCloser
}

func (s *scriptedGetObjSeam) get(context.Context, string) (io.ReadCloser, error) {
	return s.reader, nil
}

type serializedReadCloser struct {
	mu           sync.Mutex
	readStarted  chan struct{}
	releaseRead  chan struct{}
	closeStarted chan struct{}
	blockClose   chan struct{}
	readCause    error
	closeCause   error
	readOnce     sync.Once
	closeOnce    sync.Once
	readCalls    atomic.Int32
	closeCalls   atomic.Int32
}

func newSerializedReadCloser(readCause, closeCause error) *serializedReadCloser {
	return &serializedReadCloser{
		readStarted:  make(chan struct{}),
		releaseRead:  make(chan struct{}),
		closeStarted: make(chan struct{}),
		readCause:    readCause,
		closeCause:   closeCause,
	}
}

func (r *serializedReadCloser) Read([]byte) (int, error) {
	r.readCalls.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readOnce.Do(func() { close(r.readStarted) })
	<-r.releaseRead
	return 0, r.readCause
}

func (r *serializedReadCloser) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closeCalls.Add(1)
	r.closeOnce.Do(func() { close(r.closeStarted) })
	if r.blockClose != nil {
		<-r.blockClose
	}
	return r.closeCause
}
