package natsstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/looprig/storekit"
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
			var bc *storekit.BlobConflictError
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
	var nf *storekit.BlobNotFoundError
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
			if _, err := s.Get(ctx, key); !errors.As(err, new(*storekit.BlobNotFoundError)) {
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
				var ine *storekit.InvalidNameError
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
			// A backend fault must never be mis-typed as a storekit blob outcome.
			if errors.As(err, new(*storekit.BlobConflictError)) || errors.As(err, new(*storekit.BlobNotFoundError)) {
				t.Error("backend fault mis-classified as a storekit blob outcome")
			}
		})
	}
}
