package natsstore

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/looprig/storage"
)

// fakeKVStoreSeam is a stateful, in-memory kvSeam for unit-testing kvStore with no
// running server. It models JetStream KV's create-if-absent / revision-CAS-update /
// delete-marker semantics closely enough to drive kvStore's full contract: a per-bucket
// monotonic revision (so revisions are globally strictly increasing, as JetStream's
// stream sequence is), errKVKeyExists on create-over-a-live-key, re-create over a delete
// marker, wrong-revision conflicts, and delete tombstones excluded from get/keys. It is
// mutex-guarded for safety even though kvStore itself spawns no goroutines.
type fakeKVStoreSeam struct {
	mu      sync.Mutex
	store   map[string]fakeKVStoreVal
	nextRev uint64

	// forceErr, when non-nil, makes every op return it (a non-sentinel backend fault) so a
	// test can exercise kvStore's fail-closed *KVOpError path.
	forceErr error
	// keysOverride, when non-nil, is returned verbatim by keys() — including duplicates and
	// out-of-order entries a real bucket never yields — to prove kvStore sorts, dedups, and
	// prefix-filters above the seam.
	keysOverride []string
}

type fakeKVStoreVal struct {
	val     []byte
	rev     uint64
	deleted bool
}

func newFakeKVStoreSeam() *fakeKVStoreSeam {
	return &fakeKVStoreSeam{store: make(map[string]fakeKVStoreVal)}
}

func (f *fakeKVStoreSeam) create(_ context.Context, key string, val []byte) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forceErr != nil {
		return 0, f.forceErr
	}
	if e, ok := f.store[key]; ok && !e.deleted {
		return 0, errKVKeyExists
	}
	f.nextRev++
	f.store[key] = fakeKVStoreVal{val: append([]byte(nil), val...), rev: f.nextRev}
	return f.nextRev, nil
}

func (f *fakeKVStoreSeam) get(_ context.Context, key string) ([]byte, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forceErr != nil {
		return nil, 0, f.forceErr
	}
	e, ok := f.store[key]
	if !ok || e.deleted {
		return nil, 0, errKVKeyNotFound
	}
	return e.val, e.rev, nil
}

func (f *fakeKVStoreSeam) update(_ context.Context, key string, val []byte, expectedRev uint64) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forceErr != nil {
		return 0, f.forceErr
	}
	e, ok := f.store[key]
	if !ok || e.deleted {
		return 0, errKVKeyNotFound
	}
	if e.rev != expectedRev {
		return 0, errKVCASConflict
	}
	f.nextRev++
	f.store[key] = fakeKVStoreVal{val: append([]byte(nil), val...), rev: f.nextRev}
	return f.nextRev, nil
}

func (f *fakeKVStoreSeam) del(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forceErr != nil {
		return f.forceErr
	}
	if e, ok := f.store[key]; ok {
		e.deleted = true
		e.val = nil
		f.store[key] = e
	}
	return nil // idempotent even when the key was never present
}

func (f *fakeKVStoreSeam) keys(_ context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forceErr != nil {
		return nil, f.forceErr
	}
	if f.keysOverride != nil {
		return append([]string(nil), f.keysOverride...), nil
	}
	var out []string
	for k, e := range f.store {
		if !e.deleted {
			out = append(out, k)
		}
	}
	return out, nil // deliberately unsorted: kvStore.Keys must sort
}

func TestKVCreateGetAndRev(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const key = "sessions/one"

	s := newKVStore(newFakeKVStoreSeam())
	rev, err := s.Put(ctx, key, 0, []byte("v1"))
	if err != nil {
		t.Fatalf("Put(create): %v", err)
	}
	if rev != 1 {
		t.Errorf("create rev = %d, want 1", rev)
	}

	gotVal, gotRev, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(gotVal, []byte("v1")) || gotRev != 1 {
		t.Errorf("Get = (%q, %d), want (%q, 1)", gotVal, gotRev, "v1")
	}

	last := rev
	for i := 0; i < 3; i++ {
		val := []byte("v" + strconv.Itoa(i+2))
		newRev, uerr := s.Put(ctx, key, last, val)
		if uerr != nil {
			t.Fatalf("Put(update) %d: %v", i, uerr)
		}
		if newRev <= last {
			t.Errorf("update rev = %d, want strictly > %d", newRev, last)
		}
		gv, gr, gerr := s.Get(ctx, key)
		if gerr != nil {
			t.Fatalf("Get: %v", gerr)
		}
		if gr != newRev || !bytes.Equal(gv, val) {
			t.Errorf("Get = (%q, %d), want (%q, %d)", gv, gr, val, newRev)
		}
		last = newRev
	}
}

func TestKVGetAbsentReturnsKeyNotFound(t *testing.T) {
	t.Parallel()
	const key = "sessions/missing"
	s := newKVStore(newFakeKVStoreSeam())
	_, _, err := s.Get(context.Background(), key)
	var nf *storekit.KeyNotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("Get(absent) = %v, want *KeyNotFoundError", err)
	}
	if nf.Key != key {
		t.Errorf("KeyNotFoundError.Key = %q, want %q", nf.Key, key)
	}
}

func TestKVPutConflictLeavesStateUnchanged(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const key = "sessions/cas"
	tests := []struct {
		name        string
		preRevs     int
		expectedRev uint64
	}{
		{name: "create-only on existing key", preRevs: 1, expectedRev: 0},
		{name: "stale rev below current", preRevs: 3, expectedRev: 1},
		{name: "rev above current", preRevs: 1, expectedRev: 5},
		{name: "update on absent key", preRevs: 0, expectedRev: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := newKVStore(newFakeKVStoreSeam())
			var curRev uint64
			for i := 0; i < tt.preRevs; i++ {
				var err error
				curRev, err = s.Put(ctx, key, curRev, []byte("seed"))
				if err != nil {
					t.Fatalf("seed Put %d: %v", i, err)
				}
			}

			_, err := s.Put(ctx, key, tt.expectedRev, []byte("nope"))
			var ce *storekit.ConflictError
			if !errors.As(err, &ce) {
				t.Fatalf("Put(expectedRev=%d) = %v, want *ConflictError", tt.expectedRev, err)
			}
			if ce.Name != key || ce.Expected != tt.expectedRev {
				t.Errorf("ConflictError = {Name:%q, Expected:%d}, want {%q, %d}",
					ce.Name, ce.Expected, key, tt.expectedRev)
			}

			if tt.preRevs == 0 {
				if _, _, gerr := s.Get(ctx, key); !errors.As(gerr, new(*storekit.KeyNotFoundError)) {
					t.Errorf("Get after rejected Put = %v, want key still absent", gerr)
				}
				return
			}
			gotVal, gotRev, gerr := s.Get(ctx, key)
			if gerr != nil {
				t.Fatalf("Get: %v", gerr)
			}
			if gotRev != curRev || !bytes.Equal(gotVal, []byte("seed")) {
				t.Errorf("state after rejected Put = (%q, %d), want (%q, %d) unchanged",
					gotVal, gotRev, "seed", curRev)
			}
		})
	}
}

func TestKVKeysSortedDedupPrefixFiltered(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		override []string
		prefix   string
		want     []string
	}{
		{
			name:     "empty prefix returns all sorted deduped",
			override: []string{"sessions/c", "sessions/a", "sessions/a", "workspaces/z", "sessions/b"},
			prefix:   "",
			want:     []string{"sessions/a", "sessions/b", "sessions/c", "workspaces/z"},
		},
		{
			name:     "prefix filters and sorts",
			override: []string{"sessions/c", "sessions/a", "workspaces/z", "sessions/b"},
			prefix:   "sessions/",
			want:     []string{"sessions/a", "sessions/b", "sessions/c"},
		},
		{
			name:     "partial-segment prefix filters as substring",
			override: []string{"sessions/a", "sessions/b", "session-x"},
			prefix:   "sessions/",
			want:     []string{"sessions/a", "sessions/b"},
		},
		{
			name:     "prefix matching nothing is empty",
			override: []string{"sessions/a"},
			prefix:   "workspaces/",
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
			f := newFakeKVStoreSeam()
			f.keysOverride = tt.override
			s := newKVStore(f)
			got, err := s.Keys(context.Background(), tt.prefix)
			if err != nil {
				t.Fatalf("Keys: %v", err)
			}
			if !equalStrings(got, tt.want) {
				t.Errorf("Keys(%q) = %v, want %v", tt.prefix, got, tt.want)
			}
		})
	}
}

func TestKVDeleteIdempotentAndFreesKey(t *testing.T) {
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
			const key = "sessions/del"
			s := newKVStore(newFakeKVStoreSeam())
			if tt.seed {
				if _, err := s.Put(ctx, key, 0, []byte("v")); err != nil {
					t.Fatalf("seed Put: %v", err)
				}
			}
			for i := 0; i < tt.deletes; i++ {
				if err := s.Delete(ctx, key); err != nil {
					t.Fatalf("Delete call %d = %v, want nil (idempotent)", i, err)
				}
			}
			if _, _, err := s.Get(ctx, key); !errors.As(err, new(*storekit.KeyNotFoundError)) {
				t.Errorf("Get after delete = %v, want *KeyNotFoundError", err)
			}
			// A deleted key is truly free: a fresh create-only Put succeeds.
			if _, err := s.Put(ctx, key, 0, []byte("fresh")); err != nil {
				t.Errorf("create-only Put after delete = %v, want nil (key free)", err)
			}
		})
	}
}

func TestKVInvalidKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	methods := []struct {
		method string
		call   func(s *kvStore, key string) error
	}{
		{"Get", func(s *kvStore, key string) error { _, _, err := s.Get(ctx, key); return err }},
		{"Put", func(s *kvStore, key string) error { _, err := s.Put(ctx, key, 0, []byte("x")); return err }},
		{"Delete", func(s *kvStore, key string) error { return s.Delete(ctx, key) }},
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
				f := newFakeKVStoreSeam()
				s := newKVStore(f)
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
					t.Errorf("invalid key touched the backend: %d entries, want 0", n)
				}
			})
		}
	}
}

func TestKVBackendFaultFailsClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	boom := errors.New("kv backend unavailable")
	ops := []struct {
		name string
		call func(s *kvStore) error
	}{
		{"Get", func(s *kvStore) error { _, _, err := s.Get(ctx, "sessions/x"); return err }},
		{"Put create", func(s *kvStore) error { _, err := s.Put(ctx, "sessions/x", 0, []byte("v")); return err }},
		{"Put update", func(s *kvStore) error { _, err := s.Put(ctx, "sessions/x", 3, []byte("v")); return err }},
		{"Delete", func(s *kvStore) error { return s.Delete(ctx, "sessions/x") }},
		{"Keys", func(s *kvStore) error { _, err := s.Keys(ctx, ""); return err }},
	}
	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			t.Parallel()
			f := newFakeKVStoreSeam()
			f.forceErr = boom
			s := newKVStore(f)
			err := op.call(s)
			var ke *KVOpError
			if !errors.As(err, &ke) {
				t.Fatalf("%s = %v, want *KVOpError (fail closed)", op.name, err)
			}
			if !errors.Is(err, boom) {
				t.Error("KVOpError does not unwrap to the backend cause")
			}
			// A backend fault must never be mis-typed as a storekit CAS/absence outcome.
			if errors.As(err, new(*storekit.ConflictError)) || errors.As(err, new(*storekit.KeyNotFoundError)) {
				t.Error("backend fault mis-classified as a storekit KV outcome")
			}
		})
	}
}

// equalStrings reports whether a and b hold the same elements in the same order, treating
// nil and empty as equal (mirrors storetest.equalStringSlices for in-package unit tests).
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
