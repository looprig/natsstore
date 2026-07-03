package natsstore

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/ciram-co/storekit"
)

// kvSeam is the narrow set of JetStream KV operations kvStore drives, isolated behind
// an interface (DIP + ISP) so the store's revision-CAS logic can be unit-tested with a
// stateful fake and no running server. The production binding (jetStreamKVStoreSeam,
// seam.go) wraps a real, context-aware jetstream.KeyValue bucket and maps NATS' CAS
// errors onto the three package sentinels (errKVKeyExists / errKVKeyNotFound /
// errKVCASConflict, defined in lease.go) so the logic above it stays free of any nats.go
// dependency. It is the ONLY kv-store code that talks to NATS directly.
//
// It intentionally does NOT embed kvLeaseSeam (the leaser's three-op seam): though the
// create/get/update signatures coincide, keeping the interfaces separate means a change
// to the leaser's seam can never ripple into the KV store's contract (ISP). The
// production bindings DO share the concrete translation (jetStreamKVStoreSeam embeds
// jetStreamKVSeam) so the mapping is written once.
type kvSeam interface {
	// create writes val at key only if absent, returning the new revision. A key that
	// already holds a live value yields errKVKeyExists; a key whose last op is a delete
	// marker is re-created (success), matching JetStream KV's Create semantics.
	create(ctx context.Context, key string, val []byte) (rev uint64, err error)
	// get returns the stored value and its current revision at key, or errKVKeyNotFound
	// if absent or deleted. Any other error is a genuine backend fault.
	get(ctx context.Context, key string) (val []byte, rev uint64, err error)
	// update writes val at key only if its current revision equals expectedRev, returning
	// the new revision. A revision mismatch yields errKVCASConflict; an absent/deleted key
	// yields errKVKeyNotFound.
	update(ctx context.Context, key string, val []byte, expectedRev uint64) (rev uint64, err error)
	// del removes key. It is idempotent: deleting an absent key is a success.
	del(ctx context.Context, key string) error
	// keys returns every live key in the bucket (deleted keys excluded), in no particular
	// order and without a prefix filter; an empty bucket yields an empty slice, never an
	// error. The store sorts, dedups, and prefix-filters above it.
	keys(ctx context.Context) (keys []string, err error)
}

// KVOpError reports a definite failure of a KV operation kvStore performs (get / create
// / update / delete / keys) that is NOT one of the expected CAS outcomes — a backend
// fault or an ambiguous read. It fails closed and names the key (or, for keys, the
// prefix), the operation, and unwraps to the underlying cause. (The storekit taxonomy's
// only KV errors are KeyNotFoundError and ConflictError — neither fits a backend fault —
// so this is a natsstore-specific typed error, analogous to StreamOpError / LeaseOpError.)
type KVOpError struct {
	Key   string
	Op    string
	Cause error
}

func (e *KVOpError) Error() string {
	return "natsstore: kv " + strconv.Quote(e.Key) + " " + e.Op + " failed: " + e.Cause.Error()
}
func (e *KVOpError) Unwrap() error { return e.Cause }

// kvStore implements storekit.KV over one JetStream KV bucket, one entry per key. It
// holds only the seam (DIP) and carries no mutable state, so it is as safe for concurrent
// use as the seam beneath it. Keys map DIRECTLY to JetStream KV keys (see kvKeyForName)
// and revisions map directly to JetStream KV revisions.
type kvStore struct {
	seam kvSeam
}

var _ storekit.KV = (*kvStore)(nil)

// newKVStore builds a KV store over seam.
func newKVStore(seam kvSeam) *kvStore {
	return &kvStore{seam: seam}
}

// Get returns the value and revision at key, or *storekit.KeyNotFoundError if absent.
// A backend fault is a typed *KVOpError (fail closed) — never conflated with absence.
func (s *kvStore) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	k, err := kvKeyForName(key)
	if err != nil {
		return nil, 0, err // *storekit.InvalidNameError, verbatim
	}
	val, rev, err := s.seam.get(ctx, k)
	if err != nil {
		if errors.Is(err, errKVKeyNotFound) {
			return nil, 0, &storekit.KeyNotFoundError{Key: key}
		}
		return nil, 0, &KVOpError{Key: key, Op: "get", Cause: err}
	}
	return val, rev, nil
}

// Put writes val at key under a revision fence and returns the new revision. expectedRev
// 0 is a create (the key must be absent); a losing create — the key already holds a live
// value — is a *storekit.ConflictError{Expected:0}. A non-zero expectedRev is a
// revision-CAS update; a mismatch (including an update against an absent/deleted key,
// which JetStream also rejects as a wrong-last-sequence) is a
// *storekit.ConflictError{Expected:expectedRev}, leaving the stored value unchanged.
func (s *kvStore) Put(ctx context.Context, key string, expectedRev uint64, val []byte) (uint64, error) {
	k, err := kvKeyForName(key)
	if err != nil {
		return 0, err // *storekit.InvalidNameError, verbatim
	}
	if expectedRev == 0 {
		return s.create(ctx, key, k, val)
	}
	return s.update(ctx, key, k, expectedRev, val)
}

// create maps a create-if-absent onto the storekit contract: success → new revision,
// a losing create → *ConflictError{Expected:0}, any other fault → *KVOpError.
func (s *kvStore) create(ctx context.Context, key, k string, val []byte) (uint64, error) {
	rev, err := s.seam.create(ctx, k, val)
	if err != nil {
		if errors.Is(err, errKVKeyExists) {
			return 0, &storekit.ConflictError{Name: key, Expected: 0}
		}
		return 0, &KVOpError{Key: key, Op: "create", Cause: err}
	}
	return rev, nil
}

// update maps a revision-CAS update onto the storekit contract: success → new revision,
// a CAS conflict OR an absent/deleted key → *ConflictError{Expected:expectedRev} (both
// mean "the revision you fenced on is not the current one"), any other fault → *KVOpError.
func (s *kvStore) update(ctx context.Context, key, k string, expectedRev uint64, val []byte) (uint64, error) {
	rev, err := s.seam.update(ctx, k, val, expectedRev)
	if err != nil {
		if errors.Is(err, errKVCASConflict) || errors.Is(err, errKVKeyNotFound) {
			return 0, &storekit.ConflictError{Name: key, Expected: expectedRev}
		}
		return 0, &KVOpError{Key: key, Op: "update", Cause: err}
	}
	return rev, nil
}

// Keys returns the live keys that begin with prefix, lexicographically ascending and
// duplicate-free. The prefix is a literal string filter over the direct key mapping (no
// validation — a caller may list by any prefix, including a partial segment). Sorting and
// dedup happen locally, so the backend need not return an ordered set.
func (s *kvStore) Keys(ctx context.Context, prefix string) ([]string, error) {
	all, err := s.seam.keys(ctx)
	if err != nil {
		return nil, &KVOpError{Key: prefix, Op: "keys", Cause: err}
	}
	return sortedDedupFiltered(all, prefix), nil
}

// Delete removes key. It is idempotent: deleting an absent key is a success, so a deleted
// key is indistinguishable from a never-written one.
func (s *kvStore) Delete(ctx context.Context, key string) error {
	k, err := kvKeyForName(key)
	if err != nil {
		return err // *storekit.InvalidNameError, verbatim
	}
	if err := s.seam.del(ctx, k); err != nil {
		return &KVOpError{Key: key, Op: "delete", Cause: err}
	}
	return nil
}

// kvKeyForName validates a storekit key and returns it UNCHANGED as the JetStream KV key.
// The storekit grammar ([a-z0-9][a-z0-9_.-]* segments joined by '/') is a strict subset
// of JetStream KV's allowed key charset [-/_=.a-zA-Z0-9], so the mapping is the identity:
// no two valid keys alias one entry, Keys round-trips them verbatim, and Keys(prefix) is a
// literal substring filter. It returns the *storekit.InvalidNameError verbatim for an
// invalid key, so every entry point validates in one call.
func kvKeyForName(key string) (string, error) {
	if err := storekit.ValidateName(key); err != nil {
		return "", err
	}
	return key, nil
}

// sortedDedupFiltered returns the elements of all that begin with prefix, sorted
// ascending with duplicates removed. It returns nil (not an empty non-nil slice) for no
// matches, which the storekit listing contract treats as equal to empty. Shared by
// KV.Keys and Blobs.List, whose listing contracts are identical.
func sortedDedupFiltered(all []string, prefix string) []string {
	var out []string
	seen := make(map[string]struct{}, len(all))
	for _, k := range all {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
