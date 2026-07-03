//go:build integration

package natsstore

import (
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ciram-co/storekit"
	"github.com/ciram-co/storekit/storetest"
)

// TestLedgerConformance runs the full storekit ledger conformance suite against the
// JetStream-backed ledger over an embedded, in-process engine (no network). Every
// factory call stands up a FRESH engine on its own StoreDir (a unique subdirectory
// under the confined test root), so each subtest's backend is fully isolated and the
// ledger sees the caller's real, unmangled names — the suite asserts on
// ConflictError.Name/InvalidNameError.Name, so name rewriting is not an option. The
// suite reuses a logical name (e.g. "sessions/cas") across a subtest's inner cases;
// a fresh engine per case keeps those from colliding on one stream.
func TestLedgerConformance(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", root) // confinement root for every per-backend StoreDir
	var counter atomic.Uint64

	storetest.TestLedger(t, func(t *testing.T) storekit.Ledger {
		n := counter.Add(1)
		dir := filepath.Join(root, "e"+strconv.FormatUint(n, 10), "jetstream")
		eng, err := Open(EngineOptions{DataDir: dir, SyncInterval: 50 * time.Millisecond})
		if err != nil {
			t.Fatalf("Open engine: %v", err)
		}
		t.Cleanup(func() { _ = eng.Close() })
		return newLedgerStore(newJetStreamSeam(eng.JetStream()))
	})
}
