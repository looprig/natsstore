package natsstore

import (
	"path/filepath"

	"github.com/nats-io/nats.go"
)

// natsSubDir is the subdirectory under a locked StoreDir that holds the embedded
// JetStream state, kept separate from the store.lock file.
const natsSubDir = "nats"

// engineHandle is the narrow subset of *Engine the locked-engine lifecycle depends on, so
// Close ordering can be unit-tested without starting an embedded server.
type engineHandle interface {
	JetStream() nats.JetStreamContext
	Close() error
}

// acquireLock is the lock-acquisition seam. Production points it at the platform flock
// implementation; tests swap it to exercise contention and unsupported-platform paths.
var acquireLock = acquireStoreLock

// openEngine is the engine-construction seam. Production opens a real embedded engine;
// tests swap it to avoid starting NATS.
var openEngine = func(opts EngineOptions) (engineHandle, error) { return Open(opts) }

// LockedEngine is one embedded engine bound to a single StoreDir and guarded by a
// process-exclusive lock. It is the per-directory unit a consumer opens and closes; the
// Engine itself stays directory-agnostic.
type LockedEngine struct {
	engine engineHandle
	lock   storeLock
}

// OpenLockedEngine takes the exclusive lock on dir, then opens an embedded engine whose
// StoreDir lives beneath dir (<dir>/nats). A directory already open by a live engine
// returns *StoreLockedError before any server starts. dir must already exist (it holds the
// lock file); the engine's StoreDir subdirectory is created on demand.
func OpenLockedEngine(dir string) (*LockedEngine, error) {
	lock, err := acquireLock(dir)
	if err != nil {
		return nil, err
	}

	engine, err := openEngine(EngineOptions{DataDir: filepath.Join(dir, natsSubDir)})
	if err != nil {
		_ = lock.Unlock()
		return nil, err
	}
	return &LockedEngine{engine: engine, lock: lock}, nil
}

// JetStream returns the locked engine's bound JetStreamContext, valid until Close.
func (e *LockedEngine) JetStream() nats.JetStreamContext {
	if e == nil || e.engine == nil {
		return nil
	}
	return e.engine.JetStream()
}

// Close shuts the embedded engine down and always releases the store lock afterwards, even
// when the engine drain fails. The drain error takes precedence in the return.
func (e *LockedEngine) Close() error {
	if e == nil {
		return nil
	}
	var engineErr error
	if e.engine != nil {
		engineErr = e.engine.Close()
		e.engine = nil
	}
	var lockErr error
	if e.lock != nil {
		lockErr = e.lock.Unlock()
		e.lock = nil
	}
	if engineErr != nil {
		return engineErr
	}
	return lockErr
}
