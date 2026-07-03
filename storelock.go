package natsstore

import "errors"

const (
	// storeLockFileName is the per-directory advisory lock file. Holding it marks the
	// StoreDir as in use by one live engine (this or another process).
	storeLockFileName = "store.lock"
	// storeLockFilePerm keeps the lock file owner-only.
	storeLockFilePerm = 0o600
)

// errStoreLockUnsupported is returned by acquireStoreLock on platforms without the
// build-tagged flock implementation. The engine never opens unguarded.
var errStoreLockUnsupported = errors.New("natsstore: store-directory file locking is unsupported on this platform")

// StoreLockedError reports that a StoreDir is already locked by a live engine (in this or
// another process). The directory is in use; the caller must not open a second engine
// over it.
type StoreLockedError struct {
	Path string
}

func (e *StoreLockedError) Error() string {
	if e.Path != "" {
		return "natsstore: store directory already locked: " + e.Path
	}
	return "natsstore: store directory already locked"
}

// StoreLockError reports that the store lock file could not be opened or that the flock
// syscall failed for a reason other than contention. It fails closed: without the lock the
// engine never opens.
type StoreLockError struct {
	Path  string
	Cause error
}

func (e *StoreLockError) Error() string {
	msg := "natsstore: store lock"
	if e.Path != "" {
		msg += " " + e.Path
	}
	if e.Cause != nil {
		return msg + ": " + e.Cause.Error()
	}
	return msg
}
func (e *StoreLockError) Unwrap() error { return e.Cause }

// storeLock is a process-exclusive advisory lock on a StoreDir. The Unix build backs it
// with flock; unsupported platforms fail closed at acquisition time.
type storeLock interface {
	Unlock() error
}
