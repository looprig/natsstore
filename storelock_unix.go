//go:build unix

package natsstore

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

// flockStoreLock holds an exclusive flock on an open store.lock descriptor. The lock is
// associated with the open file description, so a second open of the same file — even in
// this process — is denied, which is exactly the single-engine-per-StoreDir guarantee.
type flockStoreLock struct {
	file *os.File
}

// acquireStoreLock takes a non-blocking exclusive flock on <dir>/store.lock. A lock
// already held elsewhere returns *StoreLockedError; any other failure returns a typed
// *StoreLockError.
func acquireStoreLock(dir string) (storeLock, error) {
	path := filepath.Join(dir, storeLockFileName)
	// #nosec G304 -- path is the confined lock file under a validated store directory.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, storeLockFilePerm)
	if err != nil {
		return nil, &StoreLockError{Path: path, Cause: err}
	}

	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, &StoreLockedError{Path: path}
		}
		return nil, &StoreLockError{Path: path, Cause: err}
	}
	return &flockStoreLock{file: file}, nil
}

// Unlock releases the flock and closes the descriptor. It is idempotent.
func (l *flockStoreLock) Unlock() error {
	if l == nil || l.file == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}
