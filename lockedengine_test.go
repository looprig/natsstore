package natsstore

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
)

// fakeEngine is a stand-in engineHandle so the LockedEngine lifecycle (especially Close
// ordering) can be exercised without starting an embedded NATS server.
type fakeEngine struct {
	closeErr error
	closed   bool
}

func (f *fakeEngine) JetStream() nats.JetStreamContext { return nil }

func (f *fakeEngine) Close() error {
	f.closed = true
	return f.closeErr
}

// fakeLock records release so tests can assert the lock is always freed.
type fakeLock struct {
	unlockErr error
	unlocked  bool
}

func (f *fakeLock) Unlock() error {
	f.unlocked = true
	return f.unlockErr
}

func swapAcquireLock(t *testing.T, fn func(string) (storeLock, error)) {
	t.Helper()
	prev := acquireLock
	acquireLock = fn
	t.Cleanup(func() { acquireLock = prev })
}

func swapOpenEngine(t *testing.T, fn func(EngineOptions) (engineHandle, error)) {
	t.Helper()
	prev := openEngine
	openEngine = fn
	t.Cleanup(func() { openEngine = prev })
}

// TestLockedEngineLockedBeforeStartup proves a second open of a locked StoreDir returns
// *StoreLockedError before any engine (server) construction is attempted.
func TestLockedEngineLockedBeforeStartup(t *testing.T) {
	dir := t.TempDir()
	swapOpenEngine(t, func(EngineOptions) (engineHandle, error) {
		t.Fatalf("openEngine called despite a held store lock")
		return nil, nil
	})

	held, err := acquireLock(dir)
	if err != nil {
		t.Fatalf("acquireLock(%q): %v", dir, err)
	}
	t.Cleanup(func() { _ = held.Unlock() })

	_, err = OpenLockedEngine(dir)
	var locked *StoreLockedError
	if !errors.As(err, &locked) {
		t.Fatalf("OpenLockedEngine() error = %T %v, want *StoreLockedError", err, err)
	}
}

// TestStoreLockReleaseAllowsReacquire proves releasing a lock lets it be retaken — the
// property a reopen after a clean close depends on.
func TestStoreLockReleaseAllowsReacquire(t *testing.T) {
	dir := t.TempDir()

	first, err := acquireLock(dir)
	if err != nil {
		t.Fatalf("first acquireLock: %v", err)
	}
	if err := first.Unlock(); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	second, err := acquireLock(dir)
	if err != nil {
		t.Fatalf("second acquireLock after release: %v", err)
	}
	if err := second.Unlock(); err != nil {
		t.Fatalf("second Unlock: %v", err)
	}
}

// TestLockedEngineCloseReleasesLockOnDrainError proves Close frees the lock even when the
// engine drain fails, and surfaces the drain error.
func TestLockedEngineCloseReleasesLockOnDrainError(t *testing.T) {
	drainErr := errors.New("drain failed")
	lock := &fakeLock{}
	engine := &fakeEngine{closeErr: drainErr}
	le := &LockedEngine{engine: engine, lock: lock}

	err := le.Close()
	if !errors.Is(err, drainErr) {
		t.Fatalf("Close() = %v, want drain error %v", err, drainErr)
	}
	if !engine.closed {
		t.Error("engine.Close was not called")
	}
	if !lock.unlocked {
		t.Error("lock was not released after a drain error")
	}
}

// TestLockedEngineUnsupportedPlatform proves an unsupported locking platform surfaces a
// typed error rather than opening an unguarded engine.
func TestLockedEngineUnsupportedPlatform(t *testing.T) {
	swapAcquireLock(t, func(string) (storeLock, error) {
		return nil, errStoreLockUnsupported
	})
	swapOpenEngine(t, func(EngineOptions) (engineHandle, error) {
		t.Fatalf("openEngine called on an unsupported locking platform")
		return nil, nil
	})

	if _, err := OpenLockedEngine(t.TempDir()); !errors.Is(err, errStoreLockUnsupported) {
		t.Fatalf("OpenLockedEngine() error = %v, want errStoreLockUnsupported", err)
	}
}

// TestLockedEngineTwoDistinctDirsLockIndependently proves two different StoreDirs lock
// without contending with each other.
func TestLockedEngineTwoDistinctDirsLockIndependently(t *testing.T) {
	a, err := acquireLock(t.TempDir())
	if err != nil {
		t.Fatalf("acquireLock(a): %v", err)
	}
	t.Cleanup(func() { _ = a.Unlock() })

	b, err := acquireLock(t.TempDir())
	if err != nil {
		t.Fatalf("acquireLock(b): %v", err)
	}
	t.Cleanup(func() { _ = b.Unlock() })
}

// TestLockedEngineCloseReleasesLockOnSuccess proves the ordinary close path frees the lock
// and reports no error.
func TestLockedEngineCloseReleasesLockOnSuccess(t *testing.T) {
	lock := &fakeLock{}
	engine := &fakeEngine{}
	le := &LockedEngine{engine: engine, lock: lock}

	if err := le.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
	if !lock.unlocked {
		t.Error("lock was not released on a clean close")
	}
}
