//go:build integration

package natsstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nats-io/nats.go"
)

// lockedEngineDir creates a StoreDir under a temp containment root (XDG_DATA_HOME), so a
// real embedded engine opened beneath it passes the fail-secure confinement check.
func lockedEngineDir(t *testing.T, name string) string {
	t.Helper()
	root := os.Getenv("XDG_DATA_HOME")
	if root == "" {
		root = t.TempDir()
		t.Setenv("XDG_DATA_HOME", root)
	}
	dir := filepath.Join(root, "looprig", "stores", name)
	// #nosec G301 -- test-only StoreDir under a temp root.
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("MkdirAll(%q): %v", dir, err)
	}
	return dir
}

// TestLockedEngineLifecycle opens a real embedded engine over one StoreDir, proves a
// second open of the same directory is rejected as locked, and that a clean close releases
// the lock so the directory can be reopened with its persisted stream intact.
func TestLockedEngineLifecycle(t *testing.T) {
	dir := lockedEngineDir(t, "s1")

	first, err := OpenLockedEngine(dir)
	if err != nil {
		t.Fatalf("OpenLockedEngine: %v", err)
	}
	js := first.JetStream()
	if js == nil {
		t.Fatal("JetStream() returned nil")
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "S", Subjects: []string{"s.>"}}); err != nil {
		t.Fatalf("AddStream: %v", err)
	}

	// A second open of the same live directory must fail closed, before any server starts.
	if _, err := OpenLockedEngine(dir); err == nil {
		t.Fatal("second OpenLockedEngine succeeded, want *StoreLockedError")
	} else {
		var locked *StoreLockedError
		if !errors.As(err, &locked) {
			t.Fatalf("second open error = %T %v, want *StoreLockedError", err, err)
		}
	}

	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// After a clean close the lock is released and the directory reopens; its StoreDir is
	// durable, so the stream created above survives.
	reopened, err := OpenLockedEngine(dir)
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := reopened.JetStream().StreamInfo("S"); err != nil {
		t.Fatalf("StreamInfo after reopen: %v", err)
	}
}

// TestLockedEngineDistinctDirsCoexist proves two different StoreDirs run simultaneously
// with isolated state.
func TestLockedEngineDistinctDirsCoexist(t *testing.T) {
	a, err := OpenLockedEngine(lockedEngineDir(t, "a"))
	if err != nil {
		t.Fatalf("OpenLockedEngine(a): %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	b, err := OpenLockedEngine(lockedEngineDir(t, "b"))
	if err != nil {
		t.Fatalf("OpenLockedEngine(b): %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	if _, err := a.JetStream().AddStream(&nats.StreamConfig{Name: "A", Subjects: []string{"a.>"}}); err != nil {
		t.Fatalf("AddStream(a): %v", err)
	}
	if _, err := b.JetStream().AddStream(&nats.StreamConfig{Name: "B", Subjects: []string{"b.>"}}); err != nil {
		t.Fatalf("AddStream(b): %v", err)
	}

	// Each engine sees only its own stream — the StoreDirs are isolated.
	if _, err := a.JetStream().StreamInfo("B"); err == nil {
		t.Error("engine a can see engine b's stream; StoreDirs are not isolated")
	}
}
