package natsstore

import (
	"path/filepath"
	"slices"
)

// localPathReporter is the immutable local persistence-path capability shared by a
// Store and its concrete providers. Its zero value reports no local paths.
type localPathReporter struct {
	paths []string
}

func newLocalPathReporter(paths ...string) localPathReporter {
	return localPathReporter{paths: slices.Clone(paths)}
}

// StoragePaths returns a defensive copy of the provider's frozen local persistence
// roots. Remote providers use the zero value and therefore return nil.
func (r localPathReporter) StoragePaths() []string {
	return slices.Clone(r.paths)
}

// canonicalStoragePath resolves an existing persistence root to the physical path used
// by the filesystem. Embedded startup creates the directory before this is called, so
// EvalSymlinks can resolve the complete path rather than only an existing ancestor.
func canonicalStoragePath(path string) (string, error) {
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", &StoreDirError{Path: path, Cause: err}
	}
	canonical, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", &StoreDirError{Path: path, Cause: err}
	}
	return filepath.Clean(canonical), nil
}
