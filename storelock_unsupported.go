//go:build !unix

package natsstore

// acquireStoreLock fails closed on platforms without flock support: rather than open a
// StoreDir without an exclusive guard, it returns errStoreLockUnsupported so the caller
// never runs two engines over one StoreDir.
func acquireStoreLock(dir string) (storeLock, error) {
	return nil, errStoreLockUnsupported
}
