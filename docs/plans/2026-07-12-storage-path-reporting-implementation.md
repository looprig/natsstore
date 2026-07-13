# NATS Store Storage-Path Reporting Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make embedded natsstore providers report their frozen canonical JetStream persistence directory through `storage.PathReporter`, while remote providers report none.

**Architecture:** Add one private immutable reporter value that clones paths on construction and return. Embed it in `Store` and all four concrete primitive adapters, and inject the same value while composing an embedded or remote store. Canonicalize the embedded directory only after the engine has created it, closing the engine on any resolution failure.

**Tech Stack:** Go 1.25, `github.com/looprig/storage` v0.2.0, NATS JetStream, standard-library `path/filepath`, Go race detector.

---

### Task 1: Add the private path-reporting contract

**Files:**
- Create: `paths.go`
- Create: `paths_test.go`
- Modify: `ledger.go`
- Modify: `lease.go`
- Modify: `kv.go`
- Modify: `blobs.go`
- Modify: `natsstore.go`

**Step 1: Write the failing test**

Create table-driven tests for a zero reporter and a reporter initialized with one path. Assert `StoragePaths` returns the expected values and that mutating either the constructor input or a returned slice cannot alter later results. Add compile-time assertions for the outer store and all primitive types:

```go
var (
	_ storage.PathReporter = (*Store)(nil)
	_ storage.PathReporter = (*ledgerStore)(nil)
	_ storage.PathReporter = (*leaserStore)(nil)
	_ storage.PathReporter = (*kvStore)(nil)
	_ storage.PathReporter = (*blobStore)(nil)
)
```

**Step 2: Run the test to verify it fails**

Run: `GOWORK=off GOCACHE=/private/tmp/natsstore-gocache go test -race ./... -run TestLocalPathReporter -count=1`

Expected: compilation fails because `localPathReporter`, `newLocalPathReporter`, and the primitive `StoragePaths` methods do not exist.

**Step 3: Write the minimal implementation**

Implement a private value that owns and returns cloned slices:

```go
type localPathReporter struct {
	paths []string
}

func newLocalPathReporter(paths ...string) localPathReporter {
	return localPathReporter{paths: slices.Clone(paths)}
}

func (r localPathReporter) StoragePaths() []string {
	return slices.Clone(r.paths)
}
```

Embed `localPathReporter` in `Store`, `ledgerStore`, `leaserStore`, `kvStore`, and `blobStore`. Existing primitive constructors leave it at the zero value so unit-only adapters correctly report no local persistence path.

**Step 4: Run the test to verify it passes**

Run: `GOWORK=off GOCACHE=/private/tmp/natsstore-gocache go test -race ./... -run TestLocalPathReporter -count=1`

Expected: PASS.

**Step 5: Run the full unit suite**

Run: `GOWORK=off GOCACHE=/private/tmp/natsstore-gocache go test -race ./...`

Expected: PASS.

### Task 2: Wire canonical embedded paths into every provider

**Files:**
- Modify: `natsstore.go`
- Modify: `natsstore_integration_test.go`

**Step 1: Write the failing integration test**

Add a table-driven integration test that creates a real directory, creates a symlink to its parent, and opens `Options{EmbeddedDir: filepath.Join(link, "jetstream")}`. The expected path is `filepath.Join(realParent, "jetstream")`. Assert the outer store and every provider exposed through `Store.Composite` implements `storage.PathReporter` and returns exactly that canonical path. Mutate one result and assert all later reports remain unchanged.

**Step 2: Run the test to verify it fails**

Run: `GOWORK=off GOCACHE=/private/tmp/natsstore-gocache go test -tags integration -race ./... -run TestOpenEmbeddedReportsCanonicalStoragePath -count=1`

Expected: FAIL because `Open` has not injected a path and each report is empty.

**Step 3: Write the minimal implementation**

Add a helper that canonicalizes an existing path:

```go
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
```

After `openEngineAt` succeeds, resolve its StoreDir, close the engine on resolution error, and create `newLocalPathReporter(canonical)`. Change `buildComposite` to accept a reporter, install it on all four concrete stores before `storage.NewComposite`, and pass the zero reporter from remote mode. Store the reporter on the outer `Store` too.

**Step 4: Run the focused integration test to verify it passes**

Run: `GOWORK=off GOCACHE=/private/tmp/natsstore-gocache go test -tags integration -race ./... -run TestOpenEmbeddedReportsCanonicalStoragePath -count=1`

Expected: PASS.

**Step 5: Verify remote semantics without a broker**

Add a unit test over a `Store{localPathReporter: newLocalPathReporter()}` asserting `StoragePaths` is empty. This represents the exact reporter passed by `openRemote`; remote URLs are never inspected for server-side paths.

Run: `GOWORK=off GOCACHE=/private/tmp/natsstore-gocache go test -race ./... -run TestStoreStoragePaths -count=1`

Expected: PASS after first observing a failure if the test introduces any new behavior.

**Step 6: Commit behavior**

```bash
git add paths.go paths_test.go ledger.go lease.go kv.go blobs.go natsstore.go natsstore_integration_test.go
git commit -m "feat: report embedded storage paths"
```

### Task 3: Update the storage dependency and verify release gates

**Files:**
- Modify: `go.mod`
- Modify if required by module tooling: `go.sum`

**Step 1: Update the dependency**

Change `github.com/looprig/storage v0.1.0` to `v0.2.0`. Keep `replace github.com/looprig/storage => ../storage` for coordinated local development.

**Step 2: Run formatting and static checks**

Run: `gofmt -w paths.go paths_test.go ledger.go lease.go kv.go blobs.go natsstore.go natsstore_integration_test.go`

Run: `GOCACHE=/private/tmp/natsstore-gocache make check`

Expected: formatting, vet, and unit race tests all PASS.

**Step 3: Run integration race tests**

Run: `GOWORK=off GOCACHE=/private/tmp/natsstore-gocache go test -tags integration -race ./...`

Expected: PASS.

**Step 4: Review requirements and repository diff**

Run: `git diff --check HEAD~1` and `git status --short`.

Confirm embedded canonicalization follows engine creation, every provider receives the same frozen reporter, remote mode receives the zero reporter, returned slices are defensive clones, errors remain typed, and no public API beyond the optional interface implementation was added.

**Step 5: Commit the dependency update**

```bash
git add go.mod go.sum
git commit -m "chore: require storage v0.2.0"
```

**Step 6: Run fresh final gates**

Run: `GOCACHE=/private/tmp/natsstore-gocache make check`

Run: `GOWORK=off GOCACHE=/private/tmp/natsstore-gocache go test -tags integration -race ./...`

Expected: both commands exit 0 with no failures.
