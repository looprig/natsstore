# CLAUDE.md — natsstore

`natsstore` implements [`storage`](../storage)'s storage primitives (`Ledger`, `Leaser`,
`KV`, `Blobs`) over **NATS JetStream**, and owns an **embedded, in-process JetStream
server** (no TCP socket) over a persistent on-disk StoreDir. This is the ONLY module in the
tree where the NATS dependencies are sanctioned — they moved here from looprig when the
embedded JetStream backend was extracted.

## Dependencies

Sanctioned in this module (and nowhere else in the tree):

- `github.com/nats-io/nats.go` — JetStream client (the `nats.JetStreamContext` the adapters
  write through).
- `github.com/nats-io/nats-server/v2` — the embedded in-process JetStream server (the
  `DontListen` engine; no TCP). Pulls in `nats-io/jwt`, `nats-io/nkeys`, `nats-io/nuid` as
  indirect deps.
- `github.com/looprig/storage` — the storage contracts this module implements (local
  `replace => ../storage`).
- Otherwise **stdlib only**. Any further third-party dependency requires explicit approval;
  reach for the standard library first.

## Code rules (same discipline as the consuming repos)

- **Strict typing.** No `any`/`interface{}` except at explicit serialization boundaries,
  narrowed immediately. Named types over bare primitives when the value carries domain
  meaning. No untyped magic numbers/strings.
- **All errors are typed.** Every distinct failure mode is a concrete struct with an
  `Error()` method (and `Unwrap()` when it carries a cause). Never return
  `errors.New(...)`/`fmt.Errorf(...)` from a package-level API. Sentinel errors are
  permitted only for leaf causes with no context fields. Callers classify with `errors.As`.
- **Contracts first.** Write the interface, then the implementation. Keep interfaces small
  and segregated.
- Return errors explicitly; never swallow with `_`.
- Functions over ~30 lines invite an SRP check before growing further.

## Security

- **Fail secure.** The embedded engine never starts on an unconfined or unwritable
  StoreDir: the path is `filepath.Clean`ed and confined within the containment root before
  the server starts (a traversal-crafted DataDir is rejected with a typed error).
- StoreDir is owner-only (`0700`); the durable journal may hold conversation content.
- The store lock fails closed: a platform without `flock` refuses to open rather than run
  two engines over one StoreDir.
- Every I/O method takes a `context.Context`; callers set deadlines. No unbounded blocking.

## Testing

- **Table-driven tests, mandatory**, each with `t.Parallel()`. Cover happy path, boundary
  values (zero/empty/max), error cases, and domain edge cases.
- **Unit vs integration split.** Unit tests run with no server and no network. Anything that
  needs a running embedded server is tagged `//go:build integration` and lives in a
  `*_integration_test.go` file (the embedded server needs no network — it is in-process).
- **Always `-race`:**
  - Unit: `GOWORK=off go test -race ./...`
  - Integration: `GOWORK=off go test -tags integration -race ./...`
- `gofmt`-clean and `go vet`-clean at all times.

## Build & workspace

- **Every Go command runs with `GOWORK=off`** — there is a `go.work` at `~/code` that must
  not capture this module.
- `make check` (fmt-check + vet + `-race` test) before every commit.
