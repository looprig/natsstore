# Contributing to looprig/natsstore

Thanks for considering a contribution. `natsstore` implements
[`storage`](../storage)'s storage primitives (`Ledger`, `Leaser`, `KV`,
`Blobs`) over NATS JetStream, and owns an embedded, in-process JetStream
server (no TCP socket) over a persistent on-disk StoreDir. This file is the
short guide for working in *this* repository.

## Before you write code

1. Read [`CLAUDE.md`](CLAUDE.md) (a.k.a. `AGENTS.md`) first. It is the
   authoritative source for the design, security, dependency, build, and
   code rules this module follows. PRs that contradict it will be asked to
   change.
2. Know the dependency boundary. This is the **only** module in the tree
   where the NATS dependencies (`github.com/nats-io/nats.go`,
   `github.com/nats-io/nats-server/v2`) are sanctioned, alongside
   `github.com/looprig/storage` (the contracts this module implements, via
   a local `replace => ../storage`). Any further third-party dependency
   requires explicit approval — reach for the standard library first. If a
   task seems to need a new external package, raise it before writing code;
   don't route around the rule.
3. Open an issue for anything non-trivial so direction can be agreed before
   you spend the time, especially for anything that touches the embedded
   server lifecycle or the StoreDir containment logic.

## Design and security rules (the short version)

- **Strict typing.** No `any`/`interface{}` except at explicit
  serialization boundaries, narrowed immediately. Named types over bare
  primitives when the value carries domain meaning. No untyped magic
  numbers/strings.
- **All errors are typed.** Every distinct failure mode is a concrete
  struct with an `Error()` method (and `Unwrap()` when it carries a cause).
  Never return `errors.New(...)`/`fmt.Errorf(...)` from a package-level
  API. Sentinel errors are permitted only for leaf causes with no context
  fields. Callers classify with `errors.As`.
- **Contracts first.** Write the interface, then the implementation. Keep
  interfaces small and segregated.
- Return errors explicitly; never swallow with `_`.
- Functions over ~30 lines invite an SRP check before growing further.
- **Fail secure.** The embedded engine never starts on an unconfined or
  unwritable StoreDir: the path is `filepath.Clean`ed and confined within
  the containment root before the server starts (a traversal-crafted
  DataDir is rejected with a typed error).
- StoreDir is owner-only (`0700`); the durable journal may hold
  conversation content.
- The store lock fails closed: a platform without `flock` refuses to open
  rather than run two engines over one StoreDir.
- Every I/O method takes a `context.Context`; callers set deadlines. No
  unbounded blocking.

## Build, test, and secure

Run these before pushing. CI runs the same. Every target runs with
`GOWORK=off` so the parent `go.work` at `~/code` never captures this
module.

```sh
make fmt              # gofmt the whole module in place
make fmt-check        # verify gofmt cleanliness without writing
make vet              # go vet
make test             # go test -race ./...                  (always -race)
make test-integration # go test -tags integration -race ./...
make lint             # fmt-check + vet + staticcheck + gosec
make vuln             # go mod verify + govulncheck
make secure           # lint + vuln
make check            # fmt-check + vet + test — the pre-commit gate
```

`staticcheck`, `gosec`, and `govulncheck` are `go.mod` `tool` dependencies
(dev/tool only, approved in `CLAUDE.md`), invoked via `go tool <name>` —
not PATH binaries. `go mod tidy` keeps them resolved.

## Tests

- **Table-driven tests, mandatory**, each with `t.Parallel()`. Cover the
  happy path, boundary values (zero/empty/max), error cases, and domain
  edge cases.
- **Unit vs integration split.** Unit tests run with no server and no
  network. Anything that needs a running embedded server is tagged
  `//go:build integration` and lives in a `*_integration_test.go` file.
- A test that passes without `-race` but fails with it is **not passing**.
- Fuzz any parser of external input: `go test -fuzz=FuzzXxx ./pkg -fuzztime=30s`.
- Never assume a test framework or script beyond what's in the `Makefile`;
  if you change how tests run, update it.

## Design docs and plans

Non-trivial work goes through a short design doc in
[`docs/plans/`](docs/plans/), named `YYYY-MM-DD-<topic>-design.md` (and,
when ready, `YYYY-MM-DD-<topic>-implementation.md`) — see the existing
`2026-07-12-storage-path-reporting-*` pair for the style. Date the file the
day you start; one topic per file.

## Pull requests

- Branch from `main`, name the branch something descriptive.
- One logical change per PR. If a change spans modules (e.g. a contract
  change in `storage`), open a PR per module and stack them.
- Write a clear description: what, why, the design alternative you
  rejected, and how you verified. `make secure` output is welcome in the
  PR body.
- Don't force-push after review; add commits and let the reviewer squash.
- Don't commit secrets, tokens, or credentials. Don't add a new external
  dependency without prior approval (see `CLAUDE.md`).
- Don't update `CLAUDE.md`, `Makefile`, or `go.mod` unless the change is
  the point of the PR.

## Code of conduct

Be excellent to each other. Discussions stay technical and respectful;
personal attacks, harassment, and discrimination are not welcome.

## License

By contributing, you agree that your contributions are licensed under the
Apache License 2.0, as described in [`LICENSE`](LICENSE).
