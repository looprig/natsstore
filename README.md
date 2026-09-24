# natsstore

`natsstore` implements [`storage`](https://github.com/looprig/storage)'s five storage primitives — `Ledger`,
`Leaser`, `KV`, `Blobs`, and `OrderedIndex` — over **NATS JetStream**. It is the only module in the tree
that depends on the NATS packages; consumers depend on the neutral `storage` contracts and
wire `natsstore` in at their composition root.

A `natsstore.Store` runs over one of two backends, chosen at `Open`:

- **Embedded** (`Options.EmbeddedDir`) — the Store owns an in-process JetStream server (a
  `DontListen` engine, no TCP socket) over a durable on-disk StoreDir at the
  caller-provided directory, so a single process gets a durable JetStream backend with no
  external broker to run. The directory is used **directly** as the StoreDir (created
  `0700`); the caller owns the path, so no `~/.looprig` default and no `$XDG_DATA_HOME`
  confinement are applied. No cross-process store lock is taken, so the caller must
  guarantee a **single open per `EmbeddedDir`** — two Stores or processes over the same
  dir would corrupt the JetStream file store.
- **Remote** (`Options.URL`) — the Store dials an external NATS server (secure defaults; no
  `InsecureSkipVerify`) and owns only the connection.

Exactly one of `URL` / `EmbeddedDir` must be set (else an `*OptionsError`).

## Status

- Implements all five storage primitives and conforms to the `github.com/looprig/storage`
  v0.7.0 contract, including its nested-name KV and Blobs cases (a name and a name extending
  it with `/…` coexist).
- Implements the optional `storage.BlobReaderLifecycle` capability (bounded Blob reader
  shutdown, since v0.5.1), so it satisfies consumers such as `sessionstore.Open` that require
  it.
- Also exports the lower-level embedded-engine API (`OpenEngine`, `OpenLockedEngine`) that
  `Open` builds on; `OpenLockedEngine` takes a cross-process store lock, `Open`'s embedded
  mode does not.

## Install

```sh
go get github.com/looprig/natsstore@latest
```

It sits in tier 1 of the Looprig graph (foundation adapters); its only Looprig dependency is
`github.com/looprig/storage`.

## Usage

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

st, err := natsstore.Open(ctx, natsstore.Options{EmbeddedDir: "/var/lib/myapp/store"})
if err != nil {
    return err
}
defer st.Close(ctx)

// Hand the complete five-primitive bundle to a facade such as sessionstore.Open.
sess, err := sessionstore.Open(ctx, st.Composite) // or st.Backend()
```

`Store` embeds `*storage.Composite`, so each primitive is reachable as a promoted field
(`st.Ledger`, `st.Leaser`, `st.KV`, `st.Blobs`, `st.OrderedIndex`); the whole bundle is
`st.Composite` (or `st.Backend()`). `Close` first cancels and waits for every derived
OrderedIndex namespace view, then drains the connection and — in embedded mode — shuts
the in-process server down. It is idempotent. A queried-but-unwritten OrderedIndex
namespace retains one retrying subscription view so a later stream can be observed; the
view is process-local derived state, and `Close` cancels and accounts for it. Applications
with unbounded dynamic namespaces should therefore bound or reuse namespace names rather
than treating a read as a free probe.

## Buckets & durability

The ledger and OrderedIndex namespace streams provision lazily on first write; the lease
KV, application KV, and blob object-store buckets are provisioned idempotently at `Open`
(`CreateOrUpdate*`), so
reopening the same embedded StoreDir rebinds the existing buckets and sees the persisted
data rather than failing.

OrderedIndex creates publish a two-message atomic batch (scope counter plus record) and
serialize same-scope creators within one process. Independent handles and processes still
coordinate through JetStream subject-sequence preconditions and bounded jittered retries.
The pinned embedded NATS server's default limit is 50 in-flight atomic batches per stream.
Remote operators may configure `MaxBatchInflightPerStream`, so their effective cap can
differ. The provider does not add a distributed admission coordinator: deployments that
can exceed their server's configured cap with simultaneous creates in one namespace should
shard namespaces or externally bound that concurrency. A server batch-cap rejection
remains a definite typed batch error for the caller to retry; repeated subject-sequence
races exhaust the provider's bounded retry budget as `*OrderedContentionError`. Neither
case silently weakens atomicity or order guarantees.

## Blob reader lifecycle

natsstore implements Storage's optional `storage.BlobReaderLifecycle`
capability (introduced in storage v0.6.0). Each reader returned by `Blobs.Get`
supports bounded shutdown while retaining JetStream's streaming ObjectStore
path. Close publishes closure before touching the ObjectStore result, is safe
concurrent with Read, calls the underlying Close exactly once, and returns one
stable result to repeated or concurrent callers. After Close returns, later
Reads return no bytes and `fs.ErrClosed`; a successful Get never returns a
literal or typed-nil reader.

The provider owns an ephemeral, exact-subject pull consumer for each non-empty
object reader. Every blocked chunk fetch has a five-second ceiling, while a
reader that continues making progress has no absolute lifetime. The provider
uses AckNone, in-memory consumer state, and no client-side auto-reset path, then
advertises a conservative six-second shutdown bound. Composition code can
discover the optional capability through the Storage contract:

```go
if lifecycle, ok := st.Blobs.(storage.BlobReaderLifecycle); ok {
    bound := lifecycle.BlobReaderCloseBound()
    _ = bound
}
```

The reader validates the ObjectStore chunk order, count, total size, and SHA-256
digest before EOF. Caller cancellation and Close stop its ephemeral consumer;
the server-side inactivity threshold bounds cleanup if immediate teardown is
interrupted. The provider runs Storage's shared lifecycle conformance suite and
retains its native missing-chunk blocked-read shutdown proof.

## Dependencies

The NATS dependencies are sanctioned **only in this module** (`github.com/nats-io/nats.go`
for the JetStream client, `github.com/nats-io/nats-server/v2` for the embedded in-process
server). Everything else is stdlib plus the local `storage` contracts. See `CLAUDE.md`.

## Development

The baseline is Go 1.26.8. Verify the module standalone, never against a workspace:

```sh
GOWORK=off go test ./...
make check              # fmt-check, vet, staticcheck, gosec, govulncheck, race tests, build
make test-integration   # GOWORK=off go test -tags integration -race ./...
```

The `integration` build tag enables the storage conformance suite and the JetStream
integration tests. They run against an embedded in-process server in a temporary directory,
so no external NATS server is required. `examples/embedded` is a runnable example of the
embedded mode.

## License

Apache License 2.0; see [LICENSE](LICENSE).
