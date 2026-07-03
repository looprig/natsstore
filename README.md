# natsstore

`natsstore` implements [`storekit`](../storekit)'s four storage primitives — `Ledger`,
`Leaser`, `KV`, and `Blobs` — over **NATS JetStream**. It is the only module in the tree
that depends on the NATS packages; consumers depend on the neutral `storekit` contracts and
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

## Usage

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

st, err := natsstore.Open(ctx, natsstore.Options{EmbeddedDir: "/var/lib/myapp/store"})
if err != nil {
    return err
}
defer st.Close(ctx)

// Hand the four-primitive bundle to a facade such as sessionstore.Open.
sess, err := sessionstore.Open(ctx, st.Composite) // or st.Backend()
```

`Store` embeds `*storekit.Composite`, so each primitive is reachable as a promoted field
(`st.Ledger`, `st.Leaser`, `st.KV`, `st.Blobs`); the whole bundle is `st.Composite` (or
`st.Backend()`). `Close` drains the connection and — in embedded mode — shuts the in-process
server down afterwards; it is idempotent.

## Buckets & durability

The ledger provisions its stream lazily on first append; the lease KV, application KV, and
blob object-store buckets are provisioned idempotently at `Open` (`CreateOrUpdate*`), so
reopening the same embedded StoreDir rebinds the existing buckets and sees the persisted
data rather than failing.

## Dependencies

The NATS dependencies are sanctioned **only in this module** (`github.com/nats-io/nats.go`
for the JetStream client, `github.com/nats-io/nats-server/v2` for the embedded in-process
server). Everything else is stdlib plus the local `storekit` contracts. See `CLAUDE.md`.
