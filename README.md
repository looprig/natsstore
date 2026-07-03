# natsstore

`natsstore` implements [`storekit`](../storekit)'s storage primitives — `Ledger`, `Leaser`,
`KV`, and `Blobs` — over **NATS JetStream**, and ships an **embedded, in-process JetStream
server** (no TCP socket) over a persistent on-disk StoreDir, so a single process gets a
durable JetStream backend with no external broker to run. It is the only module in the tree
that depends on the NATS packages; consumers depend on the neutral `storekit` contracts and
wire `natsstore` in at their composition root.
