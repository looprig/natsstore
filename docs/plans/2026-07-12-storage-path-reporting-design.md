# NATS Store Storage-Path Reporting Design

**Date:** 2026-07-12

## Context

`storage` v0.2.0 adds the optional `storage.PathReporter` capability. Consumers use it to discover canonical local persistence roots and reject workspace snapshots that overlap the storage backend. `natsstore` has two modes: an embedded JetStream server that persists beneath `Options.EmbeddedDir`, and a remote NATS connection whose filesystem is outside the current process.

## Decision

An embedded `Store` reports exactly one path: the actual canonical JetStream `StoreDir`. A remote `Store` reports no paths. The embedded path is resolved only after `openEngineAt` has created and opened the directory, using `filepath.Clean`, `filepath.Abs`, and `filepath.EvalSymlinks`. This ensures a symlinked `EmbeddedDir` reports the physical persistence root used for overlap checks.

Path discovery is frozen during `Open`. Every `StoragePaths` call returns a defensive clone, so callers cannot mutate later results. The outer `Store` holds the reporter in a named private field and exposes it only through an explicit pointer-receiver method; this avoids making a value copy of `Store` (including its mutex and live backend handles) satisfy `storage.PathReporter`. Each concrete persistence primitive (`ledgerStore`, `leaserStore`, `kvStore`, and `blobStore`) receives the same immutable private reporter. This matters because callers often hand out `Store.Backend()` and then see only the primitive interface fields on `storage.Composite`; each provider remains independently discoverable with a `storage.PathReporter` type assertion.

The reporting helper remains private and does not expand constructors or public options. Existing unit-test constructors continue to produce a provider that reports no paths. `buildComposite` accepts the private reporter and installs it on the four concrete adapters before converting them to storage interfaces.

## Error Handling and Cleanup

Canonicalization failure is a local StoreDir resolution failure and returns the existing typed `*StoreDirError`, with the requested embedded path and underlying filesystem cause. Because canonicalization follows engine startup, `openEmbedded` closes the engine before returning the error. No half-open store escapes.

Remote mode never attempts to resolve server-side paths and never infers a filesystem root from its URL.

## Dependency Update

`go.mod` requires `github.com/looprig/storage v0.2.0`. The existing local replace remains for coordinated workspace development.

## Testing

- Compile-time assertions prove `Store` and all four concrete primitives implement `storage.PathReporter`.
- Unit tests cover empty reports and mutation defense on the private reporter without starting a server.
- Embedded integration tests open a store through a symlinked parent and verify the outer store and every primitive report the canonical target directory.
- A remote-mode unit-level composition test verifies a store assembled without local paths reports an empty slice; this avoids requiring a network broker. The build path passes the same empty reporter in actual remote mode.
- Full unit and integration race suites, formatting, and vet remain the release gates.

## Non-Goals

- Reporting NATS server paths for remote URLs.
- Exposing engine internals or a new public path configuration API.
- Changing JetStream persistence layout or store lifecycle semantics.
