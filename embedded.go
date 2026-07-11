// Package natsstore implements storage's storage primitives over NATS JetStream and owns
// an embedded, in-process JetStream server (no TCP socket) over a persistent on-disk
// StoreDir, so a single process gets a durable JetStream backend with no external broker.
//
// This file owns the embedded engine lifecycle: it starts the in-process server over a
// confined StoreDir, connects to it in-process, hands back a bound JetStreamContext the
// adapters write through, and shuts the server down cleanly. It is the only place that
// imports nats-server/v2/server — the embedded server is a composition concern.
package natsstore

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const (
	// defaultDirName is the per-user data directory under the home root. It matches the
	// existing ~/.looprig convention (where the CLI already writes looprig.log), so all
	// state lives under one directory.
	defaultDirName = ".looprig"
	// jetstreamDirName is the subdirectory under the data dir that holds the embedded
	// server's StoreDir (its file-backed streams + KV + object buckets).
	jetstreamDirName = "jetstream"
	// storeDirPerm is the StoreDir permission: owner-only (0700). The durable journal may
	// hold conversation content, so it is never group/world readable.
	storeDirPerm = 0o700
	// defaultSyncInterval is the embedded server's fsync cadence — the power-loss
	// durability knob, set EXPLICITLY rather than left to the server default. A
	// conservative few seconds bounds the data lost on an OS crash / power loss to that
	// window without fsyncing on every append (which would dominate latency). It is a
	// deliberate durability/throughput trade-off for a local single-user backend.
	defaultSyncInterval = 2 * time.Second
	// readyTimeout bounds how long Open waits for the embedded server to accept
	// connections before failing closed.
	readyTimeout = 10 * time.Second
	// maxPayload is the connection-level maximum message size the embedded server
	// accepts. The nats-server default is exactly 1 MB (1048576 bytes), but every
	// storage backend must accept a 1 MiB (1<<20) ledger payload — and JetStream
	// adds subject + header framing on top — so the default would reject a
	// floor-sized append. This is set comfortably above the ledger stream's own
	// 4 MiB per-message ceiling (ledgerMaxMsgSize) so the stream cap, not the
	// connection, is the effective per-record limit.
	maxPayload int32 = 8 << 20
)

// StoreDirError reports that the embedded server's StoreDir could not be resolved or
// created: an empty/unresolvable home, an empty data dir, a path that escapes the home
// root (traversal), or a mkdir failure. It fails closed — the engine never starts on an
// unconfined or unwritable StoreDir. Cause chains the underlying os error when present.
type StoreDirError struct {
	Path  string
	Cause error
}

func (e *StoreDirError) Error() string {
	msg := "natsstore: invalid StoreDir"
	if e.Path != "" {
		msg += " " + e.Path
	}
	if e.Cause != nil {
		return msg + ": " + e.Cause.Error()
	}
	return msg
}
func (e *StoreDirError) Unwrap() error { return e.Cause }

// ServerStartError reports that the embedded JetStream server could not be created,
// became ready within the timeout, or could not be connected to in-process. It fails
// closed: without a live engine there is no durable backend.
type ServerStartError struct{ Cause error }

func (e *ServerStartError) Error() string {
	if e.Cause != nil {
		return "natsstore: embedded server start failed: " + e.Cause.Error()
	}
	return "natsstore: embedded server start failed"
}
func (e *ServerStartError) Unwrap() error { return e.Cause }

// errServerNotReady is the leaf cause when the embedded server does not accept
// connections within readyTimeout. A sentinel is permitted (no context fields).
var errServerNotReady = errors.New("natsstore: embedded server not ready within timeout")

// EngineOptions configures the embedded engine. DataDir is the StoreDir (resolved +
// confined to the home root); SyncInterval is the explicit fsync cadence (the power-loss
// knob). A zero SyncInterval falls back to the conservative default.
type EngineOptions struct {
	DataDir      string
	SyncInterval time.Duration
	// MaxPayload is the connection-level maximum message size the DontListen server
	// accepts. A zero/negative value falls back to the package default (maxPayload,
	// 8 MiB). It must be >= the ledger stream's per-message ceiling (ledgerMaxMsgSize)
	// so a floor-sized append is not rejected at the connection; natsstore.Open's
	// embedded mode validates that floor before driving the engine, and openEngineAt
	// applies the default when this is unset.
	MaxPayload int32
}

// DefaultEngineOptions returns convenience engine options: StoreDir at
// ~/.looprig/jetstream (overridable by $XDG_DATA_HOME → $XDG_DATA_HOME/looprig/jetstream)
// and the conservative explicit SyncInterval. It resolves the home/XDG root via os,
// failing closed (typed *StoreDirError) if neither is available.
func DefaultEngineOptions() (EngineOptions, error) {
	dir, err := defaultDataDir()
	if err != nil {
		return EngineOptions{}, err
	}
	return EngineOptions{DataDir: dir, SyncInterval: defaultSyncInterval}, nil
}

// defaultDataDir computes the default StoreDir. It honors $XDG_DATA_HOME when set
// ($XDG_DATA_HOME/looprig/jetstream); otherwise it falls back to ~/.looprig/jetstream,
// matching the existing ~/.looprig convention (looprig.log). It never returns an
// unconfined path: the XDG/home root is treated as the containment root.
func defaultDataDir() (string, error) {
	if xdg := strings.TrimSpace(os.Getenv("XDG_DATA_HOME")); xdg != "" {
		return filepath.Join(xdg, "looprig", jetstreamDirName), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", &StoreDirError{Cause: err}
	}
	return filepath.Join(home, defaultDirName, jetstreamDirName), nil
}

// resolveStoreDir cleans dataDir and verifies it stays within root (the home/XDG
// containment root) — a fail-secure guard against a traversal-crafted DataDir escaping
// the user's own tree. It returns the cleaned, confined absolute-or-relative path (the
// same form root is in) or a typed *StoreDirError. It does NOT create the directory
// (Open does, after this check).
func resolveStoreDir(root, dataDir string) (string, error) {
	if strings.TrimSpace(root) == "" {
		return "", &StoreDirError{Path: dataDir, Cause: errors.New("natsstore: empty home root")}
	}
	if strings.TrimSpace(dataDir) == "" {
		return "", &StoreDirError{Cause: errors.New("natsstore: empty data dir")}
	}
	cleanRoot := filepath.Clean(root)
	clean := filepath.Clean(dataDir)
	// Confinement: the cleaned path must be the root itself or live beneath it. A
	// rel-based check defeats ".." traversal that string-prefix checks miss.
	rel, err := filepath.Rel(cleanRoot, clean)
	if err != nil {
		return "", &StoreDirError{Path: clean, Cause: err}
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", &StoreDirError{Path: clean, Cause: errors.New("natsstore: StoreDir escapes the home root")}
	}
	return clean, nil
}

// Engine owns the embedded JetStream server, the in-process client connection, and the
// bound JetStreamContext. It is the composition-root handle a consumer builds once at
// startup and Closes once at shutdown. It is NOT safe for concurrent Close, but the
// consumer closes it exactly once on exit.
type Engine struct {
	srv *server.Server
	nc  *nats.Conn
	js  nats.JetStreamContext
}

// OpenEngine starts an embedded JetStream server on a home/XDG-CONFINED StoreDir,
// connects to it in-process (no TCP), and returns a live Engine. It resolves the
// containment root ($XDG_DATA_HOME or home), verifies opts.DataDir stays within it
// (fail-secure against traversal), then hands off to openEngineAt for the fail-secure
// startup. It is the confined convenience entry point (the ~/.looprig / $XDG_DATA_HOME
// app-policy path); natsstore.Open's embedded mode instead drives openEngineAt directly
// on a caller-owned absolute dir, deliberately WITHOUT this home/XDG confinement (the
// caller explicitly owns the path).
func OpenEngine(opts EngineOptions) (*Engine, error) {
	root, err := containmentRoot()
	if err != nil {
		return nil, err
	}
	storeDir, err := resolveStoreDir(root, opts.DataDir)
	if err != nil {
		return nil, err
	}
	return openEngineAt(storeDir, opts.SyncInterval, opts.MaxPayload)
}

// openEngineAt is the fail-secure half of engine startup, shared by OpenEngine (which
// confines storeDir to the home/XDG root first) and natsstore.Open's embedded mode
// (which passes a caller-owned absolute dir directly, no confinement). storeDir must be
// already resolved and cleaned by the caller. It creates storeDir 0700 if absent, starts
// the DontListen server (SyncInterval and MaxPayload defaulted when non-positive), and
// connects in-process. On any failure the partially-started server is shut down before
// returning the typed error, so it never leaks a server.
func openEngineAt(storeDir string, sync time.Duration, maxPay int32) (*Engine, error) {
	// #nosec G301 -- StoreDir is owner-only by design (may hold conversation content).
	if err := os.MkdirAll(storeDir, storeDirPerm); err != nil {
		return nil, &StoreDirError{Path: storeDir, Cause: err}
	}
	if sync <= 0 {
		sync = defaultSyncInterval
	}
	if maxPay <= 0 {
		maxPay = maxPayload
	}

	srv, err := server.NewServer(&server.Options{
		JetStream:    true,
		StoreDir:     storeDir,
		DontListen:   true,   // in-process only — no TCP socket
		SyncInterval: sync,   // explicit power-loss durability knob
		MaxPayload:   maxPay, // accept the storage 1 MiB payload floor + JetStream framing (default is 1 MB)
		NoSigs:       true,   // the consumer owns signal handling; the server must not install handlers
		NoLog:        true,   // server logs would corrupt a TUI's stdout/scrollback
	})
	if err != nil {
		return nil, &ServerStartError{Cause: err}
	}
	go srv.Start()
	if !srv.ReadyForConnections(readyTimeout) {
		srv.Shutdown()
		return nil, &ServerStartError{Cause: errServerNotReady}
	}

	nc, err := nats.Connect("", nats.InProcessServer(srv))
	if err != nil {
		srv.Shutdown()
		return nil, &ServerStartError{Cause: err}
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		srv.Shutdown()
		return nil, &ServerStartError{Cause: err}
	}
	return &Engine{srv: srv, nc: nc, js: js}, nil
}

// containmentRoot is the directory the StoreDir must stay within: $XDG_DATA_HOME when
// set, else the user's home directory. It mirrors defaultDataDir's root selection so the
// confinement check matches the default path.
func containmentRoot() (string, error) {
	if xdg := strings.TrimSpace(os.Getenv("XDG_DATA_HOME")); xdg != "" {
		return xdg, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", &StoreDirError{Cause: err}
	}
	return home, nil
}

// JetStream returns the bound JetStreamContext the adapters write through. It is valid
// until Close.
func (e *Engine) JetStream() nats.JetStreamContext { return e.js }

// Conn returns the in-process client connection. It is the handle a caller uses to
// build the context-aware jetstream.JetStream (jetstream.New) that the lease KV seam
// needs — the legacy JetStreamContext above cannot carry a per-call context. It is
// valid until Close.
func (e *Engine) Conn() *nats.Conn { return e.nc }

// Close drains the client connection and shuts the embedded server down cleanly,
// flushing JetStream state to the StoreDir. It is best-effort and safe to call once at
// exit; a drain error is returned but the server is always shut down.
func (e *Engine) Close() error {
	var err error
	if e.nc != nil {
		// Drain flushes pending publishes + unsubscribes before closing, so an in-flight
		// append is not lost on a clean exit. It closes the conn when done.
		err = e.nc.Drain()
	}
	if e.srv != nil {
		e.srv.Shutdown()
		e.srv.WaitForShutdown()
	}
	return err
}
