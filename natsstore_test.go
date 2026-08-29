package natsstore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/looprig/storage"
)

// TestOptionsResolve covers the server-free validation half of Open: exactly one of
// URL/EmbeddedDir must be set, an EmbeddedDir must be absolute (and is cleaned), and a
// MaxPayload is either defaulted (non-positive) or floor-checked against the ledger stream
// ceiling. Every rejection is a typed *OptionsError.
func TestOptionsResolve(t *testing.T) {
	t.Parallel()

	const defPayload = maxPayload // 8 MiB default

	tests := []struct {
		name        string
		opts        Options
		wantErr     bool
		wantField   string // asserted when wantErr
		wantMode    storeMode
		wantDir     string
		wantURL     string
		wantPayload int32
	}{
		{
			name:      "neither url nor dir",
			opts:      Options{},
			wantErr:   true,
			wantField: "URL/EmbeddedDir",
		},
		{
			name:      "both url and dir",
			opts:      Options{URL: "nats://localhost:4222", EmbeddedDir: "/data/store"},
			wantErr:   true,
			wantField: "URL/EmbeddedDir",
		},
		{
			name:      "whitespace-only dir with no url is neither",
			opts:      Options{EmbeddedDir: "   "},
			wantErr:   true,
			wantField: "URL/EmbeddedDir",
		},
		{
			name:      "relative dir rejected",
			opts:      Options{EmbeddedDir: "relative/store"},
			wantErr:   true,
			wantField: "EmbeddedDir",
		},
		{
			name:      "dot-relative dir rejected",
			opts:      Options{EmbeddedDir: "./store"},
			wantErr:   true,
			wantField: "EmbeddedDir",
		},
		{
			name:      "maxpayload below ledger ceiling rejected",
			opts:      Options{EmbeddedDir: "/data/store", MaxPayload: ledgerMaxMsgSize - 1},
			wantErr:   true,
			wantField: "MaxPayload",
		},
		{
			name:        "absolute dir, default payload applied",
			opts:        Options{EmbeddedDir: "/data/store"},
			wantMode:    modeEmbedded,
			wantDir:     "/data/store",
			wantPayload: defPayload,
		},
		{
			name:        "uncleaned absolute dir is cleaned",
			opts:        Options{EmbeddedDir: "/data/./sub/../store"},
			wantMode:    modeEmbedded,
			wantDir:     "/data/store",
			wantPayload: defPayload,
		},
		{
			name:        "explicit valid maxpayload preserved",
			opts:        Options{EmbeddedDir: "/data/store", MaxPayload: ledgerMaxMsgSize},
			wantMode:    modeEmbedded,
			wantDir:     "/data/store",
			wantPayload: ledgerMaxMsgSize,
		},
		{
			name:     "remote url (payload unused in remote mode)",
			opts:     Options{URL: "nats://localhost:4222"},
			wantMode: modeRemote,
			wantURL:  "nats://localhost:4222",
			// maxPayload is not resolved in remote mode: the remote broker owns its limit.
			wantPayload: 0,
		},
		{
			name:        "remote url is trimmed",
			opts:        Options{URL: "  nats://host:4222  "},
			wantMode:    modeRemote,
			wantURL:     "nats://host:4222",
			wantPayload: 0,
		},
		{
			name:     "remote url ignores a below-floor payload",
			opts:     Options{URL: "nats://host:4222", MaxPayload: 1},
			wantMode: modeRemote,
			wantURL:  "nats://host:4222",
			// MaxPayload is neither applied nor floor-checked in remote mode.
			wantPayload: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.opts.resolve()
			if (err != nil) != tt.wantErr {
				t.Fatalf("resolve() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				var oe *OptionsError
				if !errors.As(err, &oe) {
					t.Fatalf("resolve() error = %T %v, want *OptionsError", err, err)
				}
				if oe.Field != tt.wantField {
					t.Errorf("OptionsError.Field = %q, want %q", oe.Field, tt.wantField)
				}
				return
			}
			if got.mode != tt.wantMode {
				t.Errorf("mode = %d, want %d", got.mode, tt.wantMode)
			}
			if got.embeddedDir != tt.wantDir {
				t.Errorf("embeddedDir = %q, want %q", got.embeddedDir, tt.wantDir)
			}
			if got.url != tt.wantURL {
				t.Errorf("url = %q, want %q", got.url, tt.wantURL)
			}
			if got.maxPayload != tt.wantPayload {
				t.Errorf("maxPayload = %d, want %d", got.maxPayload, tt.wantPayload)
			}
		})
	}
}

// TestOpenRejectsBadOptions proves the public Open entry point fails closed with an
// *OptionsError on an invalid combination BEFORE standing any backend up (no server, no
// dial) — the validation runs first.
func TestOpenRejectsBadOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts Options
	}{
		{name: "neither", opts: Options{}},
		{name: "both", opts: Options{URL: "nats://x:4222", EmbeddedDir: "/data/store"}},
		{name: "relative dir", opts: Options{EmbeddedDir: "rel/store"}},
		{name: "below-floor payload", opts: Options{EmbeddedDir: "/data/store", MaxPayload: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			st, err := Open(context.Background(), tt.opts)
			if st != nil {
				t.Errorf("Open returned a non-nil Store on invalid options")
			}
			var oe *OptionsError
			if !errors.As(err, &oe) {
				t.Fatalf("Open() error = %T %v, want *OptionsError", err, err)
			}
		})
	}
}

// TestStoreCloseIdempotent proves Close is a no-op on a second call and does not panic on
// a Store with no live backend (both engine and conn nil) — the idempotency guard is pure
// (no backend I/O), so it is testable without a server.
func TestStoreCloseIdempotent(t *testing.T) {
	t.Parallel()

	s := &Store{}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("first Close = %v, want nil", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
	if !s.closed {
		t.Error("closed flag not set after Close")
	}
}

func TestStoreCloseOrdersAndJoinsLifecycleErrors(t *testing.T) {
	t.Run("ordered views stop before backend", func(t *testing.T) {
		ordered := newOrderedStore(newFakeOrderedSeam())
		viewCtx, cancelView := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			<-viewCtx.Done()
			close(done)
		}()
		ordered.views["sessions"] = &orderedView{namespace: "sessions", cancel: cancelView, done: done}

		backendErr := errors.New("backend close failed")
		backendCalls := 0
		st := newStore(&storage.Composite{OrderedIndex: ordered}, newLocalPathReporter(), ordered, func() error {
			backendCalls++
			select {
			case <-done:
			default:
				t.Error("backend close ran before the ordered view stopped")
			}
			return backendErr
		})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := st.Close(ctx); !errors.Is(err, backendErr) {
			t.Fatalf("Close = %v, want backend error", err)
		}
		if err := st.Close(ctx); err != nil {
			t.Fatalf("second Close = %v, want nil", err)
		}
		if backendCalls != 1 {
			t.Errorf("backend close calls = %d, want 1", backendCalls)
		}
	})

	t.Run("ordered and backend errors are joined", func(t *testing.T) {
		ordered := newOrderedStore(newFakeOrderedSeam())
		ordered.views["stuck"] = &orderedView{namespace: "stuck", cancel: func() {}, done: make(chan struct{})}
		backendErr := errors.New("backend close failed")
		st := newStore(&storage.Composite{OrderedIndex: ordered}, newLocalPathReporter(), ordered, func() error { return backendErr })
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := st.Close(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Close error = %v, want joined context.Canceled", err)
		}
		if !errors.Is(err, backendErr) {
			t.Errorf("Close error = %v, want joined backend error", err)
		}
	})
}

// TestRedactURL proves a NATS URL's userinfo password never survives into a *ConnectError:
// a password is replaced, a credential-free URL is unchanged, each entry of a
// comma-separated server list is redacted independently, and an unparseable entry collapses
// to a placeholder rather than echoing a possible secret.
func TestRedactURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		raw        string
		want       string
		mustNotHas string // substring that must be absent (a leaked secret)
	}{
		{
			name: "no credentials unchanged",
			raw:  "nats://host:4222",
			want: "nats://host:4222",
		},
		{
			name:       "password redacted",
			raw:        "nats://user:sup3rsecret@host:4222",
			want:       "nats://user:xxxxx@host:4222",
			mustNotHas: "sup3rsecret",
		},
		{
			name:       "comma list redacts each entry",
			raw:        "nats://u:p1@a:4222,nats://v:p2@b:4222",
			want:       "nats://u:xxxxx@a:4222,nats://v:xxxxx@b:4222",
			mustNotHas: "p1",
		},
		{
			name: "unparseable collapses to placeholder",
			raw:  "%zz",
			want: "[redacted]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := redactURL(tt.raw)
			if got != tt.want {
				t.Errorf("redactURL(%q) = %q, want %q", tt.raw, got, tt.want)
			}
			if tt.mustNotHas != "" && strings.Contains(got, tt.mustNotHas) {
				t.Errorf("redactURL(%q) = %q leaked secret %q", tt.raw, got, tt.mustNotHas)
			}
		})
	}
}

// TestTypedErrorMessages proves the package's Open-path typed errors render a stable,
// non-empty message and unwrap their cause where they carry one — so callers can classify
// with errors.As and log a useful string.
func TestTypedErrorMessages(t *testing.T) {
	t.Parallel()

	cause := errors.New("boom")
	tests := []struct {
		name        string
		err         error
		wantSub     string
		wantUnwraps bool
	}{
		{
			name:    "options error",
			err:     &OptionsError{Field: "MaxPayload", Reason: "too small"},
			wantSub: "MaxPayload",
		},
		{
			name:        "connect error unwraps",
			err:         &ConnectError{URL: "nats://host:4222", Cause: cause},
			wantSub:     "nats://host:4222",
			wantUnwraps: true,
		},
		{
			name:        "wiring error unwraps",
			err:         &WiringError{Component: "kv-bucket", Cause: cause},
			wantSub:     "kv-bucket",
			wantUnwraps: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if msg := tt.err.Error(); !strings.Contains(msg, tt.wantSub) {
				t.Errorf("Error() = %q, want substring %q", msg, tt.wantSub)
			}
			if tt.wantUnwraps && !errors.Is(tt.err, cause) {
				t.Errorf("errors.Is(err, cause) = false, want the error to unwrap its cause")
			}
		})
	}
}
