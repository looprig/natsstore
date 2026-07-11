package natsstore

import (
	"errors"
	"math/rand"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/looprig/storage"
)

// TestSubjectForNameVectors pins the JetStream-subject encoding against explicit
// vectors: literal dots escape to %2E, then slashes become subject tokens.
func TestSubjectForNameVectors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "hierarchical id", in: "sessions/0b0e1f2a", want: "sessions.0b0e1f2a"},
		{name: "mixed segment bytes", in: "a/b_c.d-e", want: "a.b_c%2Ed-e"},
		{name: "dot escapes", in: "a.b", want: "a%2Eb"},
		{name: "slash becomes dot", in: "a/b", want: "a.b"},
		{name: "single segment", in: "abc", want: "abc"},
		{name: "dash and underscore pass through", in: "a-b_c", want: "a-b_c"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := subjectForName(tt.in)
			if err != nil {
				t.Fatalf("subjectForName(%q) error = %v, want nil", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("subjectForName(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestNameFromSubjectVectors pins the inverse mapping on valid subjects and
// asserts malformed subjects surface a *NameEncodingError.
func TestNameFromSubjectVectors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "hierarchical id", in: "sessions.0b0e1f2a", want: "sessions/0b0e1f2a"},
		{name: "mixed segment bytes", in: "a.b_c%2Ed-e", want: "a/b_c.d-e"},
		{name: "escaped dot", in: "a%2Eb", want: "a.b"},
		{name: "single token", in: "a.b", want: "a/b"},
		{name: "single segment", in: "abc", want: "abc"},
		{name: "empty is malformed", in: "", wantErr: true},
		{name: "stray percent is malformed", in: "a%b", wantErr: true},
		{name: "lowercase escape is malformed", in: "a%2eb", wantErr: true},
		{name: "leading token decodes to leading slash", in: ".a", wantErr: true},
		{name: "bare escape decodes to bare dot", in: "%2E", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := nameFromSubject(tt.in)
			if tt.wantErr {
				var nee *NameEncodingError
				if !errors.As(err, &nee) {
					t.Fatalf("nameFromSubject(%q) error = %T %v, want *NameEncodingError", tt.in, err, err)
				}
				if nee.Value != tt.in {
					t.Errorf("NameEncodingError.Value = %q, want %q", nee.Value, tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("nameFromSubject(%q) error = %v, want nil", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("nameFromSubject(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestStreamForNameVectors pins the stream-name encoding: '_' escapes itself
// (_u), '.' (_d) and '/' (_s); every other legal byte passes through.
func TestStreamForNameVectors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "hierarchical id", in: "sessions/0b0e1f2a", want: "sessions_s0b0e1f2a"},
		{name: "mixed segment bytes", in: "a/b_c.d-e", want: "a_sb_uc_dd-e"},
		{name: "dot escapes", in: "a.b", want: "a_db"},
		{name: "slash escapes", in: "a/b", want: "a_sb"},
		{name: "single segment", in: "abc", want: "abc"},
		{name: "underscore escapes, dash passes", in: "a-b_c", want: "a-b_uc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := streamForName(tt.in)
			if err != nil {
				t.Fatalf("streamForName(%q) error = %v, want nil", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("streamForName(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestNameFromStreamVectors pins the inverse stream mapping and asserts malformed
// stream tokens surface a *NameEncodingError.
func TestNameFromStreamVectors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "hierarchical id", in: "sessions_s0b0e1f2a", want: "sessions/0b0e1f2a"},
		{name: "mixed segment bytes", in: "a_sb_uc_dd-e", want: "a/b_c.d-e"},
		{name: "escaped dot", in: "a_db", want: "a.b"},
		{name: "escaped slash", in: "a_sb", want: "a/b"},
		{name: "single segment", in: "abc", want: "abc"},
		{name: "empty is malformed", in: "", wantErr: true},
		{name: "dangling escape is malformed", in: "a_", wantErr: true},
		{name: "unknown escape is malformed", in: "a_x", wantErr: true},
		{name: "uppercase is malformed", in: "A_db", wantErr: true},
		{name: "literal dot is non-canonical", in: "a.b", wantErr: true},
		{name: "escaped leading dot is malformed", in: "_dabc", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := nameFromStream(tt.in)
			if tt.wantErr {
				var nee *NameEncodingError
				if !errors.As(err, &nee) {
					t.Fatalf("nameFromStream(%q) error = %T %v, want *NameEncodingError", tt.in, err, err)
				}
				if nee.Value != tt.in {
					t.Errorf("NameEncodingError.Value = %q, want %q", nee.Value, tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("nameFromStream(%q) error = %v, want nil", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("nameFromStream(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestSubjectAntiCollision is the core correctness property for subjects: the
// names "a.b" and "a/b" — a literal dot vs a hierarchy slash — must encode to
// distinct subjects, or the two locations would alias.
func TestSubjectAntiCollision(t *testing.T) {
	t.Parallel()
	dot, err := subjectForName("a.b")
	if err != nil {
		t.Fatalf("subjectForName(\"a.b\"): %v", err)
	}
	slash, err := subjectForName("a/b")
	if err != nil {
		t.Fatalf("subjectForName(\"a/b\"): %v", err)
	}
	if dot == slash {
		t.Fatalf("subjectForName(\"a.b\") == subjectForName(\"a/b\") == %q; distinct names must not collide", dot)
	}
}

// TestStreamAntiCollision asserts the same anti-collision property for stream
// names: "a.b" and "a/b" must encode differently.
func TestStreamAntiCollision(t *testing.T) {
	t.Parallel()
	dot, err := streamForName("a.b")
	if err != nil {
		t.Fatalf("streamForName(\"a.b\"): %v", err)
	}
	slash, err := streamForName("a/b")
	if err != nil {
		t.Fatalf("streamForName(\"a/b\"): %v", err)
	}
	if dot == slash {
		t.Fatalf("streamForName(\"a.b\") == streamForName(\"a/b\") == %q; distinct names must not collide", dot)
	}
}

// TestEncodersRejectInvalidName proves both encoders validate first and surface
// the storage *InvalidNameError verbatim on a name that breaks the grammar.
func TestEncodersRejectInvalidName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
	}{
		{name: "empty", in: ""},
		{name: "uppercase", in: "Abc"},
		{name: "leading slash", in: "/abc"},
		{name: "trailing slash", in: "abc/"},
		{name: "doubled slash", in: "a//b"},
		{name: "space", in: "a b"},
		{name: "percent", in: "a%b"},
		{name: "leading dot in segment", in: ".abc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var ine *storage.InvalidNameError
			if _, err := subjectForName(tt.in); !errors.As(err, &ine) {
				t.Errorf("subjectForName(%q) error = %T %v, want *storage.InvalidNameError", tt.in, err, err)
			}
			if _, err := streamForName(tt.in); !errors.As(err, &ine) {
				t.Errorf("streamForName(%q) error = %T %v, want *storage.InvalidNameError", tt.in, err, err)
			}
		})
	}
}

// streamCharset is the exact byte set a stream name may contain.
var streamCharset = regexp.MustCompile(`^[a-z0-9_-]+$`)

// TestSubjectRoundTripProperty generates many valid storage names and proves
// nameFromSubject(subjectForName(n)) == n, and that no subject output contains a
// subject wildcard/space (' ', '*', '>').
func TestSubjectRoundTripProperty(t *testing.T) {
	t.Parallel()
	seed := time.Now().UnixNano()
	t.Logf("subject round-trip seed = %d", seed)
	rng := rand.New(rand.NewSource(seed))

	for i := 0; i < 4000; i++ {
		n := genValidName(rng)
		if err := storage.ValidateName(n); err != nil {
			t.Fatalf("generator produced invalid name %q: %v", n, err)
		}
		subj, err := subjectForName(n)
		if err != nil {
			t.Fatalf("subjectForName(%q): %v", n, err)
		}
		if strings.ContainsAny(subj, " *>") {
			t.Fatalf("subjectForName(%q) = %q contains a subject wildcard/space", n, subj)
		}
		back, err := nameFromSubject(subj)
		if err != nil {
			t.Fatalf("nameFromSubject(%q): %v (from name %q)", subj, err, n)
		}
		if back != n {
			t.Fatalf("subject round-trip: got %q, want %q (subject %q)", back, n, subj)
		}
	}
}

// TestStreamRoundTripProperty generates many valid storage names and proves
// nameFromStream(streamForName(n)) == n, and that every stream output matches
// ^[a-z0-9_-]+$.
func TestStreamRoundTripProperty(t *testing.T) {
	t.Parallel()
	seed := time.Now().UnixNano()
	t.Logf("stream round-trip seed = %d", seed)
	rng := rand.New(rand.NewSource(seed))

	for i := 0; i < 4000; i++ {
		n := genValidName(rng)
		if err := storage.ValidateName(n); err != nil {
			t.Fatalf("generator produced invalid name %q: %v", n, err)
		}
		stream, err := streamForName(n)
		if err != nil {
			t.Fatalf("streamForName(%q): %v", n, err)
		}
		if !streamCharset.MatchString(stream) {
			t.Fatalf("streamForName(%q) = %q is outside [a-z0-9_-]", n, stream)
		}
		back, err := nameFromStream(stream)
		if err != nil {
			t.Fatalf("nameFromStream(%q): %v (from name %q)", stream, err, n)
		}
		if back != n {
			t.Fatalf("stream round-trip: got %q, want %q (stream %q)", back, n, stream)
		}
	}
}

// genValidName builds a random name that satisfies storage.ValidateName: 1..4
// segments joined by '/', each segment starting with [a-z0-9] and continuing
// with [a-z0-9_.-]. The alphabets over-sample '.', '_' and '-' to stress the
// escape paths.
func genValidName(rng *rand.Rand) string {
	const startBytes = "abcdefghijklmnopqrstuvwxyz0123456789"
	const restBytes = "abcdefghijklmnopqrstuvwxyz0123456789___...---"

	segs := 1 + rng.Intn(4)
	parts := make([]string, segs)
	for s := 0; s < segs; s++ {
		var b strings.Builder
		b.WriteByte(startBytes[rng.Intn(len(startBytes))])
		rest := rng.Intn(8)
		for r := 0; r < rest; r++ {
			b.WriteByte(restBytes[rng.Intn(len(restBytes))])
		}
		parts[s] = b.String()
	}
	return strings.Join(parts, "/")
}
