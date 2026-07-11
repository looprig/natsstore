package natsstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/looprig/storage"
	"github.com/nats-io/nats.go"
)

// fakeSeam is a scripted jsSeam for unit-testing ledgerStore with no running
// server. It counts publish/ensure/delete calls (so a test can assert the ledger
// NEVER retries), serves reads from an in-memory record map, and returns
// caller-scripted outcomes for each operation. Each instance is used by exactly one
// test goroutine, so it needs no locking.
type fakeSeam struct {
	ensureErr   error
	ensureCalls int

	publishSeq   uint64
	publishErr   error
	publishCalls int
	lastExpected uint64

	records    map[uint64][]byte // seq -> payload, served by getMsg/Read
	tip        uint64
	lastSeqErr error
	getMsgErr  error

	deleteErr   error
	deleteCalls int
}

func (f *fakeSeam) ensureStream(_ context.Context, _, _ string, _ int32) error {
	f.ensureCalls++
	return f.ensureErr
}

func (f *fakeSeam) publish(_ context.Context, _ string, _ []byte, expectedLastSeq uint64) (uint64, error) {
	f.publishCalls++
	f.lastExpected = expectedLastSeq
	if f.publishErr != nil {
		return 0, f.publishErr
	}
	return f.publishSeq, nil
}

func (f *fakeSeam) getMsg(_ context.Context, _ string, seq uint64) ([]byte, error) {
	if f.getMsgErr != nil {
		return nil, f.getMsgErr
	}
	data, ok := f.records[seq]
	if !ok {
		return nil, nats.ErrMsgNotFound
	}
	return data, nil
}

func (f *fakeSeam) lastSeq(_ context.Context, _ string) (uint64, error) {
	if f.lastSeqErr != nil {
		return 0, f.lastSeqErr
	}
	return f.tip, nil
}

func (f *fakeSeam) deleteStream(_ context.Context, _ string) error {
	f.deleteCalls++
	return f.deleteErr
}

// assertAmbiguous fails unless err is a *storage.AmbiguousError naming the given
// name/expected and wrapping cause (both on .Cause and via errors.Is on err).
func assertAmbiguous(t *testing.T, err error, name string, expected uint64, cause error) {
	t.Helper()
	var ae *storage.AmbiguousError
	if !errors.As(err, &ae) {
		t.Fatalf("Append = %v, want *storage.AmbiguousError", err)
	}
	if ae.Name != name || ae.Expected != expected {
		t.Errorf("AmbiguousError = {Name:%q, Expected:%d}, want {%q, %d}", ae.Name, ae.Expected, name, expected)
	}
	if !errors.Is(ae.Cause, cause) {
		t.Errorf("AmbiguousError.Cause = %v, want it to wrap %v", ae.Cause, cause)
	}
	if !errors.Is(err, cause) {
		t.Errorf("Append error does not unwrap to the ambiguous cause %v", cause)
	}
}

func TestLedgerAppendClassification(t *testing.T) {
	t.Parallel()
	const name = "sessions/append"
	const expected = uint64(3)
	otherErr := errors.New("definite backend failure")

	tests := []struct {
		name       string
		publishSeq uint64
		publishErr error
		check      func(t *testing.T, err error)
	}{
		{
			name:       "clean append returns nil",
			publishSeq: expected + 1,
			check: func(t *testing.T, err error) {
				if err != nil {
					t.Fatalf("Append = %v, want nil", err)
				}
			},
		},
		{
			name:       "wrong-last-sequence maps to ConflictError",
			publishErr: &nats.APIError{ErrorCode: nats.JSErrCodeStreamWrongLastSequence, Code: 400},
			check: func(t *testing.T, err error) {
				var ce *storage.ConflictError
				if !errors.As(err, &ce) {
					t.Fatalf("Append = %v, want *storage.ConflictError", err)
				}
				if ce.Name != name || ce.Expected != expected {
					t.Errorf("ConflictError = {Name:%q, Expected:%d}, want {%q, %d}", ce.Name, ce.Expected, name, expected)
				}
			},
		},
		{
			name:       "nats timeout maps to AmbiguousError",
			publishErr: nats.ErrTimeout,
			check:      func(t *testing.T, err error) { assertAmbiguous(t, err, name, expected, nats.ErrTimeout) },
		},
		{
			name:       "no-stream-response maps to AmbiguousError",
			publishErr: nats.ErrNoStreamResponse,
			check:      func(t *testing.T, err error) { assertAmbiguous(t, err, name, expected, nats.ErrNoStreamResponse) },
		},
		{
			name:       "context deadline maps to AmbiguousError",
			publishErr: context.DeadlineExceeded,
			check:      func(t *testing.T, err error) { assertAmbiguous(t, err, name, expected, context.DeadlineExceeded) },
		},
		{
			name:       "other publish error is surfaced verbatim (fail closed)",
			publishErr: otherErr,
			check: func(t *testing.T, err error) {
				if !errors.Is(err, otherErr) {
					t.Fatalf("Append = %v, want the raw publish error %v", err, otherErr)
				}
				var ce *storage.ConflictError
				var ae *storage.AmbiguousError
				if errors.As(err, &ce) || errors.As(err, &ae) {
					t.Errorf("Append mis-classified a definite error as Conflict/Ambiguous: %v", err)
				}
			},
		},
		{
			// context.Canceled is deliberately NOT ambiguous (see isAmbiguous): a
			// caller-driven abort is surfaced verbatim, fail-closed by choice. Pin
			// that exclusion so a future edit to isAmbiguous cannot silently fold
			// cancel into the ambiguous set.
			name:       "context canceled is surfaced verbatim (fail closed by choice)",
			publishErr: context.Canceled,
			check: func(t *testing.T, err error) {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Append = %v, want context.Canceled surfaced verbatim", err)
				}
				var ce *storage.ConflictError
				var ae *storage.AmbiguousError
				if errors.As(err, &ce) || errors.As(err, &ae) {
					t.Errorf("Append mis-classified context.Canceled as Conflict/Ambiguous: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := &fakeSeam{publishSeq: tt.publishSeq, publishErr: tt.publishErr}
			l := newLedgerStore(f)

			err := l.Append(context.Background(), name, expected, []byte("payload"))
			tt.check(t, err)

			// Regardless of outcome the ledger ensures the stream once and publishes
			// EXACTLY once — it must never retry (storage.AppendDefinite owns
			// ambiguity resolution, not the backend).
			if f.ensureCalls != 1 {
				t.Errorf("ensureStream calls = %d, want 1", f.ensureCalls)
			}
			if f.publishCalls != 1 {
				t.Errorf("publish calls = %d, want 1 (ledger must not retry)", f.publishCalls)
			}
			if f.lastExpected != expected {
				t.Errorf("publish expectedLastSeq = %d, want %d (CAS fence == expected)", f.lastExpected, expected)
			}
		})
	}
}

func TestLedgerAppendInvalidName(t *testing.T) {
	t.Parallel()
	badNames := []struct {
		label string
		value string
	}{
		{"empty", ""},
		{"leading slash", "/leading"},
		{"trailing slash", "trailing/"},
		{"doubled slash", "a//b"},
		{"uppercase", "Upper"},
		{"space", "has space"},
		{"dot-dot segment", ".."},
	}
	for _, bad := range badNames {
		t.Run(bad.label, func(t *testing.T) {
			t.Parallel()
			f := &fakeSeam{}
			l := newLedgerStore(f)

			err := l.Append(context.Background(), bad.value, 0, []byte("x"))
			var ine *storage.InvalidNameError
			if !errors.As(err, &ine) {
				t.Fatalf("Append(%q) = %v, want *storage.InvalidNameError", bad.value, err)
			}
			if ine.Name != bad.value {
				t.Errorf("InvalidNameError.Name = %q, want %q", ine.Name, bad.value)
			}
			// An invalid name is rejected BEFORE any backend I/O.
			if f.ensureCalls != 0 || f.publishCalls != 0 {
				t.Errorf("invalid name touched the backend: ensure=%d publish=%d, want 0/0", f.ensureCalls, f.publishCalls)
			}
		})
	}
}

func TestLedgerReadFromOffset(t *testing.T) {
	t.Parallel()
	records := map[uint64][]byte{
		1: []byte("r1"), 2: []byte("r2"), 3: []byte("r3"), 4: []byte("r4"),
	}
	tests := []struct {
		name     string
		from     uint64
		wantSeqs []uint64
	}{
		{"from zero yields all", 0, []uint64{1, 2, 3, 4}},
		{"from one yields all", 1, []uint64{1, 2, 3, 4}},
		{"interior from two", 2, []uint64{2, 3, 4}},
		{"interior from three", 3, []uint64{3, 4}},
		{"from tip yields last only", 4, []uint64{4}},
		{"from tip+1 is drained", 5, nil},
		{"far beyond tip is drained", 100, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := &fakeSeam{records: records, tip: 4}
			l := newLedgerStore(f)

			cur, err := l.Read(context.Background(), "sessions/read", tt.from)
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			defer func() {
				if cerr := cur.Close(); cerr != nil {
					t.Errorf("cursor Close = %v, want nil", cerr)
				}
			}()

			var gotSeqs []uint64
			for {
				rec, nerr := cur.Next(context.Background())
				if errors.Is(nerr, io.EOF) {
					break
				}
				if nerr != nil {
					t.Fatalf("Next: %v", nerr)
				}
				gotSeqs = append(gotSeqs, rec.Seq)
				if !bytes.Equal(rec.Payload, records[rec.Seq]) {
					t.Errorf("record seq %d payload = %q, want %q", rec.Seq, rec.Payload, records[rec.Seq])
				}
			}
			if !equalUint64(gotSeqs, tt.wantSeqs) {
				t.Errorf("Read(from=%d) seqs = %v, want %v", tt.from, gotSeqs, tt.wantSeqs)
			}
		})
	}
}

func TestLedgerCursorReadErrorFailsClosed(t *testing.T) {
	t.Parallel()
	boom := errors.New("backend get failed")
	f := &fakeSeam{tip: 2, getMsgErr: boom}
	l := newLedgerStore(f)

	cur, err := l.Read(context.Background(), "sessions/err", 1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer func() { _ = cur.Close() }()

	_, nerr := cur.Next(context.Background())
	if errors.Is(nerr, io.EOF) {
		t.Fatal("Next returned EOF on a backend read failure, want a typed error (fail closed)")
	}
	var rre *RecordReadError
	if !errors.As(nerr, &rre) {
		t.Fatalf("Next = %v, want *RecordReadError", nerr)
	}
	if rre.Seq != 1 {
		t.Errorf("RecordReadError.Seq = %d, want 1", rre.Seq)
	}
	if !errors.Is(nerr, boom) {
		t.Errorf("RecordReadError does not unwrap to the backend cause %v", boom)
	}
}

func TestLedgerReadInvalidName(t *testing.T) {
	t.Parallel()
	f := &fakeSeam{}
	l := newLedgerStore(f)
	_, err := l.Read(context.Background(), "Bad Name", 1)
	var ine *storage.InvalidNameError
	if !errors.As(err, &ine) {
		t.Fatalf("Read(invalid) = %v, want *storage.InvalidNameError", err)
	}
}

func TestLedgerTip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		tip     uint64
		tipErr  error
		want    uint64
		wantErr bool
	}{
		{"absent stream reports zero", 0, nil, 0, false},
		{"populated stream reports tip", 7, nil, 7, false},
		{"backend error propagates", 0, errors.New("info failed"), 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := &fakeSeam{tip: tt.tip, lastSeqErr: tt.tipErr}
			l := newLedgerStore(f)
			got, err := l.Tip(context.Background(), "sessions/tip")
			if (err != nil) != tt.wantErr {
				t.Fatalf("Tip err = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Tip = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestLedgerDelete(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		deleteErr error
		calls     int
		wantErr   bool
	}{
		{"idempotent success across repeats", nil, 3, false},
		{"backend error propagates", errors.New("delete failed"), 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := &fakeSeam{deleteErr: tt.deleteErr}
			l := newLedgerStore(f)
			var lastErr error
			for i := 0; i < tt.calls; i++ {
				lastErr = l.Delete(context.Background(), "sessions/del")
			}
			if (lastErr != nil) != tt.wantErr {
				t.Fatalf("Delete err = %v, wantErr %v", lastErr, tt.wantErr)
			}
			if f.deleteCalls != tt.calls {
				t.Errorf("deleteStream calls = %d, want %d", f.deleteCalls, tt.calls)
			}
		})
	}
}

// equalUint64 reports whether a and b hold the same elements in order, treating nil
// and empty as equal (a drained read yields a nil slice).
func equalUint64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
