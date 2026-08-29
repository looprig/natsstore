//go:build integration

package natsstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// orderCapStream is the disposable stream each subtest stands up on its own engine.
	orderCapStream = "ORDERCAP"
	// orderCapSubjects is the reserved space: ordercap.counter.<scope> holds the
	// per-scope order counter, ordercap.record.<scope>.<order> holds a stable record.
	orderCapSubjects = "ordercap.>"

	// The raw atomic-batch headers. nats.go v1.53.1 exposes AllowAtomicPublish on the
	// stream config but ships no client-side batch publisher, so the batch protocol is
	// spoken with raw headers here.
	hdrBatchID     = "Nats-Batch-Id"
	hdrBatchSeq    = "Nats-Batch-Sequence"
	hdrBatchCommit = "Nats-Batch-Commit"
	hdrExpectLast  = "Nats-Expected-Last-Subject-Sequence"
)

// orderCapAPIError mirrors the server's ApiError as it appears inside a publish ack.
type orderCapAPIError struct {
	Code        int    `json:"code"`
	ErrCode     uint16 `json:"err_code"`
	Description string `json:"description"`
}

func (e *orderCapAPIError) String() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("code=%d err_code=%d description=%q", e.Code, e.ErrCode, e.Description)
}

// orderCapAck mirrors the server's PubAck response for a batch commit. nats.go's own
// PubAck type carries neither the batch id nor the batch size, so the commit reply is
// decoded here directly.
type orderCapAck struct {
	Stream    string            `json:"stream"`
	Sequence  uint64            `json:"seq"`
	BatchID   string            `json:"batch"`
	BatchSize uint64            `json:"count"`
	Error     *orderCapAPIError `json:"error"`
}

// orderCapMsg is one message of a batch: a subject, a payload, and its OWN expected
// last sequence on that subject (0 meaning "this subject must not exist yet").
type orderCapMsg struct {
	subject       string
	data          []byte
	expectLast    uint64
	hasExpectLast bool
}

// orderCapSeam is the test-local seam that speaks the atomic-batch protocol. Keeping
// the raw headers behind one type is the same discipline seam.go applies to the
// ledger: exactly one place talks the wire protocol. It lives in the test because
// F4.1 is a capability proof that must run BEFORE any production OrderedIndex code
// exists; promoting it is the implementation task's job, not this one's.
type orderCapSeam struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	stream  jetstream.Stream
	batchID atomic.Uint64
}

// newOrderCapSeam stands up a fresh embedded engine (the repository's existing
// in-process server seam) and a disposable AllowAtomicPublish stream on it.
func newOrderCapSeam(t *testing.T) *orderCapSeam {
	t.Helper()
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", root)
	dir := filepath.Join(root, "looprig", "jetstream")

	eng, err := OpenEngine(EngineOptions{DataDir: dir, SyncInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("OpenEngine: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	js, err := jetstream.New(eng.Conn())
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	st, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:               orderCapStream,
		Subjects:           []string{orderCapSubjects},
		Retention:          jetstream.LimitsPolicy,
		Replicas:           1,
		AllowAtomicPublish: true,
	})
	if err != nil {
		// A server that does not understand allow_atomic fails HERE, and that failure
		// is the whole point of this test: report it verbatim.
		t.Fatalf("CreateStream with AllowAtomicPublish=true: %v", err)
	}
	info, err := st.Info(ctx)
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if !info.Config.AllowAtomicPublish {
		t.Fatalf("server accepted the stream but reported AllowAtomicPublish=false; config round-trip: %+v", info.Config)
	}
	return &orderCapSeam{nc: eng.Conn(), js: js, stream: st}
}

func counterSubject(scope string) string { return "ordercap.counter." + scope }

func recordSubject(scope string, order uint64) string {
	return "ordercap.record." + scope + "." + strconv.FormatUint(order, 10)
}

// lastSeq returns the stream sequence of the last message on subject, or 0 when the
// subject has never been written. 0 is exactly the value the expected-last-subject
// header uses to mean "must not exist", so the two agree by construction.
func (s *orderCapSeam) lastSeq(ctx context.Context, subject string) (uint64, []byte, error) {
	msg, err := s.stream.GetLastMsgForSubject(ctx, subject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return 0, nil, nil
	}
	if err != nil {
		return 0, nil, err
	}
	return msg.Sequence, msg.Data, nil
}

// publishBatch sends msgs as one atomic batch. Every message but the last is a
// fire-and-forget publish carrying Nats-Batch-Id/Nats-Batch-Sequence; the last also
// carries Nats-Batch-Commit and is a request, whose reply is the batch's single ack.
// A returned ack with a non-nil Error means the batch was rejected as a whole.
func (s *orderCapSeam) publishBatch(ctx context.Context, msgs []orderCapMsg) (orderCapAck, error) {
	id := "ordercap-" + strconv.FormatUint(s.batchID.Add(1), 10)
	for i, m := range msgs {
		msg := nats.NewMsg(m.subject)
		msg.Data = m.data
		msg.Header.Set(hdrBatchID, id)
		msg.Header.Set(hdrBatchSeq, strconv.Itoa(i+1))
		if m.hasExpectLast {
			msg.Header.Set(hdrExpectLast, strconv.FormatUint(m.expectLast, 10))
		}
		if i < len(msgs)-1 {
			// Same connection, so these arrive at the server in order, ahead of the
			// commit request below.
			if err := s.nc.PublishMsg(msg); err != nil {
				return orderCapAck{}, fmt.Errorf("publish batch member %d: %w", i+1, err)
			}
			continue
		}
		msg.Header.Set(hdrBatchCommit, "1")
		reply, err := s.nc.RequestMsgWithContext(ctx, msg)
		if err != nil {
			return orderCapAck{}, fmt.Errorf("commit batch %q: %w", id, err)
		}
		var ack orderCapAck
		if err := json.Unmarshal(reply.Data, &ack); err != nil {
			return orderCapAck{}, fmt.Errorf("decode commit ack %q: %w (raw %q)", id, err, reply.Data)
		}
		return ack, nil
	}
	return orderCapAck{}, errors.New("publishBatch called with no messages")
}

// allocate performs one full allocation attempt for scope: read the counter head,
// then commit {counter := n+1, record for order n+1} as one batch, each message
// fenced by its own expectation. It returns the ack unchanged so callers can
// distinguish a lost race (ack.Error set) from success without interpretation.
func (s *orderCapSeam) allocate(ctx context.Context, scope string) (order uint64, ack orderCapAck, err error) {
	cSubj := counterSubject(scope)
	seq, data, err := s.lastSeq(ctx, cSubj)
	if err != nil {
		return 0, orderCapAck{}, fmt.Errorf("read counter head %q: %w", cSubj, err)
	}
	var current uint64
	if len(data) > 0 {
		if current, err = strconv.ParseUint(string(data), 10, 64); err != nil {
			return 0, orderCapAck{}, fmt.Errorf("counter head %q holds %q: %w", cSubj, data, err)
		}
	}
	next := current + 1
	ack, err = s.publishBatch(ctx, []orderCapMsg{
		// The counter must still be at the sequence we read (0 = never written).
		{subject: cSubj, data: []byte(strconv.FormatUint(next, 10)), expectLast: seq, hasExpectLast: true},
		// The record subject is order-addressed, so it must not exist at all.
		{subject: recordSubject(scope, next), data: []byte("record-" + strconv.FormatUint(next, 10)), expectLast: 0, hasExpectLast: true},
	})
	return next, ack, err
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func (s *orderCapSeam) msgCount(ctx context.Context, t *testing.T) uint64 {
	t.Helper()
	info, err := s.stream.Info(ctx)
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	return info.State.Msgs
}

// TestOrderedCapability is the GO/NO-GO capability proof for the OrderedIndex design.
// It asserts, against the PINNED nats.go / nats-server pair this module builds with,
// that JetStream itself supplies every primitive the design leans on:
//
//	(a) an atomic two-message batch commits all-or-nothing,
//	(b) each message in that batch carries its OWN Nats-Expected-Last-Subject-Sequence,
//	(c) a wrong counter expectation commits NEITHER message,
//	(d) two concurrent allocators each obtain exactly one next order after retry,
//	(e) DeliverLastPerSubject bootstraps the current counter and record heads,
//	(f) an ordered consumer catches up to a previously captured stream sequence.
//
// It is deliberately a primitives test, not an API test: no production natsstore type
// is exercised, so a failure here is a statement about the server, not about our code.
// If any subtest fails, the implementation lane it gates must stop rather than fall
// back to KV.Keys or a process-local order allocator.
func TestOrderedCapability(t *testing.T) {
	t.Run("AtomicBatchWithIndependentExpectations", func(t *testing.T) {
		ctx := testCtx(t)
		s := newOrderCapSeam(t)

		// First allocation: counter expects 0 (absent), record expects 0 (absent).
		order, ack, err := s.allocate(ctx, "alpha")
		if err != nil {
			t.Fatalf("allocate #1: %v", err)
		}
		if ack.Error != nil {
			t.Fatalf("allocate #1 rejected: %s", ack.Error)
		}
		if order != 1 {
			t.Fatalf("first order = %d, want 1", order)
		}
		if ack.BatchSize != 2 {
			t.Fatalf("commit ack BatchSize = %d, want 2 (ack %+v)", ack.BatchSize, ack)
		}

		// Second allocation: the counter now expects a NON-ZERO sequence while the new
		// record subject still expects 0. Two different expectations in one batch is
		// property (b); if the server collapsed them to a single per-batch check this
		// commit could not succeed.
		cSeq, _, err := s.lastSeq(ctx, counterSubject("alpha"))
		if err != nil {
			t.Fatalf("read counter head: %v", err)
		}
		if cSeq == 0 {
			t.Fatal("counter head sequence is 0 after a successful allocation")
		}
		order, ack, err = s.allocate(ctx, "alpha")
		if err != nil {
			t.Fatalf("allocate #2: %v", err)
		}
		if ack.Error != nil {
			t.Fatalf("allocate #2 rejected (counter expected seq %d, record expected 0): %s", cSeq, ack.Error)
		}
		if order != 2 {
			t.Fatalf("second order = %d, want 2", order)
		}

		if got := s.msgCount(ctx, t); got != 4 {
			t.Fatalf("stream holds %d msgs after two 2-message batches, want 4", got)
		}
		// Both members of both batches are individually retrievable.
		for _, subj := range []string{counterSubject("alpha"), recordSubject("alpha", 1), recordSubject("alpha", 2)} {
			seq, _, err := s.lastSeq(ctx, subj)
			if err != nil {
				t.Fatalf("read %q: %v", subj, err)
			}
			if seq == 0 {
				t.Fatalf("subject %q is absent; the batch did not commit both messages", subj)
			}
		}
		if _, data, err := s.lastSeq(ctx, counterSubject("alpha")); err != nil {
			t.Fatalf("read counter: %v", err)
		} else if string(data) != "2" {
			t.Fatalf("counter head = %q, want %q", data, "2")
		}
	})

	t.Run("WrongCounterExpectationCommitsNeither", func(t *testing.T) {
		ctx := testCtx(t)
		s := newOrderCapSeam(t)

		if _, ack, err := s.allocate(ctx, "beta"); err != nil || ack.Error != nil {
			t.Fatalf("seed allocation failed: err=%v ack.Error=%s", err, ack.Error)
		}
		before := s.msgCount(ctx, t)
		counterSeq, counterData, err := s.lastSeq(ctx, counterSubject("beta"))
		if err != nil {
			t.Fatalf("read counter head: %v", err)
		}

		// Deliberately stale counter expectation, valid record expectation. Only the
		// FIRST message of the batch is wrong; the second is fine on its own.
		ack, err := s.publishBatch(ctx, []orderCapMsg{
			{subject: counterSubject("beta"), data: []byte("99"), expectLast: counterSeq + 7, hasExpectLast: true},
			{subject: recordSubject("beta", 99), data: []byte("record-99"), expectLast: 0, hasExpectLast: true},
		})
		if err != nil {
			t.Fatalf("publish batch with stale counter expectation: %v", err)
		}
		if ack.Error == nil {
			t.Fatalf("batch with a stale counter expectation was ACCEPTED (ack %+v); the precondition is not enforced", ack)
		}
		t.Logf("stale-expectation rejection: %s", ack.Error)

		// All-or-nothing: neither message may be visible.
		if after := s.msgCount(ctx, t); after != before {
			t.Fatalf("stream msg count moved %d -> %d after a rejected batch; the commit was not all-or-nothing", before, after)
		}
		if seq, data, err := s.lastSeq(ctx, counterSubject("beta")); err != nil {
			t.Fatalf("re-read counter head: %v", err)
		} else if seq != counterSeq || string(data) != string(counterData) {
			t.Fatalf("counter head changed after a rejected batch: seq %d->%d data %q->%q", counterSeq, seq, counterData, data)
		}
		if seq, _, err := s.lastSeq(ctx, recordSubject("beta", 99)); err != nil {
			t.Fatalf("read record subject: %v", err)
		} else if seq != 0 {
			t.Fatalf("record subject %q exists at seq %d after the batch was rejected; the second message committed alone", recordSubject("beta", 99), seq)
		}

		// The mirror case, and the sharper one: the FIRST message is valid and the
		// SECOND fails its own precondition. All-or-nothing means the valid first
		// message must be discarded too, not left behind as a partial write.
		ack, err = s.publishBatch(ctx, []orderCapMsg{
			{subject: counterSubject("beta"), data: []byte("2"), expectLast: counterSeq, hasExpectLast: true},
			// recordSubject(beta, 1) already exists from the seed, so "must not exist"
			// (expect 0) is false for it.
			{subject: recordSubject("beta", 1), data: []byte("clobber"), expectLast: 0, hasExpectLast: true},
		})
		if err != nil {
			t.Fatalf("publish batch with a failing second member: %v", err)
		}
		if ack.Error == nil {
			t.Fatalf("batch whose second member violated its expectation was ACCEPTED (ack %+v)", ack)
		}
		t.Logf("failing-second-member rejection: %s", ack.Error)
		if after := s.msgCount(ctx, t); after != before {
			t.Fatalf("stream msg count moved %d -> %d; the valid FIRST member of a rejected batch was committed", before, after)
		}
		if seq, data, err := s.lastSeq(ctx, counterSubject("beta")); err != nil {
			t.Fatalf("re-read counter head: %v", err)
		} else if seq != counterSeq || string(data) != string(counterData) {
			t.Fatalf("counter head changed to seq %d data %q after the batch was rejected; the first message committed alone", seq, data)
		}
	})

	t.Run("ConcurrentBatchesEachGetOneOrder", func(t *testing.T) {
		ctx := testCtx(t)
		s := newOrderCapSeam(t)

		const allocators = 8
		const maxAttempts = 200

		var wg sync.WaitGroup
		orders := make([]uint64, allocators)
		attempts := make([]int, allocators)
		errs := make([]error, allocators)
		start := make(chan struct{})

		for i := range allocators {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				for attempt := 1; attempt <= maxAttempts; attempt++ {
					order, ack, err := s.allocate(ctx, "gamma")
					if err != nil {
						errs[i] = err
						return
					}
					if ack.Error == nil {
						orders[i], attempts[i] = order, attempt
						return
					}
					// Lost the race: re-read the head and try again. This is the retry
					// the design relies on, and it must converge.
				}
				errs[i] = fmt.Errorf("allocator %d did not win an order in %d attempts", i, maxAttempts)
			}(i)
		}
		close(start)
		wg.Wait()

		for i, err := range errs {
			if err != nil {
				t.Fatalf("allocator %d: %v", i, err)
			}
		}
		seen := make(map[uint64]int, allocators)
		for i, o := range orders {
			if prev, dup := seen[o]; dup {
				t.Fatalf("order %d handed to allocators %d and %d: the counter fence does not serialize", o, prev, i)
			}
			seen[o] = i
		}
		for want := uint64(1); want <= allocators; want++ {
			if _, ok := seen[want]; !ok {
				t.Fatalf("order %d was never allocated; orders=%v (want exactly 1..%d)", want, orders, allocators)
			}
		}
		if got := s.msgCount(ctx, t); got != 2*allocators {
			t.Fatalf("stream holds %d msgs after %d successful allocations, want %d (a rejected batch left a partial write)", got, allocators, 2*allocators)
		}
		t.Logf("orders=%v attempts=%v", orders, attempts)
	})

	t.Run("DeliverLastPerSubjectBootstrapAndCatchUp", func(t *testing.T) {
		ctx := testCtx(t)
		s := newOrderCapSeam(t)

		const scopes = 3
		const perScope = 4
		for i := range scopes {
			scope := "s" + strconv.Itoa(i)
			for range perScope {
				if _, ack, err := s.allocate(ctx, scope); err != nil || ack.Error != nil {
					t.Fatalf("seed allocate scope %q: err=%v ack.Error=%s", scope, err, ack.Error)
				}
			}
		}
		info, err := s.stream.Info(ctx)
		if err != nil {
			t.Fatalf("StreamInfo: %v", err)
		}
		captured := info.State.LastSeq // (f): the sequence a live consumer must reach.

		// (e) DeliverLastPerSubject: exactly one message per distinct subject — the
		// current counter head for each scope, and every order-addressed record head.
		bootstrap, err := s.js.OrderedConsumer(ctx, orderCapStream, jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{orderCapSubjects},
			DeliverPolicy:  jetstream.DeliverLastPerSubjectPolicy,
		})
		if err != nil {
			t.Fatalf("OrderedConsumer(DeliverLastPerSubject): %v", err)
		}
		wantSubjects := scopes + scopes*perScope // one counter head per scope + one head per record subject
		iter, err := bootstrap.Messages()
		if err != nil {
			t.Fatalf("Messages: %v", err)
		}
		counters := map[string]string{}
		records := map[string]struct{}{}
		var lastStreamSeq uint64
		for range wantSubjects {
			msg, err := iter.Next()
			if err != nil {
				t.Fatalf("bootstrap Next after %d/%d subjects: %v", len(counters)+len(records), wantSubjects, err)
			}
			md, err := msg.Metadata()
			if err != nil {
				t.Fatalf("Metadata: %v", err)
			}
			if md.Sequence.Stream <= lastStreamSeq {
				t.Fatalf("bootstrap delivered stream seq %d after %d: not ordered", md.Sequence.Stream, lastStreamSeq)
			}
			lastStreamSeq = md.Sequence.Stream
			switch subj := msg.Subject(); {
			case len(subj) > len("ordercap.counter.") && subj[:len("ordercap.counter.")] == "ordercap.counter.":
				counters[subj] = string(msg.Data())
			default:
				records[subj] = struct{}{}
			}
		}
		iter.Stop()
		if len(counters) != scopes {
			t.Fatalf("bootstrap saw %d counter subjects, want %d (%v)", len(counters), scopes, counters)
		}
		for i := range scopes {
			subj := counterSubject("s" + strconv.Itoa(i))
			if got, want := counters[subj], strconv.Itoa(perScope); got != want {
				t.Fatalf("bootstrap counter %q = %q, want %q (last-per-subject did not deliver the head)", subj, got, want)
			}
		}
		if len(records) != scopes*perScope {
			t.Fatalf("bootstrap saw %d record subjects, want %d", len(records), scopes*perScope)
		}

		// (f) A fresh consumer from the beginning of the stream catches up to the
		// captured sequence, in ascending stream order, and then stays live: a batch
		// committed after it started is delivered too.
		live, err := s.js.OrderedConsumer(ctx, orderCapStream, jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{orderCapSubjects},
			DeliverPolicy:  jetstream.DeliverAllPolicy,
		})
		if err != nil {
			t.Fatalf("OrderedConsumer(DeliverAll): %v", err)
		}
		liveIter, err := live.Messages()
		if err != nil {
			t.Fatalf("Messages: %v", err)
		}
		defer liveIter.Stop()

		var prev uint64
		var seenCaptured bool
		for !seenCaptured {
			msg, err := liveIter.Next()
			if err != nil {
				t.Fatalf("catch-up Next at stream seq %d (captured %d): %v", prev, captured, err)
			}
			md, err := msg.Metadata()
			if err != nil {
				t.Fatalf("Metadata: %v", err)
			}
			if md.Sequence.Stream <= prev {
				t.Fatalf("catch-up delivered stream seq %d after %d: not ordered", md.Sequence.Stream, prev)
			}
			prev = md.Sequence.Stream
			if prev >= captured {
				seenCaptured = true
			}
		}
		if prev != captured {
			t.Fatalf("catch-up overshot: stopped at stream seq %d, captured %d", prev, captured)
		}

		// Live tail: commit one more batch and require the consumer to see it.
		order, ack, err := s.allocate(ctx, "s0")
		if err != nil || ack.Error != nil {
			t.Fatalf("post-capture allocate: err=%v ack.Error=%s", err, ack.Error)
		}
		wantRecord := recordSubject("s0", order)
		var sawRecord bool
		for range 2 {
			msg, err := liveIter.Next()
			if err != nil {
				t.Fatalf("live tail Next: %v", err)
			}
			if msg.Subject() == wantRecord {
				sawRecord = true
			}
		}
		if !sawRecord {
			t.Fatalf("live consumer did not deliver %q after the post-capture commit", wantRecord)
		}
	})
}
