package natsstore

import (
	"context"
	"errors"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func testOrderedSpec(t *testing.T) orderedStreamSpec {
	t.Helper()
	stream, err := orderedStreamName("sessions")
	if err != nil {
		t.Fatalf("orderedStreamName: %v", err)
	}
	filter, err := orderedSubjectFilter("sessions")
	if err != nil {
		t.Fatalf("orderedSubjectFilter: %v", err)
	}
	return orderedStreamSpec{stream: stream, subjectFilter: filter}
}

// TestOrderedStreamConfig pins the stream properties the design's atomicity and
// compare-and-swap depend on. A silent change to any of them would break the
// store in ways only a live server would reveal.
func TestOrderedStreamConfig(t *testing.T) {
	t.Parallel()
	spec := testOrderedSpec(t)
	cfg := orderedStreamConfig(spec)
	if cfg.Name != spec.stream {
		t.Fatalf("Name = %q, want %q", cfg.Name, spec.stream)
	}
	if !slices.Equal(cfg.Subjects, []string{spec.subjectFilter}) {
		t.Fatalf("Subjects = %v, want [%q]", cfg.Subjects, spec.subjectFilter)
	}
	if !cfg.AllowAtomicPublish {
		t.Fatal("AllowAtomicPublish is false; the create batch cannot be atomic")
	}
	if cfg.MaxMsgsPerSubject != 1 {
		t.Fatalf("MaxMsgsPerSubject = %d, want 1", cfg.MaxMsgsPerSubject)
	}
	if cfg.Description != orderedStreamDescription {
		t.Fatalf("Description = %q, want the layout marker %q", cfg.Description, orderedStreamDescription)
	}
	if cfg.MaxMsgSize != orderedMaxMsgSize {
		t.Fatalf("MaxMsgSize = %d, want %d", cfg.MaxMsgSize, orderedMaxMsgSize)
	}
	if cfg.Retention != jetstream.LimitsPolicy {
		t.Fatalf("Retention = %v, want LimitsPolicy", cfg.Retention)
	}
	if cfg.MaxConsumers != -1 {
		t.Fatalf("MaxConsumers = %d, want -1 (unlimited materialized views)", cfg.MaxConsumers)
	}
	// The configuration the seam writes must be one the seam accepts.
	if err := verifyOrderedStreamConfig(spec, cfg); err != nil {
		t.Fatalf("the seam rejects its own configuration: %v", err)
	}
}

// TestJetStreamStreamConfigFieldInventory pins the exact nats.go v1.53.1
// surface reviewed by verifyOrderedStreamConfig. A dependency upgrade that adds
// a field must fail here until its ordered-invariant classification is explicit.
func TestJetStreamStreamConfigFieldInventory(t *testing.T) {
	t.Parallel()
	typ := reflect.TypeOf(jetstream.StreamConfig{})
	got := make([]string, typ.NumField())
	for i := range typ.NumField() {
		got[i] = typ.Field(i).Name
	}
	want := []string{
		"Name", "Description", "Subjects", "Retention", "MaxConsumers", "MaxMsgs", "MaxBytes",
		"Discard", "DiscardNewPerSubject", "MaxAge", "MaxMsgsPerSubject", "MaxMsgSize", "Storage",
		"Replicas", "NoAck", "Duplicates", "Placement", "Mirror", "Sources", "Sealed", "DenyDelete",
		"DenyPurge", "AllowRollup", "Compression", "FirstSeq", "SubjectTransform", "RePublish",
		"AllowDirect", "MirrorDirect", "ConsumerLimits", "Metadata", "Template", "AllowMsgTTL",
		"SubjectDeleteMarkerTTL", "AllowMsgCounter", "AllowAtomicPublish", "AllowMsgSchedules",
		"PersistMode", "AllowBatchPublish",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("jetstream.StreamConfig fields = %v, want reviewed v1.53.1 inventory %v", got, want)
	}
}

func TestVerifyOrderedStreamConfigRejectsDrift(t *testing.T) {
	t.Parallel()
	spec := testOrderedSpec(t)
	cases := []struct {
		name   string
		mutate func(*jetstream.StreamConfig)
	}{
		{name: "foreign layout", mutate: func(c *jetstream.StreamConfig) { c.Description = "something else" }},
		{name: "no description", mutate: func(c *jetstream.StreamConfig) { c.Description = "" }},
		{name: "atomic publish disabled", mutate: func(c *jetstream.StreamConfig) { c.AllowAtomicPublish = false }},
		{name: "history kept", mutate: func(c *jetstream.StreamConfig) { c.MaxMsgsPerSubject = 5 }},
		{name: "unlimited history", mutate: func(c *jetstream.StreamConfig) { c.MaxMsgsPerSubject = -1 }},
		{name: "extra subject", mutate: func(c *jetstream.StreamConfig) { c.Subjects = append(c.Subjects, "other.>") }},
		{name: "wrong subject", mutate: func(c *jetstream.StreamConfig) { c.Subjects = []string{"other.>"} }},
		// Every limit that could delete a record behind the store's back. Order is
		// never reused, so an expired identity subject reads absent and the next
		// Create reallocates an order that was already handed out.
		{name: "records expire", mutate: func(c *jetstream.StreamConfig) { c.MaxAge = time.Hour }},
		{name: "byte limit", mutate: func(c *jetstream.StreamConfig) { c.MaxBytes = 1 << 20 }},
		{name: "message limit", mutate: func(c *jetstream.StreamConfig) { c.MaxMsgs = 1000 }},
		{name: "discard new", mutate: func(c *jetstream.StreamConfig) { c.Discard = jetstream.DiscardNew }},
		{name: "work queue retention", mutate: func(c *jetstream.StreamConfig) { c.Retention = jetstream.WorkQueuePolicy }},
		{name: "interest retention", mutate: func(c *jetstream.StreamConfig) { c.Retention = jetstream.InterestPolicy }},
		{name: "memory storage", mutate: func(c *jetstream.StreamConfig) { c.Storage = jetstream.MemoryStorage }},
		{name: "message size below ordered ceiling", mutate: func(c *jetstream.StreamConfig) { c.MaxMsgSize = orderedMaxMsgSize - 1 }},
		{name: "consumer limit", mutate: func(c *jetstream.StreamConfig) { c.MaxConsumers = 1 }},
		{name: "acknowledgements disabled", mutate: func(c *jetstream.StreamConfig) { c.NoAck = true }},
		{name: "mirror", mutate: func(c *jetstream.StreamConfig) { c.Mirror = &jetstream.StreamSource{Name: "FOREIGN"} }},
		{name: "source", mutate: func(c *jetstream.StreamConfig) { c.Sources = []*jetstream.StreamSource{{Name: "FOREIGN"}} }},
		{name: "sealed", mutate: func(c *jetstream.StreamConfig) { c.Sealed = true }},
		{name: "rollup", mutate: func(c *jetstream.StreamConfig) { c.AllowRollup = true }},
		{name: "first sequence", mutate: func(c *jetstream.StreamConfig) { c.FirstSeq = 100 }},
		{name: "subject transform", mutate: func(c *jetstream.StreamConfig) {
			c.SubjectTransform = &jetstream.SubjectTransformConfig{Source: spec.subjectFilter, Destination: "redirect.>"}
		}},
		{name: "republish", mutate: func(c *jetstream.StreamConfig) { c.RePublish = &jetstream.RePublish{Destination: "audit.>"} }},
		{name: "message ttl", mutate: func(c *jetstream.StreamConfig) { c.AllowMsgTTL = true }},
		{name: "message counter", mutate: func(c *jetstream.StreamConfig) { c.AllowMsgCounter = true }},
		{name: "message schedules", mutate: func(c *jetstream.StreamConfig) { c.AllowMsgSchedules = true }},
		{name: "async persistence", mutate: func(c *jetstream.StreamConfig) { c.PersistMode = jetstream.AsyncPersistMode }},
		{name: "consumer inactivity limit below provider request", mutate: func(c *jetstream.StreamConfig) {
			c.ConsumerLimits.InactiveThreshold = orderedViewInactiveThreshold - time.Second
		}},
		{name: "subject delete marker ttl", mutate: func(c *jetstream.StreamConfig) { c.SubjectDeleteMarkerTTL = time.Minute }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := orderedStreamConfig(spec)
			tc.mutate(&cfg)
			err := verifyOrderedStreamConfig(spec, cfg)
			var configErr *OrderedStreamConfigError
			if !errors.As(err, &configErr) {
				t.Fatalf("error = %v (%T), want *OrderedStreamConfigError", err, err)
			}
			if configErr.Stream != spec.stream {
				t.Fatalf("Stream = %q, want %q", configErr.Stream, spec.stream)
			}
		})
	}
}

func TestVerifyOrderedStreamConfigAllowsSafeOperationalDrift(t *testing.T) {
	t.Parallel()
	spec := testOrderedSpec(t)
	for _, tc := range []struct {
		name   string
		mutate func(*jetstream.StreamConfig)
	}{
		{name: "more replicas", mutate: func(c *jetstream.StreamConfig) { c.Replicas = 3 }},
		{name: "larger message ceiling", mutate: func(c *jetstream.StreamConfig) { c.MaxMsgSize++ }},
		{name: "unlimited message ceiling", mutate: func(c *jetstream.StreamConfig) { c.MaxMsgSize = -1 }},
		{name: "duplicate tracking window", mutate: func(c *jetstream.StreamConfig) { c.Duplicates = time.Minute }},
		{name: "placement", mutate: func(c *jetstream.StreamConfig) { c.Placement = &jetstream.Placement{Cluster: "operator-choice"} }},
		{name: "deny delete", mutate: func(c *jetstream.StreamConfig) { c.DenyDelete = true }},
		{name: "deny purge", mutate: func(c *jetstream.StreamConfig) { c.DenyPurge = true }},
		{name: "compression", mutate: func(c *jetstream.StreamConfig) { c.Compression = jetstream.S2Compression }},
		{name: "direct reads", mutate: func(c *jetstream.StreamConfig) { c.AllowDirect = true }},
		{name: "mirror direct irrelevant without mirror", mutate: func(c *jetstream.StreamConfig) { c.MirrorDirect = true }},
		{name: "consumer defaults", mutate: func(c *jetstream.StreamConfig) {
			c.ConsumerLimits = jetstream.StreamConsumerLimits{InactiveThreshold: time.Minute, MaxAckPending: 100}
		}},
		{name: "metadata", mutate: func(c *jetstream.StreamConfig) { c.Metadata = map[string]string{"owner": "operator"} }},
		{name: "deprecated template marker", mutate: func(c *jetstream.StreamConfig) { c.Template = "legacy" }},
		{name: "batch publish extension", mutate: func(c *jetstream.StreamConfig) { c.AllowBatchPublish = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := orderedStreamConfig(spec)
			tc.mutate(&cfg)
			if err := verifyOrderedStreamConfig(spec, cfg); err != nil {
				t.Fatalf("verifyOrderedStreamConfig = %v, want accepted safe drift", err)
			}
		})
	}
}

func TestVerifyOrderedStreamConfigReportsDiscardDrift(t *testing.T) {
	t.Parallel()
	spec := testOrderedSpec(t)
	cfg := orderedStreamConfig(spec)
	cfg.Discard = jetstream.DiscardNew
	err := verifyOrderedStreamConfig(spec, cfg)
	var configErr *OrderedStreamConfigError
	if !errors.As(err, &configErr) {
		t.Fatalf("error = %v (%T), want *OrderedStreamConfigError", err, err)
	}
	for _, want := range []string{jetstream.DiscardNew.String(), jetstream.DiscardOld.String()} {
		if !strings.Contains(configErr.Reason, want) {
			t.Fatalf("Reason = %q, want actual and required policies including %q", configErr.Reason, want)
		}
	}
}

func TestOrderedErrorClassification(t *testing.T) {
	t.Parallel()

	t.Run("wrong last sequence codes", func(t *testing.T) {
		t.Parallel()
		for _, code := range []uint16{uint16(jetstream.JSErrCodeStreamWrongLastSequence), uint16(jetstream.JSErrCodeStreamWrongLastSequenceConstant)} {
			if !isOrderedWrongLastSeqCode(code) {
				t.Fatalf("isOrderedWrongLastSeqCode(%d) = false, want true", code)
			}
		}
		for _, code := range []uint16{0, uint16(jetstream.JSErrCodeStreamNotFound), uint16(jetstream.JSErrCodeMessageNotFound)} {
			if isOrderedWrongLastSeqCode(code) {
				t.Fatalf("isOrderedWrongLastSeqCode(%d) = true, want false", code)
			}
		}
	})

	t.Run("wrong last sequence errors", func(t *testing.T) {
		t.Parallel()
		wrong := &jetstream.APIError{ErrorCode: jetstream.JSErrCodeStreamWrongLastSequence, Code: 400}
		if !isOrderedWrongLastSeq(wrong) {
			t.Fatal("a wrong-last-sequence APIError was not classified as one")
		}
		if !isOrderedWrongLastSeq(errors.Join(errors.New("wrapped"), wrong)) {
			t.Fatal("a wrapped wrong-last-sequence APIError was not classified as one")
		}
		replicated := &jetstream.APIError{ErrorCode: jetstream.JSErrCodeStreamWrongLastSequenceConstant, Code: 400}
		if !isOrderedWrongLastSeq(replicated) {
			t.Fatal("the replicated-stream spelling was not classified as a CAS loss")
		}
		if isOrderedWrongLastSeq(&jetstream.APIError{ErrorCode: jetstream.JSErrCodeStreamNotFound}) {
			t.Fatal("stream-not-found was misclassified as a CAS loss")
		}
		if isOrderedWrongLastSeq(errors.New("plain")) {
			t.Fatal("a plain error was misclassified as a CAS loss")
		}
	})

	t.Run("ambiguity", func(t *testing.T) {
		t.Parallel()
		for _, err := range []error{
			context.DeadlineExceeded,
			nats.ErrTimeout,
			nats.ErrNoResponders,
			nats.ErrNoStreamResponse,
			jetstream.ErrNoStreamResponse,
		} {
			if !isOrderedAmbiguous(err) {
				t.Fatalf("isOrderedAmbiguous(%v) = false, want true", err)
			}
		}
		for _, err := range []error{
			errors.New("plain"),
			context.Canceled,
			&jetstream.APIError{ErrorCode: jetstream.JSErrCodeStreamWrongLastSequence},
		} {
			if isOrderedAmbiguous(err) {
				t.Fatalf("isOrderedAmbiguous(%v) = true, want false", err)
			}
		}
	})
}

func TestJetStreamOrderedSeamStreamCache(t *testing.T) {
	t.Parallel()
	seam := newJetStreamOrderedSeam(nil, nil)
	if _, ok := seam.cachedVerifiedStream("OI_absent"); ok {
		t.Fatal("a fresh seam reported a cached stream")
	}
	seam.cacheVerifiedStream("OI_present", nil)
	if _, ok := seam.cachedVerifiedStream("OI_present"); !ok {
		t.Fatal("cacheVerifiedStream did not record the stream handle")
	}
	// ensureStream short-circuits on a cached stream, so it must not touch the
	// nil jetstream context here.
	if err := seam.ensureStream(context.Background(), orderedStreamSpec{stream: "OI_present"}); err != nil {
		t.Fatalf("ensureStream on a cached stream: %v", err)
	}
}

// TestJetStreamOrderedSeamBatchIDsAreGloballyUnique pins the property the
// server's batch bookkeeping depends on. The server keys in-flight atomic-batch
// state by batch id WITHIN THE STREAM and records no client identity, so two
// seams — in one process or two — that emit the same id abort each other's
// staged messages, and an interleaving that satisfies the server's batch-sequence
// gap check commits one seam's message together with the other's. A per-seam
// counter alone cannot prevent that: it restarts at 1 everywhere.
func TestJetStreamOrderedSeamBatchIDsAreGloballyUnique(t *testing.T) {
	t.Parallel()

	const seams, perSeam = 8, 64
	ids := map[string]struct{}{}
	for range seams {
		seam := newJetStreamOrderedSeam(nil, nil)
		for range perSeam {
			id := seam.nextBatchID()
			if _, dup := ids[id]; dup {
				t.Fatalf("two seams emitted the batch id %q", id)
			}
			// The server refuses a longer id outright.
			if len(id) > orderedMaxBatchIDLen {
				t.Fatalf("batch id %q is %d bytes, over the server's %d byte ceiling", id, len(id), orderedMaxBatchIDLen)
			}
			if !strings.HasPrefix(id, orderedBatchIDPrefix) {
				t.Fatalf("batch id %q does not carry the %q marker", id, orderedBatchIDPrefix)
			}
			ids[id] = struct{}{}
		}
	}
	if len(ids) != seams*perSeam {
		t.Fatalf("got %d distinct ids from %d seams, want %d", len(ids), seams, seams*perSeam)
	}
	// The longest id this seam can ever emit must still fit.
	seam := newJetStreamOrderedSeam(nil, nil)
	seam.batchID.Store(math.MaxUint64 - 1)
	if id := seam.nextBatchID(); len(id) > orderedMaxBatchIDLen {
		t.Fatalf("the widest batch id %q is %d bytes, over the %d byte ceiling", id, len(id), orderedMaxBatchIDLen)
	}
}

func TestJetStreamOrderedSeamRejectsEmptyBatch(t *testing.T) {
	t.Parallel()
	seam := newJetStreamOrderedSeam(nil, nil)
	err := seam.publishBatch(context.Background(), "OI_x", nil)
	var rejected *OrderedBatchRejectedError
	if !errors.As(err, &rejected) {
		t.Fatalf("error = %v (%T), want *OrderedBatchRejectedError", err, err)
	}
}

// TestClassifyOrderedBatchAck pins the classification the whole ordered design
// turns on: a batch the server ACKNOWLEDGED never yields a definite failure,
// because some prefix of it may be durable.
func TestClassifyOrderedBatchAck(t *testing.T) {
	t.Parallel()
	const id = "ns-test-1"
	cases := []struct {
		name      string
		sent      uint64
		ack       orderedBatchAck
		wantErr   error
		wantTyped bool
	}{
		{name: "committed", sent: 2, ack: orderedBatchAck{BatchSize: 2}},
		{
			name:    "fence lost",
			sent:    2,
			ack:     orderedBatchAck{Error: &orderedAPIError{Code: 400, ErrCode: uint16(jetstream.JSErrCodeStreamWrongLastSequence)}},
			wantErr: errOrderedPrecondition,
		},
		{
			name:    "replicated fence lost",
			sent:    2,
			ack:     orderedBatchAck{Error: &orderedAPIError{Code: 400, ErrCode: uint16(jetstream.JSErrCodeStreamWrongLastSequenceConstant)}},
			wantErr: errOrderedPrecondition,
		},
		{
			name:      "rejected",
			sent:      2,
			ack:       orderedBatchAck{Error: &orderedAPIError{Code: 400, ErrCode: 10176, Description: "atomic publish batch is incomplete"}},
			wantTyped: true,
		},
		{name: "short commit", sent: 2, ack: orderedBatchAck{BatchSize: 1}, wantErr: errOrderedAmbiguous},
		{name: "long commit", sent: 2, ack: orderedBatchAck{BatchSize: 3}, wantErr: errOrderedAmbiguous},
		{name: "zero commit", sent: 2, ack: orderedBatchAck{BatchSize: 0}, wantErr: errOrderedAmbiguous},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := classifyOrderedBatchAck(id, tc.sent, tc.ack)
			switch {
			case tc.wantTyped:
				var rejected *OrderedBatchRejectedError
				if !errors.As(err, &rejected) {
					t.Fatalf("error = %v (%T), want *OrderedBatchRejectedError", err, err)
				}
				if rejected.BatchID != id || rejected.Description != tc.ack.Error.Description {
					t.Fatalf("rejected = %+v, want the batch id and the server description", rejected)
				}
				// A rejection must never be mistaken for either of the two outcomes
				// the store handles specially.
				if errors.Is(err, errOrderedAmbiguous) || errors.Is(err, errOrderedPrecondition) {
					t.Fatalf("a rejection was also classified as ambiguous or a fence loss: %v", err)
				}
			case tc.wantErr == nil:
				if err != nil {
					t.Fatalf("error = %v, want nil", err)
				}
			default:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("error = %v, want one wrapping %v", err, tc.wantErr)
				}
			}
			// An acknowledged batch is never reported as "rejected", whose contract
			// is that nothing committed.
			if tc.ack.Error == nil {
				var rejected *OrderedBatchRejectedError
				if errors.As(err, &rejected) {
					t.Fatalf("an acknowledged batch was reported as rejected: %v", err)
				}
			}
			if errors.Is(err, errOrderedAmbiguous) {
				var mismatch *OrderedBatchAckMismatchError
				if !errors.As(err, &mismatch) {
					t.Fatalf("ambiguous error %v carries no *OrderedBatchAckMismatchError", err)
				}
				if mismatch.Sent != tc.sent || mismatch.Committed != tc.ack.BatchSize {
					t.Fatalf("mismatch = %+v, want sent=%d committed=%d", mismatch, tc.sent, tc.ack.BatchSize)
				}
				if msg := mismatch.Error(); !strings.Contains(msg, "unknown") {
					t.Fatalf("Error() = %q, want it to say the outcome is unknown", msg)
				}
			}
		})
	}
}

// TestJetStreamOrderedSeamRejectsRepeatedSubject pins the F4.1 constraint that a
// batch may not name one subject twice: each member carries its own fence, so
// the second could only be evaluated against a sequence the first is about to
// change.
func TestJetStreamOrderedSeamRejectsRepeatedSubject(t *testing.T) {
	t.Parallel()
	seam := newJetStreamOrderedSeam(nil, nil)
	// A nil connection would panic on a publish, so reaching the guard is itself
	// the assertion that nothing was sent.
	err := seam.publishBatch(context.Background(), "OI_x", []orderedMsg{
		{subject: "oi.x.a"}, {subject: "oi.x.b"}, {subject: "oi.x.a"},
	})
	var subjects *OrderedBatchSubjectsError
	if !errors.As(err, &subjects) {
		t.Fatalf("error = %v (%T), want *OrderedBatchSubjectsError", err, err)
	}
	if subjects.Subject != "oi.x.a" {
		t.Fatalf("Subject = %q, want %q", subjects.Subject, "oi.x.a")
	}
}
