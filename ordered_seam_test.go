package natsstore

import (
	"context"
	"errors"
	"slices"
	"testing"

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
	// The configuration the seam writes must be one the seam accepts.
	if err := verifyOrderedStreamConfig(spec, cfg); err != nil {
		t.Fatalf("the seam rejects its own configuration: %v", err)
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
	if _, ok := seam.cachedStream("OI_absent"); ok {
		t.Fatal("a fresh seam reported a cached stream")
	}
	seam.cacheStream("OI_present", nil)
	if _, ok := seam.cachedStream("OI_present"); !ok {
		t.Fatal("cacheStream did not record the stream handle")
	}
	// ensureStream short-circuits on a cached stream, so it must not touch the
	// nil jetstream context here.
	if err := seam.ensureStream(context.Background(), orderedStreamSpec{stream: "OI_present"}); err != nil {
		t.Fatalf("ensureStream on a cached stream: %v", err)
	}
	// Batch id generation is monotonic, which is what keeps two concurrent
	// batches on one connection from sharing an id.
	first := seam.batchID.Add(1)
	if second := seam.batchID.Add(1); second <= first {
		t.Fatalf("batch ids are not monotonic: %d then %d", first, second)
	}
}

func TestJetStreamOrderedSeamRejectsEmptyBatch(t *testing.T) {
	t.Parallel()
	seam := newJetStreamOrderedSeam(nil, nil)
	err := seam.publishBatch(context.Background(), nil)
	var rejected *OrderedBatchRejectedError
	if !errors.As(err, &rejected) {
		t.Fatalf("error = %v (%T), want *OrderedBatchRejectedError", err, err)
	}
}
