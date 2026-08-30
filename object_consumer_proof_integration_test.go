//go:build integration

package natsstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// TestPlainObjectConsumerCapabilityProof pins the public-API capability required
// by the provider-owned blob reader. It intentionally uses JetStream APIs
// directly to prove that a plain pull consumer has no OrderedConsumer reset path.
func TestPlainObjectConsumerCapabilityProof(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", root)
	eng, err := OpenEngine(EngineOptions{DataDir: filepath.Join(root, "jetstream"), SyncInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	js, err := jetstream.New(eng.Conn())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const bucket = "blob_reader_proof"
	const chunkSize = 128 * 1024
	obj, err := js.CreateObjectStore(ctx, objectStoreConfig(bucket))
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("capability-proof"), 64*1024)
	info, err := obj.Put(ctx, jetstream.ObjectMeta{
		Name: "blobs/proof",
		Opts: &jetstream.ObjectMetaOptions{ChunkSize: chunkSize},
	}, bytes.NewReader(want))
	if err != nil {
		t.Fatal(err)
	}
	streamName := objectStreamPrefix + bucket
	chunkSubject := objectChunkPrefix + bucket + objectChunkMarker + info.NUID
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		t.Fatal(err)
	}
	streamInfo := stream.CachedInfo()
	if streamInfo == nil || streamInfo.Config.Name != streamName || !containsString(streamInfo.Config.Subjects, objectChunkPrefix+bucket+objectChunkMarker+">") {
		t.Fatalf("ObjectStore layout = %#v; want stream %q with chunk wildcard", streamInfo, streamName)
	}

	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		AckPolicy:         jetstream.AckNonePolicy,
		FilterSubject:     chunkSubject,
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		InactiveThreshold: time.Second,
		MemoryStorage:     true,
		MaxRequestBatch:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	consumerInfo := consumer.CachedInfo()
	if consumerInfo == nil || consumerInfo.Config.AckPolicy != jetstream.AckNonePolicy || consumerInfo.Config.DeliverPolicy != jetstream.DeliverAllPolicy || consumerInfo.Config.FilterSubject != chunkSubject || !consumerInfo.Config.MemoryStorage || consumerInfo.Config.InactiveThreshold != time.Second || consumerInfo.Config.MaxRequestBatch != 1 {
		t.Fatalf("plain consumer config = %#v", consumerInfo)
	}
	consumerName := consumerInfo.Name
	messages, err := consumer.Messages(jetstream.PullMaxMessages(1))
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	var got bytes.Buffer
	var previousStreamSeq uint64
	for chunk := uint32(1); chunk <= info.Chunks; chunk++ {
		msg, nextErr := messages.Next(jetstream.NextMaxWait(5 * time.Second))
		if nextErr != nil {
			t.Fatalf("chunk %d: %v", chunk, nextErr)
		}
		meta, metaErr := msg.Metadata()
		if metaErr != nil {
			t.Fatal(metaErr)
		}
		if msg.Subject() != chunkSubject || meta.Stream != streamName || meta.Sequence.Stream <= previousStreamSeq || meta.NumPending != uint64(info.Chunks-chunk) {
			t.Fatalf("chunk %d metadata = subject %q stream %q consumer seq %d stream seq %d pending %d", chunk, msg.Subject(), meta.Stream, meta.Sequence.Consumer, meta.Sequence.Stream, meta.NumPending)
		}
		previousStreamSeq = meta.Sequence.Stream
		got.Write(msg.Data())
		time.Sleep(750 * time.Millisecond)
	}
	if elapsed := time.Since(started); elapsed <= 5*time.Second {
		t.Fatalf("progressive read lasted %v, want >5s", elapsed)
	}
	if !bytes.Equal(got.Bytes(), want) || sha256.Sum256(got.Bytes()) != sha256.Sum256(want) {
		t.Fatal("progressive plain-consumer stream did not preserve bytes/digest")
	}
	messages.Stop()
	assertConsumerCleaned(t, ctx, stream, consumerName, 4*time.Second)

	blocked, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		AckPolicy:         jetstream.AckNonePolicy,
		FilterSubject:     objectChunkPrefix + bucket + objectChunkMarker + "MISSING",
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		InactiveThreshold: time.Second,
		MemoryStorage:     true,
		MaxRequestBatch:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	blockedName := blocked.CachedInfo().Name
	blockedMessages, err := blocked.Messages(jetstream.PullMaxMessages(1))
	if err != nil {
		t.Fatal(err)
	}
	nextDone := make(chan error, 1)
	go func() {
		_, nextErr := blockedMessages.Next(jetstream.NextMaxWait(5 * time.Second))
		nextDone <- nextErr
	}()
	select {
	case nextErr := <-nextDone:
		t.Fatalf("Next returned before Stop: %v", nextErr)
	case <-time.After(100 * time.Millisecond):
	}
	stoppedAt := time.Now()
	blockedMessages.Stop()
	select {
	case nextErr := <-nextDone:
		if !errors.Is(nextErr, jetstream.ErrMsgIteratorClosed) {
			t.Fatalf("blocked Next after Stop = %v", nextErr)
		}
		if elapsed := time.Since(stoppedAt); elapsed >= time.Second {
			t.Fatalf("Stop released blocked Next in %v, want <1s", elapsed)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("Stop did not release blocked Next within provider bound")
	}
	assertConsumerCleaned(t, ctx, stream, blockedName, 4*time.Second)

	if _, err := messages.Next(jetstream.NextMaxWait(time.Millisecond)); !errors.Is(err, jetstream.ErrMsgIteratorClosed) && !errors.Is(err, io.EOF) {
		t.Fatalf("Next after Stop = %v", err)
	}
}

func assertConsumerCleaned(t *testing.T, ctx context.Context, stream jetstream.Stream, name string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		_, err := stream.Consumer(ctx, name)
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			return
		}
		if err != nil {
			t.Fatalf("Consumer(%q): %v", name, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("consumer %q not cleaned within %v", name, within)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
