package natsstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"io/fs"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestObjectChunkReaderStreamsAndVerifies(t *testing.T) {
	t.Parallel()
	data := []byte("first-second")
	digest := sha256.Sum256(data)
	messages := newFakeObjectMessages(
		fakeObjectMsg("$O.B.C.N", "OBJ_B", 1, 10, 1, []byte("first-")),
		fakeObjectMsg("$O.B.C.N", "OBJ_B", 2, 12, 0, []byte("second")),
	)
	manager := &fakeObjectConsumerManager{consumer: &fakeObjectConsumer{messages: messages}}
	seam := newJetStreamObjectSeam(&fakeInfoObjectStore{infos: map[string]*jetstream.ObjectInfo{
		"blob": objectInfo("B", "blob", "N", 2, uint64(len(data)), digest[:]),
	}}, manager)
	reader, err := seam.get(context.Background(), "blob")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("ReadAll = %q, %v; want %q", got, err, data)
	}
	if manager.stream != "OBJ_B" || manager.cfg.FilterSubjects[0] != "$O.B.C.N" || manager.cfg.DeliverPolicy != jetstream.DeliverAllPolicy || manager.cfg.InactiveThreshold != blobReaderCloseBound || manager.messageOpts != 1 || manager.pullMaxMessages != 1 {
		t.Fatalf("consumer = stream %q config %#v opts %d pull max %d", manager.stream, manager.cfg, manager.messageOpts, manager.pullMaxMessages)
	}
	if calls := messages.stopCalls.Load(); calls != 1 {
		t.Fatalf("Messages.Stop calls = %d, want 1", calls)
	}
}

func TestObjectChunkReaderRejectsIntegrityViolations(t *testing.T) {
	t.Parallel()
	data := []byte("payload")
	digest := sha256.Sum256(data)
	cases := []struct {
		name string
		info *jetstream.ObjectInfo
		msg  jetstream.Msg
	}{
		{name: "wildcard NUID", info: objectInfo("B", "blob", "*", 1, 7, digest[:])},
		{name: "malformed digest", info: &jetstream.ObjectInfo{Bucket: "B", NUID: "N", Chunks: 1, Size: 7, Digest: "bad", ObjectMeta: jetstream.ObjectMeta{Name: "blob"}}},
		{name: "wrong subject", info: objectInfo("B", "blob", "N", 1, 7, digest[:]), msg: fakeObjectMsg("$O.B.C.X", "OBJ_B", 1, 1, 0, data)},
		{name: "wrong stream", info: objectInfo("B", "blob", "N", 1, 7, digest[:]), msg: fakeObjectMsg("$O.B.C.N", "OBJ_X", 1, 1, 0, data)},
		{name: "wrong consumer order", info: objectInfo("B", "blob", "N", 1, 7, digest[:]), msg: fakeObjectMsg("$O.B.C.N", "OBJ_B", 2, 1, 0, data)},
		{name: "unexpected pending", info: objectInfo("B", "blob", "N", 1, 7, digest[:]), msg: fakeObjectMsg("$O.B.C.N", "OBJ_B", 1, 1, 1, data)},
		{name: "oversize", info: objectInfo("B", "blob", "N", 1, 6, digest[:]), msg: fakeObjectMsg("$O.B.C.N", "OBJ_B", 1, 1, 0, data)},
		{name: "undersize", info: objectInfo("B", "blob", "N", 1, 8, digest[:]), msg: fakeObjectMsg("$O.B.C.N", "OBJ_B", 1, 1, 0, data)},
		{name: "digest mismatch", info: objectInfo("B", "blob", "N", 1, 7, make([]byte, sha256.Size)), msg: fakeObjectMsg("$O.B.C.N", "OBJ_B", 1, 1, 0, data)},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			manager := &fakeObjectConsumerManager{consumer: &fakeObjectConsumer{messages: newFakeObjectMessages(tt.msg)}}
			reader, err := newJetStreamObjectSeam(&fakeInfoObjectStore{infos: map[string]*jetstream.ObjectInfo{"blob": tt.info}}, manager).get(context.Background(), "blob")
			if err == nil {
				_, err = io.ReadAll(reader)
			}
			if err == nil {
				t.Fatal("Get/ReadAll succeeded; want integrity failure")
			}
		})
	}
}

func TestObjectChunkReaderEmptyAndLinks(t *testing.T) {
	t.Parallel()
	emptyDigest := sha256.Sum256(nil)
	target := &fakeInfoObjectStore{infos: map[string]*jetstream.ObjectInfo{"target": objectInfo("T", "target", "", 0, 0, emptyDigest[:])}}
	root := &fakeInfoObjectStore{infos: map[string]*jetstream.ObjectInfo{
		"link": {Bucket: "R", ObjectMeta: jetstream.ObjectMeta{Name: "link", Opts: &jetstream.ObjectMetaOptions{Link: &jetstream.ObjectLink{Bucket: "T", Name: "target"}}}},
	}}
	manager := &fakeObjectConsumerManager{stores: map[string]jetstream.ObjectStore{"T": target}}
	reader, err := newJetStreamObjectSeam(root, manager).get(context.Background(), "link")
	if err != nil {
		t.Fatal(err)
	}
	if data, readErr := io.ReadAll(reader); readErr != nil || len(data) != 0 {
		t.Fatalf("empty linked object = %q, %v", data, readErr)
	}
	if manager.orderedCalls != 0 {
		t.Fatalf("empty object created %d consumers", manager.orderedCalls)
	}
	root.infos["bucket-link"] = &jetstream.ObjectInfo{Bucket: "R", ObjectMeta: jetstream.ObjectMeta{Name: "bucket-link", Opts: &jetstream.ObjectMetaOptions{Link: &jetstream.ObjectLink{Bucket: "T"}}}}
	if _, err := newJetStreamObjectSeam(root, manager).get(context.Background(), "bucket-link"); !errors.Is(err, jetstream.ErrCantGetBucket) {
		t.Fatalf("bucket link = %v", err)
	}
	root.infos["cycle"] = &jetstream.ObjectInfo{Bucket: "R", ObjectMeta: jetstream.ObjectMeta{Name: "cycle", Opts: &jetstream.ObjectMetaOptions{Link: &jetstream.ObjectLink{Bucket: "R", Name: "cycle"}}}}
	if _, err := newJetStreamObjectSeam(root, manager).get(context.Background(), "cycle"); !errors.Is(err, errObjectIntegrity) {
		t.Fatalf("cycle = %v", err)
	}
}

func TestObjectChunkReaderCallerCancelAndCloseAreStable(t *testing.T) {
	t.Parallel()
	digest := sha256.Sum256([]byte("x"))
	messages := newFakeObjectMessages()
	manager := &fakeObjectConsumerManager{consumer: &fakeObjectConsumer{messages: messages}}
	ctx, cancel := context.WithCancel(context.Background())
	reader, err := newJetStreamObjectSeam(&fakeInfoObjectStore{infos: map[string]*jetstream.ObjectInfo{"blob": objectInfo("B", "blob", "N", 1, 1, digest[:])}}, manager).get(ctx, "blob")
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() { _, readErr := reader.Read(make([]byte, 1)); readDone <- readErr }()
	select {
	case <-messages.nextStarted:
	case <-time.After(time.Second):
		t.Fatal("Read did not enter Messages.Next")
	}
	cancel()
	select {
	case readErr := <-readDone:
		if !errors.Is(readErr, context.Canceled) {
			t.Fatalf("Read after caller cancel = %v", readErr)
		}
	case <-time.After(time.Second):
		t.Fatal("caller cancellation did not release Read")
	}
	for i := 0; i < 3; i++ {
		if closeErr := reader.Close(); closeErr != nil {
			t.Fatalf("Close %d = %v", i, closeErr)
		}
	}
	if calls := messages.stopCalls.Load(); calls != 1 {
		t.Fatalf("Messages.Stop calls = %d, want 1", calls)
	}
	if n, readErr := reader.Read(make([]byte, 1)); n != 0 || !errors.Is(readErr, fs.ErrClosed) {
		t.Fatalf("post-Close Read = %d, %v", n, readErr)
	}
}

func TestObjectChunkReaderCloseReleasesActiveReadExactlyOnce(t *testing.T) {
	t.Parallel()
	digest := sha256.Sum256([]byte("x"))
	messages := newFakeObjectMessages()
	manager := &fakeObjectConsumerManager{consumer: &fakeObjectConsumer{messages: messages}}
	reader, err := newJetStreamObjectSeam(&fakeInfoObjectStore{infos: map[string]*jetstream.ObjectInfo{"blob": objectInfo("B", "blob", "N", 1, 1, digest[:])}}, manager).get(context.Background(), "blob")
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() { _, readErr := reader.Read(make([]byte, 1)); readDone <- readErr }()
	select {
	case <-messages.nextStarted:
	case <-time.After(time.Second):
		t.Fatal("Read did not enter Messages.Next")
	}
	closeResults := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func() { closeResults <- reader.Close() }()
	}
	select {
	case readErr := <-readDone:
		if !errors.Is(readErr, fs.ErrClosed) {
			t.Fatalf("active Read after Close = %v", readErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not promptly release active Read")
	}
	for i := 0; i < 3; i++ {
		select {
		case closeErr := <-closeResults:
			if closeErr != nil {
				t.Fatalf("Close = %v", closeErr)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent Close did not return")
		}
	}
	if calls := messages.stopCalls.Load(); calls != 1 {
		t.Fatalf("Messages.Stop calls = %d, want 1", calls)
	}
}

func TestObjectChunkReaderRejectsNilConsumerBoundaries(t *testing.T) {
	t.Parallel()
	digest := sha256.Sum256([]byte("x"))
	store := &fakeInfoObjectStore{infos: map[string]*jetstream.ObjectInfo{"blob": objectInfo("B", "blob", "N", 1, 1, digest[:])}}
	for _, tt := range []struct {
		name     string
		consumer jetstream.Consumer
	}{
		{name: "literal nil consumer"},
		{name: "typed nil consumer", consumer: (*nilMessagesConsumer)(nil)},
		{name: "literal nil messages", consumer: &nilMessagesConsumer{}},
		{name: "typed nil messages", consumer: &nilMessagesConsumer{messages: (*fakeObjectMessages)(nil)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := newJetStreamObjectSeam(store, &fixedConsumerManager{consumer: tt.consumer}).get(context.Background(), "blob"); !errors.Is(err, errObjectIntegrity) {
				t.Fatalf("Get = %v, want integrity error", err)
			}
		})
	}
}

func objectInfo(bucket, name, nuid string, chunks uint32, size uint64, digest []byte) *jetstream.ObjectInfo {
	return &jetstream.ObjectInfo{Bucket: bucket, NUID: nuid, Chunks: chunks, Size: size, Digest: "SHA-256=" + base64.URLEncoding.EncodeToString(digest), ObjectMeta: jetstream.ObjectMeta{Name: name}}
}

type fakeInfoObjectStore struct {
	jetstream.ObjectStore
	infos map[string]*jetstream.ObjectInfo
}

type fixedConsumerManager struct{ consumer jetstream.Consumer }

func (*fixedConsumerManager) ObjectStore(context.Context, string) (jetstream.ObjectStore, error) {
	return nil, jetstream.ErrBucketNotFound
}
func (m *fixedConsumerManager) OrderedConsumer(context.Context, string, jetstream.OrderedConsumerConfig) (jetstream.Consumer, error) {
	return m.consumer, nil
}

type nilMessagesConsumer struct {
	jetstream.Consumer
	messages jetstream.MessagesContext
}

func (c *nilMessagesConsumer) Messages(...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	return c.messages, nil
}

func (s *fakeInfoObjectStore) GetInfo(_ context.Context, name string, _ ...jetstream.GetObjectInfoOpt) (*jetstream.ObjectInfo, error) {
	info := s.infos[name]
	if info == nil {
		return nil, jetstream.ErrObjectNotFound
	}
	return info, nil
}

type fakeObjectConsumerManager struct {
	stores                                     map[string]jetstream.ObjectStore
	consumer                                   *fakeObjectConsumer
	stream                                     string
	cfg                                        jetstream.OrderedConsumerConfig
	messageOpts, pullMaxMessages, orderedCalls int
}

func (m *fakeObjectConsumerManager) ObjectStore(_ context.Context, bucket string) (jetstream.ObjectStore, error) {
	store := m.stores[bucket]
	if store == nil {
		return nil, jetstream.ErrBucketNotFound
	}
	return store, nil
}
func (m *fakeObjectConsumerManager) OrderedConsumer(_ context.Context, stream string, cfg jetstream.OrderedConsumerConfig) (jetstream.Consumer, error) {
	m.stream, m.cfg = stream, cfg
	m.orderedCalls++
	if m.consumer == nil {
		m.consumer = &fakeObjectConsumer{messages: newFakeObjectMessages()}
	}
	m.consumer.manager = m
	return m.consumer, nil
}

type fakeObjectConsumer struct {
	jetstream.Consumer
	messages *fakeObjectMessages
	manager  *fakeObjectConsumerManager
}

func (c *fakeObjectConsumer) CachedInfo() *jetstream.ConsumerInfo {
	return &jetstream.ConsumerInfo{Name: "fake"}
}
func (c *fakeObjectConsumer) Messages(opts ...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	c.manager.messageOpts = len(opts)
	if len(opts) == 1 {
		if max, ok := opts[0].(jetstream.PullMaxMessages); ok {
			c.manager.pullMaxMessages = int(max)
		}
	}
	return c.messages, nil
}

type fakeObjectMessages struct {
	msgs        chan jetstream.Msg
	stopped     chan struct{}
	stopOnce    sync.Once
	stopCalls   atomic.Int32
	nextStarted chan struct{}
	nextOnce    sync.Once
}

func newFakeObjectMessages(msgs ...jetstream.Msg) *fakeObjectMessages {
	m := &fakeObjectMessages{msgs: make(chan jetstream.Msg, len(msgs)), stopped: make(chan struct{}), nextStarted: make(chan struct{})}
	for _, msg := range msgs {
		if msg != nil {
			m.msgs <- msg
		}
	}
	return m
}
func (m *fakeObjectMessages) Next(...jetstream.NextOpt) (jetstream.Msg, error) {
	m.nextOnce.Do(func() { close(m.nextStarted) })
	select {
	case msg := <-m.msgs:
		return msg, nil
	case <-m.stopped:
		return nil, jetstream.ErrMsgIteratorClosed
	}
}
func (m *fakeObjectMessages) Stop()  { m.stopCalls.Add(1); m.stopOnce.Do(func() { close(m.stopped) }) }
func (m *fakeObjectMessages) Drain() { m.Stop() }

type fakeChunkMsg struct {
	jetstream.Msg
	subject, stream                 string
	consumerSeq, streamSeq, pending uint64
	data                            []byte
}

func fakeObjectMsg(subject, stream string, consumerSeq, streamSeq, pending uint64, data []byte) jetstream.Msg {
	return &fakeChunkMsg{subject: subject, stream: stream, consumerSeq: consumerSeq, streamSeq: streamSeq, pending: pending, data: data}
}
func (m *fakeChunkMsg) Subject() string { return m.subject }
func (m *fakeChunkMsg) Data() []byte    { return m.data }
func (m *fakeChunkMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Stream: m.stream, Sequence: jetstream.SequencePair{Consumer: m.consumerSeq, Stream: m.streamSeq}, NumPending: m.pending}, nil
}
