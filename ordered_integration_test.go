//go:build integration

package natsstore

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/storage"
	"github.com/nats-io/nats.go/jetstream"
)

// newOrderedTestStore stands up a fresh embedded engine and returns an ordered
// store bound to the PRODUCTION seam, so these tests exercise the real stream
// provisioning, the real atomic-batch wire protocol, and the real fence
// classification rather than the fake used by the unit tests.
func newOrderedTestStore(t *testing.T) (*orderedStore, *jetStreamOrderedSeam) {
	t.Helper()
	eng := newOrderedTestEngine(t)
	seam := newOrderedTestSeam(t, eng)
	return newOrderedStore(seam), seam
}

// newOrderedTestEngine stands up a fresh embedded engine for one test.
func newOrderedTestEngine(t *testing.T) *Engine {
	t.Helper()
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", root)
	eng, err := OpenEngine(EngineOptions{
		DataDir:      filepath.Join(root, "looprig", "jetstream"),
		SyncInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("OpenEngine: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

// newOrderedTestSeam binds a NEW production seam to an existing engine. Two
// seams built this way share one connection, which is exactly the configuration
// that exposes a batch-id namespace shared between independent writers.
func newOrderedTestSeam(t *testing.T, eng *Engine) *jetStreamOrderedSeam {
	t.Helper()
	js, err := jetstream.New(eng.Conn())
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return newJetStreamOrderedSeam(eng.Conn(), js)
}

func orderedIntegrationID(key string) storage.OrderedID {
	return storage.OrderedID{Namespace: "sessions/live", OrderingScope: "tenant.a", StableKey: storage.StableKey(key)}
}

func TestOrderedStoreAgainstEmbeddedServer(t *testing.T) {
	store, seam := newOrderedTestStore(t)
	ctx := testCtx(t)
	id := orderedIntegrationID("Session/One.v2 Ünicode")

	rec, created, err := store.Create(ctx, id, "catalog/main", []byte("first"),
		storage.Rank{Ranked: true, Value: 10}, storage.Due{State: storage.DueAt, UnixMillis: 500})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !created || rec.Order != 1 || rec.Revision != 1 {
		t.Fatalf("Create returned created=%v order=%d revision=%d, want true/1/1", created, rec.Order, rec.Revision)
	}

	// The stream really carries the atomic-publish layout.
	spec, _, recordSubj, err := orderedLocation(id)
	if err != nil {
		t.Fatalf("orderedLocation: %v", err)
	}
	stream, err := seam.js.Stream(ctx, spec.stream)
	if err != nil {
		t.Fatalf("Stream(%q): %v", spec.stream, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if !info.Config.AllowAtomicPublish || info.Config.MaxMsgsPerSubject != 1 {
		t.Fatalf("provisioned config = %+v, want atomic publish and one message per subject", info.Config)
	}
	if info.State.Msgs != 2 {
		t.Fatalf("stream holds %d messages after one create, want 2 (counter + record)", info.State.Msgs)
	}

	// A second identity in the same order scope gets the next order.
	second, created, err := store.Create(ctx, orderedIntegrationID("Session/Two"), "catalog/main", nil, storage.Rank{}, storage.Due{})
	if err != nil {
		t.Fatalf("Create second: %v", err)
	}
	if !created || second.Order != 2 {
		t.Fatalf("second Create: created=%v order=%d, want true/2", created, second.Order)
	}

	// Create is idempotent by identity.
	dup, created, err := store.Create(ctx, id, "other/scope", []byte("ignored"), storage.Rank{}, storage.Due{})
	if err != nil {
		t.Fatalf("Create duplicate: %v", err)
	}
	if created || dup.Order != 1 || !bytes.Equal(dup.Value, []byte("first")) {
		t.Fatalf("duplicate Create returned created=%v %+v, want the original", created, dup)
	}

	// Update advances exactly one revision, under a real fence.
	updated, err := store.Update(ctx, id, 1, []byte("second"), storage.Rank{Ranked: true, Value: -3}, storage.Due{State: storage.NotDue})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Revision != 2 || updated.Order != 1 {
		t.Fatalf("Update returned revision=%d order=%d, want 2/1", updated.Revision, updated.Order)
	}
	// MaxMsgsPerSubject=1 means the update REPLACED the record message.
	info, err = stream.Info(ctx)
	if err != nil {
		t.Fatalf("StreamInfo after update: %v", err)
	}
	if info.State.Msgs != 3 {
		t.Fatalf("stream holds %d messages after 2 creates and 1 update, want 3", info.State.Msgs)
	}

	// A stale revision is a conflict and changes nothing.
	if _, err := store.Update(ctx, id, 1, []byte("stale"), storage.Rank{}, storage.Due{}); err == nil {
		t.Fatal("Update accepted a stale revision")
	} else {
		var conflict *storage.OrderedRevisionConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("error = %v (%T), want *storage.OrderedRevisionConflictError", err, err)
		}
		if conflict.ActualRevision != 2 {
			t.Fatalf("conflict.ActualRevision = %d, want 2", conflict.ActualRevision)
		}
	}
	got, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got.Value, []byte("second")) || got.Revision != 2 {
		t.Fatalf("Get returned %+v, want the second revision", got)
	}

	// Delete tombstones, and a retry with the pre-delete revision returns it.
	tomb, err := store.Delete(ctx, id, 2)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !tomb.Deleted || tomb.Revision != 3 || tomb.Order != 1 || !bytes.Equal(tomb.Value, []byte("second")) {
		t.Fatalf("Delete returned %+v, want a revision-3 tombstone preserving order and value", tomb)
	}
	again, err := store.Delete(ctx, id, 2)
	if err != nil {
		t.Fatalf("Delete retry: %v", err)
	}
	if again.Revision != tomb.Revision {
		t.Fatalf("Delete retry returned revision %d, want the original tombstone's %d", again.Revision, tomb.Revision)
	}
	if _, err := store.Update(ctx, id, 3, []byte("resurrect"), storage.Rank{}, storage.Due{}); err == nil {
		t.Fatal("Update resurrected a tombstone")
	}

	// The record subject never carries raw key bytes.
	if bytes.Contains([]byte(recordSubj), []byte("Session")) {
		t.Fatalf("record subject %q leaks the raw stable key", recordSubj)
	}
}

// TestOrderedStoreForgedPayloadFailsClosedOnServer is step 5 of the task against
// a real server: a payload committed directly to a record subject, whose own
// identity is a different one, must not be returned by Get.
func TestOrderedStoreForgedPayloadFailsClosedOnServer(t *testing.T) {
	store, seam := newOrderedTestStore(t)
	ctx := testCtx(t)
	victim := orderedIntegrationID("victim-key")
	attacker := orderedIntegrationID("attacker-key")

	if _, _, err := store.Create(ctx, victim, "catalog/main", []byte("honest"), storage.Rank{}, storage.Due{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	spec, _, victimSubj, err := orderedLocation(victim)
	if err != nil {
		t.Fatalf("orderedLocation: %v", err)
	}
	seq, _, err := seam.lastMsgForSubject(ctx, spec, victimSubj)
	if err != nil {
		t.Fatalf("lastMsgForSubject: %v", err)
	}

	// A record that is internally valid but belongs to a different identity,
	// published straight onto the victim's subject.
	forged, err := encodeOrderedRecord(storage.OrderedRecord{
		ID:           attacker,
		RankingScope: "catalog/main",
		Revision:     99,
		Order:        1,
		Value:        []byte("forged"),
	})
	if err != nil {
		t.Fatalf("encodeOrderedRecord: %v", err)
	}
	if err := seam.publish(ctx, spec.stream, orderedMsg{subject: victimSubj, data: forged, expectLastSeq: seq}); err != nil {
		t.Fatalf("publish forged record: %v", err)
	}

	got, err := store.Get(ctx, victim)
	if err == nil {
		t.Fatalf("Get returned a forged record: %+v", got)
	}
	var mismatch *OrderedIdentityMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("error = %v (%T), want *OrderedIdentityMismatchError", err, err)
	}
	// Every other entry point reads through the same guard.
	if _, err := store.Update(ctx, victim, 99, []byte("x"), storage.Rank{}, storage.Due{}); !errors.As(err, &mismatch) {
		t.Fatalf("Update error = %v (%T), want *OrderedIdentityMismatchError", err, err)
	}
	if _, err := store.Delete(ctx, victim, 99); !errors.As(err, &mismatch) {
		t.Fatalf("Delete error = %v (%T), want *OrderedIdentityMismatchError", err, err)
	}
	if _, _, err := store.Create(ctx, victim, "catalog/main", nil, storage.Rank{}, storage.Due{}); !errors.As(err, &mismatch) {
		t.Fatalf("Create error = %v (%T), want *OrderedIdentityMismatchError", err, err)
	}
}

// TestOrderedStoreConcurrentCreates proves the counter fence serializes real
// concurrent allocators: each identity gets exactly one distinct order.
func TestOrderedStoreConcurrentCreates(t *testing.T) {
	store, _ := newOrderedTestStore(t)
	ctx := testCtx(t)

	const creators = 8
	var wg sync.WaitGroup
	orders := make([]uint64, creators)
	errs := make([]error, creators)
	start := make(chan struct{})
	for i := range creators {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			id := orderedIntegrationID("concurrent-" + strconv.Itoa(i))
			rec, created, err := store.Create(ctx, id, "catalog/main", []byte(strconv.Itoa(i)), storage.Rank{}, storage.Due{})
			if err != nil {
				errs[i] = err
				return
			}
			if !created {
				errs[i] = errors.New("created=false for a fresh identity")
				return
			}
			orders[i] = rec.Order
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("creator %d: %v", i, err)
		}
	}
	seen := map[uint64]int{}
	for i, order := range orders {
		if prev, dup := seen[order]; dup {
			t.Fatalf("order %d handed to creators %d and %d: the counter fence does not serialize", order, prev, i)
		}
		seen[order] = i
	}
	for want := uint64(1); want <= creators; want++ {
		if _, ok := seen[want]; !ok {
			t.Fatalf("order %d was never allocated; orders=%v", want, orders)
		}
	}
}

// TestOrderedStoreValueFloor proves the storage 1 MiB value floor round-trips
// through the real stream, whose per-message ceiling must leave room for the
// JSON envelope and base64 expansion.
func TestOrderedStoreValueFloor(t *testing.T) {
	store, _ := newOrderedTestStore(t)
	ctx := testCtx(t)
	id := orderedIntegrationID("big")
	value := bytes.Repeat([]byte{0xC3}, storage.MaxOrderedValueBytes)

	if _, _, err := store.Create(ctx, id, "catalog/main", value, storage.Rank{}, storage.Due{}); err != nil {
		t.Fatalf("Create with a %d byte value: %v", len(value), err)
	}
	got, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got.Value, value) {
		t.Fatalf("round-tripped value differs (got %d bytes, want %d)", len(got.Value), len(value))
	}
}

// TestOrderedStoreRejectsForeignStream proves the lazy provisioning check fails
// closed when a stream already occupies the namespace's name under another
// layout.
func TestOrderedStoreRejectsForeignStream(t *testing.T) {
	store, seam := newOrderedTestStore(t)
	ctx := testCtx(t)
	id := orderedIntegrationID("k")
	spec, _, _, err := orderedLocation(id)
	if err != nil {
		t.Fatalf("orderedLocation: %v", err)
	}
	cfg := orderedStreamConfig(spec)
	cfg.Description = "someone else's stream"
	cfg.AllowAtomicPublish = false
	if _, err := seam.js.CreateStream(ctx, cfg); err != nil {
		t.Fatalf("CreateStream(foreign): %v", err)
	}
	_, _, err = store.Create(ctx, id, "catalog/main", nil, storage.Rank{}, storage.Due{})
	var configErr *OrderedStreamConfigError
	if !errors.As(err, &configErr) {
		t.Fatalf("error = %v (%T), want *OrderedStreamConfigError", err, err)
	}
	if configErr.Stream != spec.stream {
		t.Fatalf("Stream = %q, want %q", configErr.Stream, spec.stream)
	}
}

// TestOrderedStoreReadBeforeMutationStillRejectsUnsafeStream reproduces the
// cache-poisoning sequence: a read resolves an existing stream before a later
// mutation. No read may turn a foreign or expiring handle into a trusted cache
// entry, and Update/Delete must inherit the same layout gate as Create.
func TestOrderedStoreReadBeforeMutationStillRejectsUnsafeStream(t *testing.T) {
	t.Run("Get absent then Create rejects expiring stream", func(t *testing.T) {
		store, seam := newOrderedTestStore(t)
		ctx := testCtx(t)
		id := orderedIntegrationID("read-before-create")
		spec, _, _, err := orderedLocation(id)
		if err != nil {
			t.Fatalf("orderedLocation: %v", err)
		}
		cfg := orderedStreamConfig(spec)
		cfg.MaxAge = time.Hour
		stream, err := seam.js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("CreateStream(expiring): %v", err)
		}
		_, getErr := store.Get(ctx, id)
		var configErr *OrderedStreamConfigError
		if !errors.As(getErr, &configErr) {
			t.Fatalf("Get(absent) = %T %v, want *OrderedStreamConfigError", getErr, getErr)
		}
		if _, ok := seam.cachedVerifiedStream(spec.stream); ok {
			t.Fatal("Get cached a rejected expiring stream")
		}
		_, created, err := store.Create(ctx, id, "catalog/main", []byte("unsafe"), storage.Rank{}, storage.Due{})
		if created || !errors.As(err, &configErr) {
			t.Fatalf("Create after Get = created %v, err %T %v; want false, *OrderedStreamConfigError", created, err, err)
		}
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatalf("StreamInfo: %v", err)
		}
		if info.State.Msgs != 0 {
			t.Fatalf("unsafe stream holds %d messages after rejected Create, want 0", info.State.Msgs)
		}
	})

	t.Run("Get existing then Update rejects foreign layout", func(t *testing.T) {
		store, seam := newOrderedTestStore(t)
		ctx := testCtx(t)
		id := orderedIntegrationID("read-before-update")
		spec, _, subject, err := orderedLocation(id)
		if err != nil {
			t.Fatalf("orderedLocation: %v", err)
		}
		cfg := orderedStreamConfig(spec)
		cfg.Description = "foreign-layout"
		if _, err := seam.js.CreateStream(ctx, cfg); err != nil {
			t.Fatalf("CreateStream(foreign): %v", err)
		}
		seedUnsafeOrderedRecord(t, ctx, seam, subject, id)
		got, getErr := store.Get(ctx, id)
		var configErr *OrderedStreamConfigError
		if !errors.As(getErr, &configErr) {
			t.Fatalf("Get(existing) = (%+v, %T %v), want zero record and *OrderedStreamConfigError", got, getErr, getErr)
		}
		if !reflect.DeepEqual(got, storage.OrderedRecord{}) {
			t.Fatalf("Get(existing) returned unsafe record %+v", got)
		}
		if _, ok := seam.cachedVerifiedStream(spec.stream); ok {
			t.Fatal("Get cached a rejected foreign-layout stream")
		}
		_, err = store.Update(ctx, id, 1, []byte("mutated"), storage.Rank{}, storage.Due{})
		if !errors.As(err, &configErr) {
			t.Fatalf("Update after Get = %T %v, want *OrderedStreamConfigError", err, err)
		}
		assertUnsafeRecordUnchanged(t, ctx, seam, spec.stream, subject, 1, false)
	})

	t.Run("Get existing then Delete rejects history-retaining stream", func(t *testing.T) {
		store, seam := newOrderedTestStore(t)
		ctx := testCtx(t)
		id := orderedIntegrationID("read-before-delete")
		spec, _, subject, err := orderedLocation(id)
		if err != nil {
			t.Fatalf("orderedLocation: %v", err)
		}
		cfg := orderedStreamConfig(spec)
		cfg.MaxMsgsPerSubject = 2
		if _, err := seam.js.CreateStream(ctx, cfg); err != nil {
			t.Fatalf("CreateStream(history-retaining): %v", err)
		}
		seedUnsafeOrderedRecord(t, ctx, seam, subject, id)
		got, getErr := store.Get(ctx, id)
		var configErr *OrderedStreamConfigError
		if !errors.As(getErr, &configErr) {
			t.Fatalf("Get(existing) = (%+v, %T %v), want zero record and *OrderedStreamConfigError", got, getErr, getErr)
		}
		if !reflect.DeepEqual(got, storage.OrderedRecord{}) {
			t.Fatalf("Get(existing) returned unsafe record %+v", got)
		}
		if _, ok := seam.cachedVerifiedStream(spec.stream); ok {
			t.Fatal("Get cached a rejected history-retaining stream")
		}
		_, err = store.Delete(ctx, id, 1)
		if !errors.As(err, &configErr) {
			t.Fatalf("Delete after Get = %T %v, want *OrderedStreamConfigError", err, err)
		}
		assertUnsafeRecordUnchanged(t, ctx, seam, spec.stream, subject, 1, false)
	})
}

func TestOrderedStoreReadCachesOnlyVerifiedStream(t *testing.T) {
	store, seam := newOrderedTestStore(t)
	ctx := testCtx(t)
	id := orderedIntegrationID("verified-read-cache")
	spec, _, _, err := orderedLocation(id)
	if err != nil {
		t.Fatalf("orderedLocation: %v", err)
	}
	if _, err := seam.js.CreateStream(ctx, orderedStreamConfig(spec)); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	_, err = store.Get(ctx, id)
	var notFound *storage.OrderedRecordNotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("Get(absent) = %T %v, want *storage.OrderedRecordNotFoundError", err, err)
	}
	if _, ok := seam.cachedVerifiedStream(spec.stream); !ok {
		t.Fatal("Get did not cache the successfully verified stream")
	}
}

func TestOrderedStoreReadBeforeCreateRejectsLoadBearingStreamDrift(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*jetstream.StreamConfig)
	}{
		{name: "work queue retention", mutate: func(c *jetstream.StreamConfig) { c.Retention = jetstream.WorkQueuePolicy }},
		{name: "memory storage", mutate: func(c *jetstream.StreamConfig) { c.Storage = jetstream.MemoryStorage }},
		{name: "undersized messages", mutate: func(c *jetstream.StreamConfig) { c.MaxMsgSize = orderedMaxMsgSize - 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, seam := newOrderedTestStore(t)
			ctx := testCtx(t)
			id := orderedIntegrationID("read-before-create-" + strings.ReplaceAll(tc.name, " ", "-"))
			spec, _, _, err := orderedLocation(id)
			if err != nil {
				t.Fatalf("orderedLocation: %v", err)
			}
			cfg := orderedStreamConfig(spec)
			tc.mutate(&cfg)
			stream, err := seam.js.CreateStream(ctx, cfg)
			if err != nil {
				t.Fatalf("CreateStream(%s): %v", tc.name, err)
			}

			_, getErr := store.Get(ctx, id)
			var configErr *OrderedStreamConfigError
			if !errors.As(getErr, &configErr) {
				t.Fatalf("Get(absent) = %T %v, want *OrderedStreamConfigError", getErr, getErr)
			}
			if _, ok := seam.cachedVerifiedStream(spec.stream); ok {
				t.Fatal("Get cached a stream with rejected load-bearing drift")
			}
			_, created, createErr := store.Create(ctx, id, "catalog/main", []byte("unsafe"), storage.Rank{}, storage.Due{})
			if created || !errors.As(createErr, &configErr) {
				t.Fatalf("Create after Get = created %v, err %T %v; want false, *OrderedStreamConfigError", created, createErr, createErr)
			}
			info, err := stream.Info(ctx)
			if err != nil {
				t.Fatalf("StreamInfo: %v", err)
			}
			if info.State.Msgs != 0 {
				t.Fatalf("rejected stream holds %d messages, want 0", info.State.Msgs)
			}
		})
	}
}

func TestOrderedStoreRejectsUnsafeZeroDefaultStreamFeatures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(orderedStreamSpec, *jetstream.StreamConfig)
	}{
		{name: "subject transform", mutate: func(spec orderedStreamSpec, c *jetstream.StreamConfig) {
			c.SubjectTransform = &jetstream.SubjectTransformConfig{Source: spec.subjectFilter, Destination: "redirect.>"}
		}},
		{name: "no acknowledgements", mutate: func(_ orderedStreamSpec, c *jetstream.StreamConfig) { c.NoAck = true }},
		{name: "nonzero first sequence", mutate: func(_ orderedStreamSpec, c *jetstream.StreamConfig) { c.FirstSeq = 100 }},
		{name: "restrictive consumer inactivity", mutate: func(_ orderedStreamSpec, c *jetstream.StreamConfig) {
			c.ConsumerLimits.InactiveThreshold = orderedViewInactiveThreshold - time.Second
		}},
		{name: "subject delete marker ttl", mutate: func(_ orderedStreamSpec, c *jetstream.StreamConfig) {
			c.SubjectDeleteMarkerTTL = time.Minute
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, seam := newOrderedTestStore(t)
			ctx := testCtx(t)
			id := orderedIntegrationID("unsafe-zero-default-" + strings.ReplaceAll(tc.name, " ", "-"))
			spec, _, subject, err := orderedLocation(id)
			if err != nil {
				t.Fatalf("orderedLocation: %v", err)
			}
			cfg := orderedStreamConfig(spec)
			tc.mutate(spec, &cfg)
			stream, err := seam.js.CreateStream(ctx, cfg)
			if err != nil {
				t.Fatalf("CreateStream(%s): %v", tc.name, err)
			}
			info, err := stream.Info(ctx)
			if err != nil {
				t.Fatalf("StreamInfo: %v", err)
			}
			if tc.name == "subject transform" && info.Config.SubjectTransform == nil {
				t.Fatal("server did not preserve SubjectTransform")
			}
			if tc.name == "no acknowledgements" && !info.Config.NoAck {
				t.Fatal("server did not preserve NoAck")
			}
			if tc.name == "nonzero first sequence" && info.Config.FirstSeq != 100 {
				t.Fatalf("server FirstSeq = %d, want 100", info.Config.FirstSeq)
			}
			if tc.name == "restrictive consumer inactivity" && info.Config.ConsumerLimits.InactiveThreshold != orderedViewInactiveThreshold-time.Second {
				t.Fatalf("server consumer inactive threshold = %v, want %v", info.Config.ConsumerLimits.InactiveThreshold, orderedViewInactiveThreshold-time.Second)
			}
			if tc.name == "subject delete marker ttl" {
				if info.Config.SubjectDeleteMarkerTTL != time.Minute {
					t.Fatalf("server subject delete marker TTL = %v, want %v", info.Config.SubjectDeleteMarkerTTL, time.Minute)
				}
				if !info.Config.AllowMsgTTL || !info.Config.AllowRollup {
					t.Fatalf("server normalized marker TTL to AllowMsgTTL=%v AllowRollup=%v, want both true", info.Config.AllowMsgTTL, info.Config.AllowRollup)
				}
			}
			if tc.name == "restrictive consumer inactivity" {
				seedUnsafeOrderedRecord(t, ctx, seam, subject, id)
				info, err = stream.Info(ctx)
				if err != nil {
					t.Fatalf("StreamInfo after seed: %v", err)
				}
			}
			initialMsgs := info.State.Msgs
			if tc.name == "nonzero first sequence" {
				queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				defer cancel()
				_, queryErr := store.ListDue(queryCtx, id.Namespace, 100, "", 10)
				var configErr *OrderedStreamConfigError
				if !errors.As(queryErr, &configErr) {
					t.Fatalf("ListDue(empty stream with FirstSeq) = %T %v, want *OrderedStreamConfigError without waiting for watermark", queryErr, queryErr)
				}
			}
			if tc.name == "restrictive consumer inactivity" {
				queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				defer cancel()
				_, queryErr := store.ListDue(queryCtx, id.Namespace, 100, "", 10)
				var configErr *OrderedStreamConfigError
				if !errors.As(queryErr, &configErr) {
					t.Fatalf("ListDue(restrictive consumer inactivity) = %T %v, want *OrderedStreamConfigError", queryErr, queryErr)
				}
			}

			_, getErr := store.Get(ctx, id)
			var configErr *OrderedStreamConfigError
			if !errors.As(getErr, &configErr) {
				t.Fatalf("Get = %T %v, want *OrderedStreamConfigError", getErr, getErr)
			}
			if _, ok := seam.cachedVerifiedStream(spec.stream); ok {
				t.Fatal("Get cached a stream with rejected zero/default drift")
			}
			_, created, createErr := store.Create(ctx, id, "catalog/main", []byte("unsafe"), storage.Rank{}, storage.Due{})
			if created || !errors.As(createErr, &configErr) {
				t.Fatalf("Create after Get = created %v, err %T %v; want false, *OrderedStreamConfigError", created, createErr, createErr)
			}
			info, err = stream.Info(ctx)
			if err != nil {
				t.Fatalf("StreamInfo after rejection: %v", err)
			}
			if info.State.Msgs != initialMsgs {
				t.Fatalf("rejected stream holds %d messages, want unchanged %d", info.State.Msgs, initialMsgs)
			}
		})
	}
}

func seedUnsafeOrderedRecord(t *testing.T, ctx context.Context, seam *jetStreamOrderedSeam, subject string, id storage.OrderedID) {
	t.Helper()
	payload, err := encodeOrderedRecord(storage.OrderedRecord{
		ID: id, RankingScope: "catalog/main", Revision: 1, Order: 1, Value: []byte("original"),
	})
	if err != nil {
		t.Fatalf("encodeOrderedRecord: %v", err)
	}
	if _, err := seam.js.Publish(ctx, subject, payload); err != nil {
		t.Fatalf("seed record: %v", err)
	}
}

func assertUnsafeRecordUnchanged(t *testing.T, ctx context.Context, seam *jetStreamOrderedSeam, streamName, subject string, revision uint64, deleted bool) {
	t.Helper()
	stream, err := seam.js.Stream(ctx, streamName)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	msg, err := stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		t.Fatalf("GetLastMsgForSubject: %v", err)
	}
	record, err := decodeOrderedRecord(subject, msg.Data)
	if err != nil {
		t.Fatalf("decodeOrderedRecord: %v", err)
	}
	if record.Revision != revision || record.Deleted != deleted || !bytes.Equal(record.Value, []byte("original")) {
		t.Fatalf("unsafe record mutated to %+v", record)
	}
}

// TestOrderedStoreConcurrentCreatesAcrossSeams is the regression test for
// process-local batch ids. TestOrderedStoreConcurrentCreates drives a SINGLE
// seam, whose atomic counter alone keeps its batch ids distinct; it is
// structurally blind to two independent seams emitting the same id. The server
// keys atomic-batch state by batch id within the stream and carries no client
// identity, so two writers sharing an id abort each other's staged messages —
// and an interleaving that happens to satisfy the server's batch-sequence gap
// check commits a MIXED batch: one writer's counter with the other's record.
func TestOrderedStoreConcurrentCreatesAcrossSeams(t *testing.T) {
	eng := newOrderedTestEngine(t)
	ctx := testCtx(t)
	stores := []*orderedStore{
		newOrderedStore(newOrderedTestSeam(t, eng)),
		newOrderedStore(newOrderedTestSeam(t, eng)),
	}

	const perStore = 100
	type outcome struct {
		order   uint64
		created bool
		value   string
		err     error
	}
	results := make([][]outcome, len(stores))
	var wg sync.WaitGroup
	start := make(chan struct{})
	for si, store := range stores {
		results[si] = make([]outcome, perStore)
		for i := range perStore {
			wg.Add(1)
			go func(si, i int) {
				defer wg.Done()
				<-start
				key := "seam" + strconv.Itoa(si) + "-" + strconv.Itoa(i)
				id := orderedIntegrationID(key)
				rec, created, err := store.Create(ctx, id, "catalog/main", []byte(key), storage.Rank{}, storage.Due{})
				results[si][i] = outcome{order: rec.Order, created: created, value: string(rec.Value), err: err}
			}(si, i)
		}
	}
	close(start)
	wg.Wait()

	var failures int
	var first error
	seen := map[uint64]string{}
	for si := range results {
		for i, got := range results[si] {
			key := "seam" + strconv.Itoa(si) + "-" + strconv.Itoa(i)
			if got.err != nil {
				failures++
				if first == nil {
					first = got.err
				}
				continue
			}
			if !got.created {
				t.Errorf("%s: created=false for a fresh identity", key)
			}
			// A mixed batch commits one writer's counter with another's record,
			// so the value read back would not be the one this caller wrote.
			if got.value != key {
				t.Errorf("%s: record value = %q, want %q (a mixed batch committed)", key, got.value, key)
			}
			if prev, dup := seen[got.order]; dup {
				t.Errorf("order %d handed to both %s and %s", got.order, prev, key)
			}
			seen[got.order] = key
		}
	}
	if failures != 0 {
		t.Fatalf("%d of %d creates failed; first error: %v", failures, len(stores)*perStore, first)
	}
	for want := uint64(1); want <= uint64(len(stores)*perStore); want++ {
		if _, ok := seen[want]; !ok {
			t.Fatalf("order %d was never allocated", want)
		}
	}
}

// TestOrderedStoreCreateContentionScales measures the retry loop under the
// contention levels the bound must survive. Losers that retry in lockstep with
// no delay re-collide immediately, so the cost per round is O(N) round trips and
// the 64-attempt budget is reachable well below the "pathological" contention
// the bound was documented to guard.
func TestOrderedStoreCreateContentionScales(t *testing.T) {
	for _, creators := range []int{128, 200} {
		t.Run(strconv.Itoa(creators)+" creators", func(t *testing.T) {
			store, _ := newOrderedTestStore(t)
			ctx := testCtx(t)
			errs := make([]error, creators)
			orders := make([]uint64, creators)
			var wg sync.WaitGroup
			start := make(chan struct{})
			began := time.Now()
			for i := range creators {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					id := orderedIntegrationID("scale-" + strconv.Itoa(creators) + "-" + strconv.Itoa(i))
					rec, _, err := store.Create(ctx, id, "catalog/main", []byte(strconv.Itoa(i)), storage.Rank{}, storage.Due{})
					errs[i] = err
					orders[i] = rec.Order
				}(i)
			}
			close(start)
			wg.Wait()
			elapsed := time.Since(began)

			var failed int
			var first error
			for _, err := range errs {
				if err != nil {
					failed++
					if first == nil {
						first = err
					}
				}
			}
			t.Logf("%d creators: %d failures in %s", creators, failed, elapsed.Round(time.Millisecond))
			if failed != 0 {
				t.Fatalf("%d of %d creators failed; first error: %v", failed, creators, first)
			}
			seen := map[uint64]int{}
			for i, order := range orders {
				if prev, dup := seen[order]; dup {
					t.Fatalf("order %d handed to creators %d and %d", order, prev, i)
				}
				seen[order] = i
			}
		})
	}
}

// TestOrderedBatchFenceAndRollbackOnServer covers, against the real server, the
// two batch properties Create's correctness rests on and that the unit tests can
// only assert against the fake: the record subject's expectLastSeq=0 fence
// really rejects an existing identity, and a rejected batch really commits
// NOTHING — in particular the counter member does not land. Create itself cannot
// reach this path, because a duplicate identity short-circuits at the identity
// read long before the batch, so without this test the behaviour is unexercised
// against a server.
func TestOrderedBatchFenceAndRollbackOnServer(t *testing.T) {
	store, seam := newOrderedTestStore(t)
	ctx := testCtx(t)
	id := orderedIntegrationID("fenced")
	spec, counterSubj, recordSubj, err := orderedLocation(id)
	if err != nil {
		t.Fatalf("orderedLocation: %v", err)
	}
	rec, _, err := store.Create(ctx, id, "catalog/main", []byte("first"), storage.Rank{}, storage.Due{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	counterSeq, counterData, err := seam.lastMsgForSubject(ctx, spec, counterSubj)
	if err != nil {
		t.Fatalf("lastMsgForSubject(counter): %v", err)
	}
	recordSeq, _, err := seam.lastMsgForSubject(ctx, spec, recordSubj)
	if err != nil {
		t.Fatalf("lastMsgForSubject(record): %v", err)
	}

	// A well-formed allocation batch whose ONLY defect is that the identity is
	// already taken: the counter member's fence is correct, the record member's
	// "must not exist" fence is not.
	payload, err := encodeOrderedRecord(storage.OrderedRecord{
		ID: id, RankingScope: "catalog/main", Revision: 1, Order: rec.Order + 1, Value: []byte("second"),
	})
	if err != nil {
		t.Fatalf("encodeOrderedRecord: %v", err)
	}
	err = seam.publishBatch(ctx, spec.stream, []orderedMsg{
		{subject: counterSubj, data: encodeOrderedCounter(rec.Order + 1), expectLastSeq: counterSeq},
		{subject: recordSubj, data: payload, expectLastSeq: 0},
	})
	if !errors.Is(err, errOrderedPrecondition) {
		t.Fatalf("error = %v, want a fence loss; the record subject's expectLastSeq=0 did not fence", err)
	}

	// All or nothing: the counter member must not have committed either.
	gotCounterSeq, gotCounterData, err := seam.lastMsgForSubject(ctx, spec, counterSubj)
	if err != nil {
		t.Fatalf("lastMsgForSubject(counter) after rollback: %v", err)
	}
	if gotCounterSeq != counterSeq || !bytes.Equal(gotCounterData, counterData) {
		t.Fatalf("the counter moved to seq %d %q after a rejected batch; want seq %d %q",
			gotCounterSeq, gotCounterData, counterSeq, counterData)
	}
	gotRecordSeq, _, err := seam.lastMsgForSubject(ctx, spec, recordSubj)
	if err != nil {
		t.Fatalf("lastMsgForSubject(record) after rollback: %v", err)
	}
	if gotRecordSeq != recordSeq {
		t.Fatalf("the record moved to seq %d after a rejected batch, want %d", gotRecordSeq, recordSeq)
	}
	// The store still reads the original record.
	got, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Order != rec.Order || !bytes.Equal(got.Value, []byte("first")) {
		t.Fatalf("Get returned %+v after a rejected batch, want the original", got)
	}
}

// TestOrderedStoreAdoptsOnlyANonExpiringStream proves the provisioning check
// refuses a stream that carries the right layout marker but a retention limit
// that would delete records behind the store's back. Order is never reused, so
// an expired identity subject reads absent and the next Create would reallocate
// an order that has already been handed out.
func TestOrderedStoreAdoptsOnlyANonExpiringStream(t *testing.T) {
	store, seam := newOrderedTestStore(t)
	ctx := testCtx(t)
	id := orderedIntegrationID("k")
	spec, _, _, err := orderedLocation(id)
	if err != nil {
		t.Fatalf("orderedLocation: %v", err)
	}
	cfg := orderedStreamConfig(spec)
	cfg.MaxAge = time.Hour
	if _, err := seam.js.CreateStream(ctx, cfg); err != nil {
		t.Fatalf("CreateStream(expiring): %v", err)
	}
	_, _, err = store.Create(ctx, id, "catalog/main", nil, storage.Rank{}, storage.Due{})
	var configErr *OrderedStreamConfigError
	if !errors.As(err, &configErr) {
		t.Fatalf("error = %v (%T), want *OrderedStreamConfigError", err, err)
	}
}
