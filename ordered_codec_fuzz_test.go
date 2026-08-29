package natsstore

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/looprig/storage"
)

// fuzzSeedIDs are the identities whose canonical subjects and payloads seed the
// corpus. Both the subject and the payload are fuzzed arguments, so the fuzzer
// can vary either independently: no constant prefix stands between an arbitrary
// input and the JSON decoder.
func fuzzSeedIDs() []storage.OrderedID {
	return []storage.OrderedID{
		{Namespace: "ns", OrderingScope: "scope", StableKey: "key"},
		{Namespace: "a/b.c", OrderingScope: "s/1", StableKey: "K/ey.Ü"},
		{Namespace: "n", OrderingScope: "s", StableKey: storage.StableKey(bytes.Repeat([]byte("k"), storage.MaxStableKeyBytes))},
	}
}

// FuzzDecodeOrderedRecord drives the real decoder with arbitrary subject and
// payload bytes and asserts the invariants a caller relies on:
//
//   - it never panics;
//   - a record it accepts is a valid storage.OrderedRecord; and
//   - a record it accepts belongs to the subject it was read from, which is what
//     makes the hashed subject safe to trust.
func FuzzDecodeOrderedRecord(f *testing.F) {
	for _, id := range fuzzSeedIDs() {
		subj, err := orderedRecordSubject(id)
		if err != nil {
			f.Fatalf("orderedRecordSubject(%+v): %v", id, err)
		}
		rec := storage.OrderedRecord{
			ID:           id,
			RankingScope: "ranks",
			Revision:     1,
			Order:        1,
			Due:          storage.Due{State: storage.DueAt, UnixMillis: 12},
			Rank:         storage.Rank{Ranked: true, Value: -5},
			Value:        []byte("v"),
		}
		data, err := encodeOrderedRecord(rec)
		if err != nil {
			f.Fatalf("encodeOrderedRecord: %v", err)
		}
		f.Add(subj, data)
		// The same payload under a foreign subject: the identity-mismatch path.
		f.Add(subj+"x", data)
		f.Add("", data)
		// A tombstone, whose deleted invariants the decoder must also enforce.
		tomb := rec
		tomb.Deleted = true
		tomb.Rank = storage.Rank{}
		tomb.Due = storage.Due{State: storage.NotDue}
		tombData, err := encodeOrderedRecord(tomb)
		if err != nil {
			f.Fatalf("encodeOrderedRecord(tombstone): %v", err)
		}
		f.Add(subj, tombData)
		// Structural mutations that keep the input JSON-shaped.
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(data, &obj); err != nil {
			f.Fatalf("unmarshal seed payload: %v", err)
		}
		for key, raw := range map[string]json.RawMessage{
			"v":         json.RawMessage(`2`),
			"rev":       json.RawMessage(`0`),
			"order":     json.RawMessage(`0`),
			"due_state": json.RawMessage(`3`),
			"key":       json.RawMessage(`"forged"`),
			"ns":        json.RawMessage(`"other"`),
			"deleted":   json.RawMessage(`true`),
		} {
			mutated := make(map[string]json.RawMessage, len(obj)+1)
			for k, v := range obj {
				mutated[k] = v
			}
			mutated[key] = raw
			out, err := json.Marshal(mutated)
			if err != nil {
				f.Fatalf("marshal mutated payload: %v", err)
			}
			f.Add(subj, out)
		}
	}
	f.Add("OI.ns.r.scope.token", []byte("{}"))
	f.Add("", []byte(""))
	f.Add(".", []byte("{\"v\":1}"))

	f.Fuzz(func(t *testing.T, subject string, data []byte) {
		rec, err := decodeOrderedRecord(subject, data)
		if err != nil {
			return
		}
		if err := storage.ValidateOrderedRecord(rec); err != nil {
			t.Fatalf("decoder accepted an invalid record %+v: %v", rec, err)
		}
		got, err := orderedRecordSubject(rec.ID)
		if err != nil {
			t.Fatalf("accepted record whose identity has no subject: %v", err)
		}
		if got != subject {
			t.Fatalf("decoder accepted a record from subject %q whose identity hashes to %q", subject, got)
		}
		// Re-encoding an accepted record must round-trip through the decoder,
		// so acceptance is closed under the encoder's canonical form.
		reencoded, err := encodeOrderedRecord(rec)
		if err != nil {
			t.Fatalf("accepted record failed to re-encode: %v", err)
		}
		again, err := decodeOrderedRecord(subject, reencoded)
		if err != nil {
			t.Fatalf("re-encoded record failed to decode: %v", err)
		}
		if again.ID != rec.ID || again.Revision != rec.Revision || again.Order != rec.Order ||
			again.Due != rec.Due || again.Rank != rec.Rank || again.Deleted != rec.Deleted ||
			again.RankingScope != rec.RankingScope || !bytes.Equal(again.Value, rec.Value) {
			t.Fatalf("round trip changed the record: %+v then %+v", rec, again)
		}
	})
}
