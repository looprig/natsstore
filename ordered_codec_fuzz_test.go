package natsstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
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

// minFuzzCursorEchoBytes is the shortest token for which "the error does not
// contain the token" is a meaningful assertion rather than a coincidence.
const minFuzzCursorEchoBytes = 16

// FuzzDecodeOrderedCursor drives the continuation-cursor decoder with arbitrary
// bytes. A cursor is the only value in this package that arrives from outside
// with no fence, no hash, and no signature behind it, so the properties asserted
// here are the whole of its safety:
//
//   - it never panics, whatever the bytes;
//   - it never accepts a token for the wrong cursor kind;
//   - every rejection carries one of the contract's four fail-closed rules, for
//     the kind that was asked for; and
//   - it never echoes the token back through the error, which is what keeps an
//     opaque cursor opaque.
//
// The corpus is seeded with real tokens so the fuzzer starts from inside the
// grammar and mutates outwards, rather than spending its budget proving that
// random bytes are not base64.
func FuzzDecodeOrderedCursor(f *testing.F) {
	seeds := []orderedCursorPayload{
		{Kind: string(storage.RankedCursorKind), Namespace: "ns", RankingScope: "rs", Rank: 7, StableKey: "key", OrderingScope: "os"},
		{Kind: string(storage.DueCursorKind), Namespace: "a/b.c", DueBound: -1, DueAt: 1 << 40, StableKey: "K/ey.Ü", OrderingScope: "s/1"},
	}
	for _, payload := range seeds {
		token, err := encodeOrderedCursor(payload)
		if err != nil {
			f.Fatalf("encodeOrderedCursor(%+v): %v", payload, err)
		}
		f.Add(token)
	}
	f.Add("")
	f.Add("oi1.")
	f.Add(orderedMalformedCursorToken(storage.RankedCursorKind))
	f.Add(orderedUnknownVersionCursorToken(storage.DueCursorKind))

	// Both kinds are checked inside one fuzz body: a testing.F accepts exactly
	// one Fuzz call, and running the two kinds over the same input is also what
	// exposes a token that both kinds would accept.
	f.Fuzz(func(t *testing.T, token string) {
		for _, kind := range []storage.OrderedCursorKind{storage.RankedCursorKind, storage.DueCursorKind} {
			payload, err := decodeOrderedCursorPayload(kind, token)
			if err == nil {
				if payload.Kind != string(kind) {
					t.Fatalf("decode(%s) accepted a %q cursor", kind, payload.Kind)
				}
				// An accepted token must round-trip to itself, which is what
				// makes a resumed position the one that was issued.
				again, encodeErr := encodeOrderedCursor(payload)
				if encodeErr != nil {
					t.Fatalf("re-encoding an accepted cursor failed: %v", encodeErr)
				}
				if _, decodeErr := decodeOrderedCursorPayload(kind, again); decodeErr != nil {
					t.Fatalf("re-encoded cursor was rejected: %v", decodeErr)
				}
				return
			}
			var invalid *storage.InvalidOrderedCursorError
			if !errors.As(err, &invalid) {
				t.Fatalf("decode(%s) rejected with %T %v, want *InvalidOrderedCursorError", kind, err, err)
			}
			if invalid.Kind != kind {
				t.Fatalf("decode(%s) rejection reported kind %q", kind, invalid.Kind)
			}
			switch invalid.Rule {
			case storage.OrderedCursorMalformed, storage.OrderedCursorUnknownVersion,
				storage.OrderedCursorWrongKind, storage.OrderedCursorQueryMismatch:
			default:
				t.Fatalf("decode(%s) rejection carried unclassified rule %v", kind, invalid.Rule)
			}
			// Only tokens long enough to be distinctive are checked: a
			// one-byte token is a substring of the error's own prose by
			// coincidence, which is why storetest imposes the same minimum on
			// the probe tokens a provider supplies.
			if len(token) >= minFuzzCursorEchoBytes && strings.Contains(err.Error(), token) {
				t.Fatalf("decode(%s) echoed the rejected token back through its error", kind)
			}
		}
	})
}
