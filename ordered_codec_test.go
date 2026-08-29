package natsstore

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/looprig/storage"
)

// testOrderedID is a valid identity used across the codec tests.
func testOrderedID() storage.OrderedID {
	return storage.OrderedID{Namespace: "ns", OrderingScope: "scope", StableKey: "Key/With.Dots and Ünicode"}
}

// testOrderedRecord is a valid live record over testOrderedID.
func testOrderedRecord() storage.OrderedRecord {
	return storage.OrderedRecord{
		ID:           testOrderedID(),
		RankingScope: "ranks/main",
		Revision:     3,
		Order:        7,
		Due:          storage.Due{State: storage.DueAt, UnixMillis: -1700000000123},
		Rank:         storage.Rank{Ranked: true, Value: math.MinInt64},
		Value:        []byte("payload"),
	}
}

func mustRecordSubject(t *testing.T, id storage.OrderedID) string {
	t.Helper()
	subj, err := orderedRecordSubject(id)
	if err != nil {
		t.Fatalf("orderedRecordSubject(%+v): %v", id, err)
	}
	return subj
}

func TestOrderedIdentityTokenDistinguishesComponents(t *testing.T) {
	t.Parallel()
	// Every pair below differs only in where a component boundary falls. A hash
	// preimage that concatenated the three components without length framing
	// would map both members of a pair onto the same token.
	cases := []struct {
		name string
		a, b storage.OrderedID
	}{
		{
			name: "namespace/scope boundary",
			a:    storage.OrderedID{Namespace: "a", OrderingScope: "bc", StableKey: "d"},
			b:    storage.OrderedID{Namespace: "ab", OrderingScope: "c", StableKey: "d"},
		},
		{
			name: "scope/key boundary",
			a:    storage.OrderedID{Namespace: "n", OrderingScope: "ab", StableKey: "c"},
			b:    storage.OrderedID{Namespace: "n", OrderingScope: "a", StableKey: "bc"},
		},
		{
			name: "key differs by one byte",
			a:    storage.OrderedID{Namespace: "n", OrderingScope: "s", StableKey: "key-a"},
			b:    storage.OrderedID{Namespace: "n", OrderingScope: "s", StableKey: "key-b"},
		},
		{
			name: "key differs by trailing separator",
			a:    storage.OrderedID{Namespace: "n", OrderingScope: "s", StableKey: "k"},
			b:    storage.OrderedID{Namespace: "n", OrderingScope: "s", StableKey: "k\x00"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got, want := orderedIdentityToken(tc.a), orderedIdentityToken(tc.b); got == want {
				t.Fatalf("identity token collision between %+v and %+v: both %q", tc.a, tc.b, got)
			}
		})
	}
}

func TestOrderedIdentityTokenIsStableAndSubjectSafe(t *testing.T) {
	t.Parallel()
	id := testOrderedID()
	first := orderedIdentityToken(id)
	if second := orderedIdentityToken(id); first != second {
		t.Fatalf("identity token is not deterministic: %q then %q", first, second)
	}
	if want := base64.RawURLEncoding.EncodedLen(32); len(first) != want {
		t.Fatalf("identity token length = %d, want %d", len(first), want)
	}
	for i := 0; i < len(first); i++ {
		c := first[i]
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_'
		if !ok {
			t.Fatalf("identity token %q contains byte %q, which is not subject-safe", first, string(c))
		}
	}
	// The raw stable key must never appear in the token.
	if strings.Contains(first, "Key") || strings.Contains(first, "Dots") {
		t.Fatalf("identity token %q leaks raw stable key bytes", first)
	}
}

func TestOrderedSubjectsAndStreamName(t *testing.T) {
	t.Parallel()
	id := storage.OrderedID{Namespace: "a/b.c", OrderingScope: "s/1", StableKey: "k"}

	stream, err := orderedStreamName(id.Namespace)
	if err != nil {
		t.Fatalf("orderedStreamName: %v", err)
	}
	if want := "OI_a_sb_dc"; stream != want {
		t.Fatalf("stream = %q, want %q", stream, want)
	}

	filter, err := orderedSubjectFilter(id.Namespace)
	if err != nil {
		t.Fatalf("orderedSubjectFilter: %v", err)
	}
	if want := "OI.a_sb_dc.>"; filter != want {
		t.Fatalf("subject filter = %q, want %q", filter, want)
	}

	counter, err := orderedCounterSubject(id.Namespace, id.OrderingScope)
	if err != nil {
		t.Fatalf("orderedCounterSubject: %v", err)
	}
	if want := "OI.a_sb_dc.c.s_s1"; counter != want {
		t.Fatalf("counter subject = %q, want %q", counter, want)
	}

	record := mustRecordSubject(t, id)
	if want := "OI.a_sb_dc.r.s_s1." + orderedIdentityToken(id); record != want {
		t.Fatalf("record subject = %q, want %q", record, want)
	}
	// Both subjects must fall under the namespace's own stream filter and must
	// not collide with each other.
	prefix := strings.TrimSuffix(filter, ">")
	if !strings.HasPrefix(counter, prefix) || !strings.HasPrefix(record, prefix) {
		t.Fatalf("subjects %q/%q are not under filter %q", counter, record, filter)
	}
	if counter == record {
		t.Fatalf("counter and record subjects are identical: %q", counter)
	}
	// Distinct namespaces get disjoint subject spaces.
	otherFilter, err := orderedSubjectFilter("a/b/c")
	if err != nil {
		t.Fatalf("orderedSubjectFilter(other): %v", err)
	}
	if otherFilter == filter {
		t.Fatalf("namespaces %q and %q share subject filter %q", id.Namespace, "a/b/c", filter)
	}
}

func TestOrderedSubjectsRejectInvalidIdentity(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		id   storage.OrderedID
	}{
		{name: "empty namespace", id: storage.OrderedID{Namespace: "", OrderingScope: "s", StableKey: "k"}},
		{name: "uppercase namespace", id: storage.OrderedID{Namespace: "NS", OrderingScope: "s", StableKey: "k"}},
		{name: "empty scope", id: storage.OrderedID{Namespace: "n", OrderingScope: "", StableKey: "k"}},
		{name: "empty stable key", id: storage.OrderedID{Namespace: "n", OrderingScope: "s", StableKey: ""}},
		{name: "oversize stable key", id: storage.OrderedID{Namespace: "n", OrderingScope: "s", StableKey: storage.StableKey(strings.Repeat("x", storage.MaxStableKeyBytes+1))}},
		{name: "invalid utf-8 stable key", id: storage.OrderedID{Namespace: "n", OrderingScope: "s", StableKey: storage.StableKey([]byte{0xff, 0xfe})}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if subj, err := orderedRecordSubject(tc.id); err == nil {
				t.Fatalf("orderedRecordSubject(%+v) = %q, want an error", tc.id, subj)
			}
		})
	}
}

func TestOrderedRecordRoundTrip(t *testing.T) {
	t.Parallel()
	large := bytes.Repeat([]byte{0xAB}, storage.MaxOrderedValueBytes)
	cases := []struct {
		name   string
		mutate func(*storage.OrderedRecord)
	}{
		{name: "live ranked and due", mutate: func(*storage.OrderedRecord) {}},
		{name: "not due and unranked", mutate: func(r *storage.OrderedRecord) {
			r.Due = storage.Due{State: storage.NotDue}
			r.Rank = storage.Rank{}
		}},
		{name: "tombstone", mutate: func(r *storage.OrderedRecord) {
			r.Deleted = true
			r.Due = storage.Due{State: storage.NotDue}
			r.Rank = storage.Rank{}
		}},
		{name: "empty value", mutate: func(r *storage.OrderedRecord) { r.Value = []byte{} }},
		{name: "nil value", mutate: func(r *storage.OrderedRecord) { r.Value = nil }},
		{name: "one mebibyte value", mutate: func(r *storage.OrderedRecord) { r.Value = large }},
		{name: "max revision and order", mutate: func(r *storage.OrderedRecord) {
			r.Revision = math.MaxUint64
			r.Order = math.MaxUint64
		}},
		{name: "max rank and due", mutate: func(r *storage.OrderedRecord) {
			r.Rank = storage.Rank{Ranked: true, Value: math.MaxInt64}
			r.Due = storage.Due{State: storage.DueAt, UnixMillis: math.MaxInt64}
		}},
		{name: "ranked zero value", mutate: func(r *storage.OrderedRecord) {
			r.Rank = storage.Rank{Ranked: true, Value: 0}
		}},
		{name: "due at zero millis", mutate: func(r *storage.OrderedRecord) {
			r.Due = storage.Due{State: storage.DueAt, UnixMillis: 0}
		}},
		{name: "max stable key", mutate: func(r *storage.OrderedRecord) {
			r.ID.StableKey = storage.StableKey(strings.Repeat("k", storage.MaxStableKeyBytes))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want := testOrderedRecord()
			tc.mutate(&want)
			data, err := encodeOrderedRecord(want)
			if err != nil {
				t.Fatalf("encodeOrderedRecord: %v", err)
			}
			subj := mustRecordSubject(t, want.ID)
			got, err := decodeOrderedRecord(subj, data)
			if err != nil {
				t.Fatalf("decodeOrderedRecord: %v", err)
			}
			if got.ID != want.ID || got.RankingScope != want.RankingScope ||
				got.Revision != want.Revision || got.Order != want.Order ||
				got.Due != want.Due || got.Rank != want.Rank || got.Deleted != want.Deleted {
				t.Fatalf("decoded record = %+v, want %+v", got, want)
			}
			if !bytes.Equal(got.Value, want.Value) {
				t.Fatalf("decoded value len %d, want len %d", len(got.Value), len(want.Value))
			}
		})
	}
}

func TestEncodeOrderedRecordRejectsInvalidRecords(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*storage.OrderedRecord)
	}{
		{name: "zero revision", mutate: func(r *storage.OrderedRecord) { r.Revision = 0 }},
		{name: "zero order", mutate: func(r *storage.OrderedRecord) { r.Order = 0 }},
		{name: "invalid namespace", mutate: func(r *storage.OrderedRecord) { r.ID.Namespace = "NS" }},
		{name: "invalid ranking scope", mutate: func(r *storage.OrderedRecord) { r.RankingScope = "" }},
		{name: "not due with nonzero millis", mutate: func(r *storage.OrderedRecord) {
			r.Due = storage.Due{State: storage.NotDue, UnixMillis: 5}
		}},
		{name: "unknown due state", mutate: func(r *storage.OrderedRecord) {
			r.Due = storage.Due{State: storage.DueState(9)}
		}},
		{name: "oversize value", mutate: func(r *storage.OrderedRecord) {
			r.Value = make([]byte, storage.MaxOrderedValueBytes+1)
		}},
		{name: "deleted but ranked", mutate: func(r *storage.OrderedRecord) {
			r.Deleted = true
			r.Due = storage.Due{State: storage.NotDue}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := testOrderedRecord()
			tc.mutate(&rec)
			if data, err := encodeOrderedRecord(rec); err == nil {
				t.Fatalf("encodeOrderedRecord(%+v) = %q, want an error", rec, data)
			}
		})
	}
}

func TestDecodeOrderedRecordRejectsMalformedPayloads(t *testing.T) {
	t.Parallel()
	subj := mustRecordSubject(t, testOrderedID())
	valid, err := encodeOrderedRecord(testOrderedRecord())
	if err != nil {
		t.Fatalf("encodeOrderedRecord: %v", err)
	}
	// A field-level rewrite of the canonical payload, used to build payloads that
	// are syntactically valid JSON but semantically rejected.
	rewrite := func(t *testing.T, key string, raw string) []byte {
		t.Helper()
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(valid, &obj); err != nil {
			t.Fatalf("unmarshal canonical payload: %v", err)
		}
		obj[key] = json.RawMessage(raw)
		out, err := json.Marshal(obj)
		if err != nil {
			t.Fatalf("marshal rewritten payload: %v", err)
		}
		return out
	}

	cases := []struct {
		name string
		data func(t *testing.T) []byte
	}{
		{name: "empty", data: func(*testing.T) []byte { return nil }},
		{name: "not json", data: func(*testing.T) []byte { return []byte("not json at all") }},
		{name: "truncated json", data: func(*testing.T) []byte { return valid[:len(valid)/2] }},
		{name: "json array", data: func(*testing.T) []byte { return []byte(`[1,2,3]`) }},
		{name: "json null", data: func(*testing.T) []byte { return []byte(`null`) }},
		{name: "trailing garbage", data: func(*testing.T) []byte { return append(bytes.Clone(valid), []byte("{}")...) }},
		{name: "unknown field", data: func(t *testing.T) []byte { return rewrite(t, "unexpected", `1`) }},
		{name: "zero version", data: func(t *testing.T) []byte { return rewrite(t, "v", `0`) }},
		{name: "future version", data: func(t *testing.T) []byte { return rewrite(t, "v", `2`) }},
		{name: "zero revision", data: func(t *testing.T) []byte { return rewrite(t, "rev", `0`) }},
		{name: "zero order", data: func(t *testing.T) []byte { return rewrite(t, "order", `0`) }},
		{name: "unknown due state", data: func(t *testing.T) []byte { return rewrite(t, "due_state", `7`) }},
		{name: "invalid ranking scope", data: func(t *testing.T) []byte { return rewrite(t, "rs", `"NOT A NAME"`) }},
		{name: "wrong type for order", data: func(t *testing.T) []byte { return rewrite(t, "order", `"seven"`) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got, err := decodeOrderedRecord(subj, tc.data(t)); err == nil {
				t.Fatalf("decodeOrderedRecord accepted malformed payload: %+v", got)
			}
		})
	}
}

// TestDecodeOrderedRecordFailsClosedOnForgedIdentity is the security core of the
// hashed-subject scheme: the subject carries only a hash, so a payload that
// claims a different identity than the subject it is stored under must be
// rejected rather than returned.
func TestDecodeOrderedRecordFailsClosedOnForgedIdentity(t *testing.T) {
	t.Parallel()
	honest := testOrderedRecord()
	honestSubject := mustRecordSubject(t, honest.ID)

	forge := func(t *testing.T, mutate func(*storage.OrderedRecord)) []byte {
		t.Helper()
		rec := testOrderedRecord()
		mutate(&rec)
		data, err := encodeOrderedRecord(rec)
		if err != nil {
			t.Fatalf("encodeOrderedRecord(forged): %v", err)
		}
		return data
	}

	cases := []struct {
		name   string
		mutate func(*storage.OrderedRecord)
	}{
		{name: "different stable key", mutate: func(r *storage.OrderedRecord) { r.ID.StableKey = "another-key" }},
		{name: "different namespace", mutate: func(r *storage.OrderedRecord) { r.ID.Namespace = "other" }},
		{name: "different ordering scope", mutate: func(r *storage.OrderedRecord) { r.ID.OrderingScope = "other" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := forge(t, tc.mutate)
			got, err := decodeOrderedRecord(honestSubject, data)
			if err == nil {
				t.Fatalf("decodeOrderedRecord returned a forged record: %+v", got)
			}
			var mismatch *OrderedIdentityMismatchError
			if !errors.As(err, &mismatch) {
				t.Fatalf("error = %v (%T), want *OrderedIdentityMismatchError", err, err)
			}
			if mismatch.Subject != honestSubject {
				t.Fatalf("mismatch.Subject = %q, want %q", mismatch.Subject, honestSubject)
			}
			if mismatch.PayloadSubject == honestSubject {
				t.Fatalf("mismatch.PayloadSubject equals the stored subject %q; the identities were not compared", honestSubject)
			}
			if strings.Contains(mismatch.Error(), "another-key") {
				t.Fatalf("identity mismatch error leaks the raw stable key: %s", mismatch.Error())
			}
		})
	}

	// The honest payload under its own subject still decodes: the guard rejects
	// forgeries, not everything.
	honestData, err := encodeOrderedRecord(honest)
	if err != nil {
		t.Fatalf("encodeOrderedRecord(honest): %v", err)
	}
	if _, err := decodeOrderedRecord(honestSubject, honestData); err != nil {
		t.Fatalf("honest payload rejected under its own subject: %v", err)
	}
	// A payload stored under a truncated or otherwise wrong subject fails too.
	if _, err := decodeOrderedRecord(honestSubject[:len(honestSubject)-1], honestData); err == nil {
		t.Fatal("decodeOrderedRecord accepted a payload under a truncated subject")
	}
}

func TestOrderedCounterCodec(t *testing.T) {
	t.Parallel()
	t.Run("round trip", func(t *testing.T) {
		t.Parallel()
		for _, n := range []uint64{1, 2, 9, 10, 12345, math.MaxUint64} {
			data := encodeOrderedCounter(n)
			got, err := decodeOrderedCounter(data)
			if err != nil {
				t.Fatalf("decodeOrderedCounter(%q): %v", data, err)
			}
			if got != n {
				t.Fatalf("decodeOrderedCounter(%q) = %d, want %d", data, got, n)
			}
		}
	})
	t.Run("malformed", func(t *testing.T) {
		t.Parallel()
		for _, data := range [][]byte{nil, []byte(""), []byte("x"), []byte("-1"), []byte("1.5"), []byte(" 1"), []byte("01"), []byte("+1"), []byte("18446744073709551616"), []byte("0")} {
			if got, err := decodeOrderedCounter(data); err == nil {
				t.Fatalf("decodeOrderedCounter(%q) = %d, want an error", data, got)
			}
		}
	})
}
