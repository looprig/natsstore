package natsstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/looprig/storage"
)

// orderedRecordVersion is the payload schema version written into every ordered
// record. A decoder accepts exactly this version and rejects anything else, so a
// future schema change is a hard, visible cut rather than a silent
// misinterpretation of foreign bytes.
const orderedRecordVersion uint8 = 1

// orderedStreamDescription is the provisioning marker carried in the stream's
// description. It pins the on-disk layout version alongside the payload version:
// a stream provisioned by another layout is refused rather than written into.
const orderedStreamDescription = "looprig ordered index layout v1"

// The ordered subject and stream namespaces are deliberately introduced by an
// UPPERCASE token. Every other encoded name this package emits — ledger subjects
// and stream names — derives from a storage.ValidateName value, whose grammar is
// lowercase-only, so no ledger subject can begin with "OI." and no ledger stream
// name can begin with "OI_". The ordered space therefore cannot alias an existing
// one, and JetStream never sees two streams claiming one subject.
const (
	orderedSubjectRoot  = "OI"
	orderedStreamPrefix = "OI_"

	// orderedCounterToken and orderedRecordToken discriminate the two subject
	// families inside one namespace's stream.
	orderedCounterToken = "c"
	orderedRecordToken  = "r"
)

// orderedIdentityDomain domain-separates the identity hash preimage so a digest
// computed here can never coincide with one computed for another purpose.
const orderedIdentityDomain = "looprig.natsstore.ordered.identity.v1"

// OrderedCodecError reports a stored ordered payload that this package could not
// decode: malformed bytes, an unsupported schema version, or a payload that
// decodes to a record the storage contract rejects. It fails closed — a caller
// must never treat it as an absent record — and unwraps to the underlying cause
// when there is one.
type OrderedCodecError struct {
	Subject string
	Reason  string
	Cause   error
}

func (e *OrderedCodecError) Error() string {
	msg := "natsstore: ordered payload on subject " + strconv.Quote(e.Subject) + ": " + e.Reason
	if e.Cause != nil {
		msg += ": " + e.Cause.Error()
	}
	return msg
}

// Unwrap returns the underlying cause (possibly nil).
func (e *OrderedCodecError) Unwrap() error { return e.Cause }

// OrderedIdentityMismatchError reports a stored ordered payload whose own
// identity does not hash to the subject it was read from. Because a StableKey is
// never written into a subject verbatim, the subject alone proves nothing about
// the identity; this is the check that makes the hashed subject trustworthy, and
// a mismatch is always a fail-closed error rather than a returned record.
// PayloadSubject is the subject the payload's own identity hashes to, so both
// fields are hashes and neither discloses raw key bytes.
type OrderedIdentityMismatchError struct {
	Subject        string
	PayloadSubject string
}

func (e *OrderedIdentityMismatchError) Error() string {
	return "natsstore: ordered record stored on subject " + strconv.Quote(e.Subject) +
		" carries an identity that belongs on subject " + strconv.Quote(e.PayloadSubject)
}

// OrderedCounterError reports a per-scope order counter payload that is not a
// canonical decimal order value.
type OrderedCounterError struct {
	Value  []byte
	Reason string
}

func (e *OrderedCounterError) Error() string {
	return "natsstore: ordered counter payload " + strconv.Quote(string(e.Value)) + ": " + e.Reason
}

// orderedIdentityToken hashes an OrderedID into a subject-safe token. The
// preimage is domain-separated and every component is length-framed, so no two
// distinct identities share a preimage: moving a byte across the
// namespace/scope/key boundaries changes the framing, not just the
// concatenation. The StableKey never appears in the output, which is what lets
// an arbitrary UTF-8 key address a NATS subject at all.
func orderedIdentityToken(id storage.OrderedID) string {
	h := sha256.New()
	h.Write([]byte(orderedIdentityDomain))
	for _, part := range []string{id.Namespace, id.OrderingScope, string(id.StableKey)} {
		var framed [8]byte
		binary.BigEndian.PutUint64(framed[:], uint64(len(part)))
		h.Write(framed[:])
		h.Write([]byte(part))
	}
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

// orderedStreamName maps a validated namespace to its dedicated stream name.
// encodeStream's output is [a-z0-9_-], so the uppercase prefix keeps ordered
// streams disjoint from every other stream this package provisions.
func orderedStreamName(namespace string) (string, error) {
	if err := storage.ValidateName(namespace); err != nil {
		return "", err
	}
	return orderedStreamPrefix + encodeStream(namespace), nil
}

// orderedSubjectFilter returns the wildcard subject a namespace's stream binds.
// Because encodeStream is injective and emits no '.', each namespace occupies
// exactly one subject token and two namespaces can never overlap.
func orderedSubjectFilter(namespace string) (string, error) {
	prefix, err := orderedNamespacePrefix(namespace)
	if err != nil {
		return "", err
	}
	return prefix + ".>", nil
}

// orderedNamespacePrefix returns the dot-terminated-free root of a namespace's
// subject space.
func orderedNamespacePrefix(namespace string) (string, error) {
	if err := storage.ValidateName(namespace); err != nil {
		return "", err
	}
	return orderedSubjectRoot + "." + encodeStream(namespace), nil
}

// orderedCounterSubject is the subject holding an order scope's current order
// counter. It is a plain message whose payload is the last allocated order: a
// JetStream counter stream (Nats-Incr) cannot coexist with the
// expected-last-subject-sequence fence this design needs, so the counter is
// fenced rather than incremented.
func orderedCounterSubject(namespace, orderingScope string) (string, error) {
	prefix, err := orderedNamespacePrefix(namespace)
	if err != nil {
		return "", err
	}
	if err := storage.ValidateName(orderingScope); err != nil {
		return "", err
	}
	return prefix + "." + orderedCounterToken + "." + encodeStream(orderingScope), nil
}

// orderedRecordSubject is the subject holding one record's current version. The
// ordering scope stays legible (it is a validated name) while the identity token
// hashes the whole identity, including the StableKey, which the contract forbids
// placing in a subject verbatim. It is deliberately distinct from the counter
// subject of the same scope: two messages on one subject cannot both carry an
// expectation inside a single atomic batch.
func orderedRecordSubject(id storage.OrderedID) (string, error) {
	if err := storage.ValidateOrderedID(id); err != nil {
		return "", err
	}
	prefix, err := orderedNamespacePrefix(id.Namespace)
	if err != nil {
		return "", err
	}
	return prefix + "." + orderedRecordToken + "." + encodeStream(id.OrderingScope) + "." + orderedIdentityToken(id), nil
}

// orderedPayload is the wire form of an ordered record. It retains the namespace,
// both scopes, and the ORIGINAL stable key so a read can verify the payload
// against the hashed subject it came from.
type orderedPayload struct {
	Version       uint8  `json:"v"`
	Namespace     string `json:"ns"`
	OrderingScope string `json:"os"`
	StableKey     string `json:"key"`
	RankingScope  string `json:"rs"`
	Revision      uint64 `json:"rev"`
	Order         uint64 `json:"order"`
	DueState      uint8  `json:"due_state"`
	DueUnixMillis int64  `json:"due_ms"`
	Ranked        bool   `json:"ranked"`
	RankValue     int64  `json:"rank"`
	Deleted       bool   `json:"deleted"`
	Value         []byte `json:"value"`
}

// encodeOrderedRecord serialises rec after validating it against the storage
// contract, so an invalid record can never reach the stream.
func encodeOrderedRecord(rec storage.OrderedRecord) ([]byte, error) {
	if err := storage.ValidateOrderedRecord(rec); err != nil {
		return nil, err
	}
	payload := orderedPayload{
		Version:       orderedRecordVersion,
		Namespace:     rec.ID.Namespace,
		OrderingScope: rec.ID.OrderingScope,
		StableKey:     string(rec.ID.StableKey),
		RankingScope:  rec.RankingScope,
		Revision:      rec.Revision,
		Order:         rec.Order,
		DueState:      uint8(rec.Due.State),
		DueUnixMillis: rec.Due.UnixMillis,
		Ranked:        rec.Rank.Ranked,
		RankValue:     rec.Rank.Value,
		Deleted:       rec.Deleted,
		Value:         rec.Value,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, &OrderedCodecError{Reason: "encode failed", Cause: err}
	}
	return data, nil
}

// decodeOrderedRecord parses a stored payload read from subject and proves it
// belongs there. It fails closed at four gates in order: the bytes must be a
// single strict JSON object of exactly the known fields; the schema version must
// match; the resulting record must satisfy the storage contract; and the
// record's OWN identity must hash back to subject. The last gate is what makes a
// hashed subject safe — without it, any writer who reached the stream could
// serve a record under another identity's subject.
//
// Note what "strict" does NOT cover: DisallowUnknownFields rejects unknown keys,
// but encoding/json accepts a DUPLICATED known key and keeps the last one, so
// the stored bytes are not a canonical form and two distinct payloads can decode
// to the same record. Nothing here depends on canonicality — the identity gate
// re-derives the subject from the DECODED record, and no comparison in this
// package hashes or compares raw payload bytes — so this is a note for a future
// reader, not a hole. A change that starts treating the bytes as canonical
// (content-addressing them, say) must add a duplicate-key check first.
func decodeOrderedRecord(subject string, data []byte) (storage.OrderedRecord, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var payload orderedPayload
	if err := dec.Decode(&payload); err != nil {
		return storage.OrderedRecord{}, &OrderedCodecError{Subject: subject, Reason: "malformed payload", Cause: err}
	}
	if dec.More() {
		return storage.OrderedRecord{}, &OrderedCodecError{Subject: subject, Reason: "trailing bytes after the payload"}
	}
	if payload.Version != orderedRecordVersion {
		return storage.OrderedRecord{}, &OrderedCodecError{
			Subject: subject,
			Reason:  "unsupported payload version " + strconv.FormatUint(uint64(payload.Version), 10),
		}
	}
	rec := storage.OrderedRecord{
		ID: storage.OrderedID{
			Namespace:     payload.Namespace,
			OrderingScope: payload.OrderingScope,
			StableKey:     storage.StableKey(payload.StableKey),
		},
		RankingScope: payload.RankingScope,
		Revision:     payload.Revision,
		Order:        payload.Order,
		Due:          storage.Due{State: storage.DueState(payload.DueState), UnixMillis: payload.DueUnixMillis},
		Rank:         storage.Rank{Ranked: payload.Ranked, Value: payload.RankValue},
		Value:        payload.Value,
		Deleted:      payload.Deleted,
	}
	if err := storage.ValidateOrderedRecord(rec); err != nil {
		return storage.OrderedRecord{}, &OrderedCodecError{Subject: subject, Reason: "payload violates the record contract", Cause: err}
	}
	want, err := orderedRecordSubject(rec.ID)
	if err != nil {
		return storage.OrderedRecord{}, &OrderedCodecError{Subject: subject, Reason: "payload identity has no subject", Cause: err}
	}
	if want != subject {
		return storage.OrderedRecord{}, &OrderedIdentityMismatchError{Subject: subject, PayloadSubject: want}
	}
	return rec, nil
}

// encodeOrderedCounter renders an allocated order as the counter subject's
// payload. Orders are nonzero, so the encoding needs no absent representation:
// an absent counter is an absent subject.
func encodeOrderedCounter(order uint64) []byte {
	return []byte(strconv.FormatUint(order, 10))
}

// decodeOrderedCounter parses a counter payload. It accepts only the canonical
// rendering of a nonzero order — no sign, no padding, no whitespace — so a
// corrupt or foreign payload is rejected instead of silently reallocating an
// order the scope already handed out.
func decodeOrderedCounter(data []byte) (uint64, error) {
	order, err := strconv.ParseUint(string(data), 10, 64)
	if err != nil {
		reason := "not a decimal order"
		if errors.Is(err, strconv.ErrRange) {
			reason = "order out of range"
		}
		return 0, &OrderedCounterError{Value: bytes.Clone(data), Reason: reason}
	}
	if order == 0 {
		return 0, &OrderedCounterError{Value: bytes.Clone(data), Reason: "order must be nonzero"}
	}
	if !bytes.Equal(encodeOrderedCounter(order), data) {
		return 0, &OrderedCounterError{Value: bytes.Clone(data), Reason: "not a canonical decimal order"}
	}
	return order, nil
}

// --- subject classification ------------------------------------------------

// orderedSubjectFamily discriminates the subject families inside one namespace's
// stream. A view consumes every subject the stream holds, so it must decide what
// each one is before deciding what to do with it.
type orderedSubjectFamily uint8

const (
	// orderedForeignSubject is a subject outside this namespace's space, or one
	// naming a family this layout version does not define.
	orderedForeignSubject orderedSubjectFamily = iota
	// orderedCounterSubjectFamily is a per-order-scope order counter.
	orderedCounterSubjectFamily
	// orderedRecordSubjectFamily is one record's current version.
	orderedRecordSubjectFamily
)

// classifyOrderedSubject reports which family subject belongs to within
// namespace. It matches on the family token alone and does not attempt to
// recover the ordering scope or the identity: the identity is a hash, and the
// authoritative copy of every identity component is inside the payload, where
// decodeOrderedRecord verifies it against this same subject.
func classifyOrderedSubject(namespace, subject string) (orderedSubjectFamily, error) {
	prefix, err := orderedNamespacePrefix(namespace)
	if err != nil {
		return orderedForeignSubject, err
	}
	rest, ok := strings.CutPrefix(subject, prefix+".")
	if !ok {
		return orderedForeignSubject, nil
	}
	token, _, ok := strings.Cut(rest, ".")
	if !ok {
		return orderedForeignSubject, nil
	}
	switch token {
	case orderedCounterToken:
		return orderedCounterSubjectFamily, nil
	case orderedRecordToken:
		return orderedRecordSubjectFamily, nil
	default:
		return orderedForeignSubject, nil
	}
}

// --- continuation cursors --------------------------------------------------

// orderedCursorVersion is the token version this package issues and the only one
// it accepts. It is carried as a plaintext prefix rather than inside the encoded
// body so that a token from a FUTURE version is classified as an unknown version
// instead of decode-failing as malformed — the two are distinct fail-closed
// rules, and a caller upgrading across versions needs to tell them apart.
const orderedCursorVersion = 1

// orderedCursorPrefix marks this package's cursors. A token is
// "oi" + decimal version + "." + base64url(payload).
const orderedCursorPrefix = "oi"

// maxOrderedCursorBytes caps an untrusted token before any decoding, so a hostile
// cursor cannot drive an unbounded allocation. It is far above the largest token
// this package issues: the payload is a bounded namespace, two bounded scopes, a
// 256-byte stable key, and three integers.
const maxOrderedCursorBytes = 8 << 10

// orderedCursorPayload is the decoded position a continuation token carries. It
// is deliberately ONE type for both cursor kinds: the two kinds share their
// query binding (namespace) and their tiebreakers (stable key, ordering scope),
// and a single type keeps the "which fields bind this query" decision in one
// place. Kind is what makes a ranked token unusable as a due token.
//
// A cursor conveys position, not authority. Nothing here is trusted: the caller
// compares Namespace, RankingScope, and DueBound against the LIVE request and
// rejects a mismatch, so a forged token can only ask for a position the caller
// was already entitled to read. That is also why these tokens are not signed —
// a signature would protect a claim the decoder does not make, and a per-process
// key would make one process's cursors unusable by another over the same stream.
type orderedCursorPayload struct {
	Kind          string `json:"k"`
	Namespace     string `json:"ns"`
	RankingScope  string `json:"rs,omitempty"`
	Rank          int64  `json:"rank,omitempty"`
	DueBound      int64  `json:"bound,omitempty"`
	DueAt         int64  `json:"due,omitempty"`
	StableKey     string `json:"sk"`
	OrderingScope string `json:"os"`
}

// encodeOrderedCursor renders a position as an opaque, versioned token.
func encodeOrderedCursor(payload orderedCursorPayload) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		// Unreachable by construction: every field is a string or an integer.
		// It is still returned rather than discarded, because a silent encode
		// failure would hand the caller a cursor that resumes somewhere else.
		return "", &OrderedCodecError{Reason: "cursor encode failed", Cause: err}
	}
	return orderedCursorPrefix + strconv.Itoa(orderedCursorVersion) + "." +
		base64.RawURLEncoding.EncodeToString(data), nil
}

// decodeOrderedCursorPayload parses an untrusted token for kind, classifying
// every rejection under exactly one of the contract's four fail-closed rules.
// The order of the gates is what makes the classification meaningful: the length
// cap and the version prefix are checked before anything is decoded, the kind is
// checked before the position is used, and the QUERY binding is left to the
// caller, which is the only party holding the live request.
func decodeOrderedCursorPayload(kind storage.OrderedCursorKind, token string) (orderedCursorPayload, error) {
	reject := func(rule storage.OrderedCursorRule) error {
		return storage.NewInvalidOrderedCursorError(kind, token, rule)
	}
	if len(token) > maxOrderedCursorBytes {
		return orderedCursorPayload{}, reject(storage.OrderedCursorMalformed)
	}
	version, body, ok := splitOrderedCursorVersion(token)
	if !ok {
		return orderedCursorPayload{}, reject(storage.OrderedCursorMalformed)
	}
	if version != orderedCursorVersion {
		return orderedCursorPayload{}, reject(storage.OrderedCursorUnknownVersion)
	}
	data, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return orderedCursorPayload{}, reject(storage.OrderedCursorMalformed)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var payload orderedCursorPayload
	if err := dec.Decode(&payload); err != nil {
		return orderedCursorPayload{}, reject(storage.OrderedCursorMalformed)
	}
	if dec.More() {
		return orderedCursorPayload{}, reject(storage.OrderedCursorMalformed)
	}
	if payload.Kind != string(kind) {
		if payload.Kind == string(storage.RankedCursorKind) || payload.Kind == string(storage.DueCursorKind) {
			return orderedCursorPayload{}, reject(storage.OrderedCursorWrongKind)
		}
		return orderedCursorPayload{}, reject(storage.OrderedCursorMalformed)
	}
	return payload, nil
}

// splitOrderedCursorVersion separates a token's plaintext version from its body.
// It accepts only the canonical spelling this package emits — the prefix, at
// least one decimal digit with no sign or padding, then the separator — so a
// token that merely looks versioned is malformed rather than "some other
// version".
func splitOrderedCursorVersion(token string) (int, string, bool) {
	rest, ok := strings.CutPrefix(token, orderedCursorPrefix)
	if !ok {
		return 0, "", false
	}
	digits, body, ok := strings.Cut(rest, ".")
	if !ok || digits == "" {
		return 0, "", false
	}
	for index := range len(digits) {
		if digits[index] < '0' || digits[index] > '9' {
			return 0, "", false
		}
	}
	version, err := strconv.Atoi(digits)
	if err != nil || strconv.Itoa(version) != digits {
		return 0, "", false
	}
	return version, body, true
}

// orderedMalformedCursorToken returns a nonempty token this package could never
// have issued, for kind. It exists so F4.4 can satisfy
// storetest.OrderedCursorProbe without a conformance test writing a literal in
// this provider's grammar — the encoding is owned here, so the counterexamples
// are owned here too.
func orderedMalformedCursorToken(kind storage.OrderedCursorKind) string {
	return "not-an-ordered-cursor-" + string(kind) + "-token"
}

// orderedUnknownVersionCursorToken returns a token that is well formed for this
// encoding but carries a version this package does not support.
func orderedUnknownVersionCursorToken(kind storage.OrderedCursorKind) string {
	payload, err := json.Marshal(orderedCursorPayload{Kind: string(kind), Namespace: "sessions"})
	if err != nil {
		// Unreachable for the same reason as in encodeOrderedCursor; a fixed
		// body still yields a well-formed unsupported-version token.
		payload = []byte("{}")
	}
	return orderedCursorPrefix + strconv.Itoa(orderedCursorVersion+1) + "." +
		base64.RawURLEncoding.EncodeToString(payload)
}
