package natsstore

import (
	"strconv"
	"strings"

	"github.com/ciram-co/storekit"
)

// A storekit name is '/'-joined segments over [a-z0-9][a-z0-9_.-]*, so it only
// ever contains lowercase letters, digits, '_', '.', '-' and '/'. Both mappings
// below rely on that: every encoder validates with storekit.ValidateName first,
// so the escape schemes never have to reason about bytes the grammar forbids
// (notably '%', which is why it can safely mark an escape).

// subjectDotEscape stands in for a literal '.' in a name before '/' is remapped
// onto the subject-token separator. '%' cannot occur in a valid name, so the
// three-byte sequence is unambiguous and reversible.
const subjectDotEscape = "%2E"

// streamEscape introduces every stream-name escape. It is the single reserved
// byte of the stream encoding: a literal '_' is written "_u", so an '_' in the
// output is always the first byte of an escape.
const streamEscape = '_'

// NameEncodingError reports an encoded token — a JetStream subject or stream
// name — that does not decode back to a valid storekit name, i.e. one this
// package's encoders never emit. Value is the offending token.
type NameEncodingError struct {
	Value  string
	Reason string
}

func (e *NameEncodingError) Error() string {
	return "natsstore: malformed encoded name " + strconv.Quote(e.Value) + ": " + e.Reason
}

// subjectForName maps a storekit name to a JetStream subject. It validates the
// name (returning the *storekit.InvalidNameError verbatim), escapes every
// literal '.' to %2E, then remaps '/' onto '.' — the subject-token separator.
// The dot escape must run before the slash remap so the two byte roles never mix.
func subjectForName(name string) (string, error) {
	if err := storekit.ValidateName(name); err != nil {
		return "", err
	}
	return encodeSubject(name), nil
}

// nameFromSubject inverts subjectForName. It undoes the slash remap ('.' → '/')
// before the dot escape (%2E → '.'), then proves the result is a valid name
// whose canonical encoding is exactly subj; anything else is a
// *NameEncodingError. The canonical-form check makes the decode the exact
// inverse of the encode, so the pair is bijective onto the encoder's image.
func nameFromSubject(subj string) (string, error) {
	unhier := strings.ReplaceAll(subj, ".", "/")
	name := strings.ReplaceAll(unhier, subjectDotEscape, ".")
	if err := storekit.ValidateName(name); err != nil {
		return "", &NameEncodingError{Value: subj, Reason: "does not decode to a valid storekit name"}
	}
	if encodeSubject(name) != subj {
		return "", &NameEncodingError{Value: subj, Reason: "not a canonical subject encoding"}
	}
	return name, nil
}

// encodeSubject applies the subject transform to an already-validated name.
func encodeSubject(name string) string {
	escaped := strings.ReplaceAll(name, ".", subjectDotEscape)
	return strings.ReplaceAll(escaped, "/", ".")
}

// streamForName maps a storekit name to a JetStream stream name, which may only
// contain [a-z0-9_-]. It validates first (returning *storekit.InvalidNameError),
// then reversibly escapes the three bytes a name can carry that a stream name
// cannot: '_' → "_u", '.' → "_d", '/' → "_s". '-', lowercase letters and digits
// pass through. Because '_' escapes itself, the mapping is injective — unlike a
// naive "'.'/'/' both become '_'", which would alias "a.b" and "a/b".
func streamForName(name string) (string, error) {
	if err := storekit.ValidateName(name); err != nil {
		return "", err
	}
	return encodeStream(name), nil
}

// nameFromStream inverts streamForName. It expands the escapes, then requires
// the result to be a valid name whose canonical encoding is exactly stream;
// otherwise it is a *NameEncodingError. As with subjects the canonical-form
// check pins the decode to the exact inverse of the encode.
func nameFromStream(stream string) (string, error) {
	name, err := decodeStream(stream)
	if err != nil {
		return "", err
	}
	if err := storekit.ValidateName(name); err != nil {
		return "", &NameEncodingError{Value: stream, Reason: "does not decode to a valid storekit name"}
	}
	if encodeStream(name) != stream {
		return "", &NameEncodingError{Value: stream, Reason: "not a canonical stream encoding"}
	}
	return name, nil
}

// encodeStream applies the stream transform to an already-validated name.
func encodeStream(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for i := 0; i < len(name); i++ {
		switch name[i] {
		case streamEscape:
			b.WriteString("_u")
		case '.':
			b.WriteString("_d")
		case '/':
			b.WriteString("_s")
		default:
			b.WriteByte(name[i])
		}
	}
	return b.String()
}

// decodeStream expands the stream escapes byte by byte. A dangling or unknown
// escape is a *NameEncodingError; the caller still validates the decoded name.
func decodeStream(stream string) (string, error) {
	var b strings.Builder
	b.Grow(len(stream))
	for i := 0; i < len(stream); i++ {
		if stream[i] != streamEscape {
			b.WriteByte(stream[i])
			continue
		}
		i++
		if i >= len(stream) {
			return "", &NameEncodingError{Value: stream, Reason: "dangling escape"}
		}
		switch stream[i] {
		case 'u':
			b.WriteByte('_')
		case 'd':
			b.WriteByte('.')
		case 's':
			b.WriteByte('/')
		default:
			return "", &NameEncodingError{Value: stream, Reason: "unknown escape sequence"}
		}
	}
	return b.String(), nil
}
