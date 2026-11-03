package skiplist

import (
	"errors"
	"fmt"
	"strings"
)

const (
	// DefaultSeparator splits the key and value fields of a record. It matches
	// the format written by the C++ original, so dumps produced by that
	// implementation can still be read as long as they contain no backslashes.
	DefaultSeparator = ':'

	// escapeChar introduces an escape sequence inside a record field.
	escapeChar = '\\'

	// recordTerminator ends every record.
	recordTerminator = '\n'
)

// Format describes how Dump serialises elements and how Load parses them back.
//
// The zero value is not usable; build one with NewFormat or fill in both
// codecs explicitly.
type Format[K, V any] struct {
	// Key and Value convert between in-memory values and their textual form.
	Key   TextCodec[K]
	Value TextCodec[V]

	// Separator splits the two fields of a record. The zero value selects
	// DefaultSeparator. It must be a printable ASCII byte other than the
	// backslash, 'n' or 'r', which are reserved for escape sequences.
	Separator byte
}

// NewFormat returns a Format that uses the default separator.
func NewFormat[K, V any](key TextCodec[K], value TextCodec[V]) Format[K, V] {
	return Format[K, V]{Key: key, Value: value, Separator: DefaultSeparator}
}

// normalize fills in the defaults and validates the format.
func (f Format[K, V]) normalize() (Format[K, V], error) {
	switch {
	case f.Key.Encode == nil, f.Key.Decode == nil:
		return f, errors.New("skiplist: format needs a key codec with both Encode and Decode set")
	case f.Value.Encode == nil, f.Value.Decode == nil:
		return f, errors.New("skiplist: format needs a value codec with both Encode and Decode set")
	}
	if f.Separator == 0 {
		f.Separator = DefaultSeparator
	}
	if err := validateSeparator(f.Separator); err != nil {
		return f, err
	}
	return f, nil
}

func validateSeparator(separator byte) error {
	switch {
	case separator < 0x21 || separator > 0x7e:
		return fmt.Errorf("skiplist: separator %#x is not a printable ASCII character", separator)
	case separator == escapeChar, separator == 'n', separator == 'r':
		return fmt.Errorf("skiplist: separator %#x is reserved for escape sequences", separator)
	}
	return nil
}

// escapeField renders s so that it occupies exactly one field of one record:
// the separator, the escape character and both line break characters are
// prefixed with a backslash, and line breaks are replaced by a letter code.
func escapeField(s string, separator byte) string {
	if !strings.ContainsRune(s, escapeChar) &&
		!strings.ContainsRune(s, rune(separator)) &&
		!strings.ContainsRune(s, recordTerminator) &&
		!strings.ContainsRune(s, '\r') {
		return s
	}

	var builder strings.Builder
	builder.Grow(len(s) + 4)
	for index := 0; index < len(s); index++ {
		char := s[index]
		switch char {
		case escapeChar, separator:
			builder.WriteByte(escapeChar)
		case recordTerminator:
			builder.WriteByte(escapeChar)
			char = 'n'
		case '\r':
			builder.WriteByte(escapeChar)
			char = 'r'
		}
		builder.WriteByte(char)
	}
	return builder.String()
}

// unescapeField reverses escapeField. It reports an error wrapping
// ErrInvalidEscape when the input contains a backslash that does not introduce
// a recognised sequence, which is how a truncated or corrupted dump is
// detected.
func unescapeField(s string, separator byte) (string, error) {
	if !strings.ContainsRune(s, escapeChar) {
		return s, nil
	}

	var builder strings.Builder
	builder.Grow(len(s))
	for index := 0; index < len(s); index++ {
		char := s[index]
		if char != escapeChar {
			builder.WriteByte(char)
			continue
		}
		index++
		if index == len(s) {
			return "", fmt.Errorf("%w: field ends with a lone escape character", ErrInvalidEscape)
		}
		switch escaped := s[index]; escaped {
		case 'n':
			builder.WriteByte(byte(recordTerminator))
		case 'r':
			builder.WriteByte('\r')
		case escapeChar, separator:
			builder.WriteByte(escaped)
		default:
			return "", fmt.Errorf("%w: %q", ErrInvalidEscape, string([]byte{escapeChar, escaped}))
		}
	}
	return builder.String(), nil
}

// splitRecord divides record into a key and a value at the first separator
// that is not part of an escape sequence.
//
// strings.Cut cannot do this. Escaping a separator prefixes it with a
// backslash but leaves the separator byte in the field, so a key of "a:b" is
// stored as `a\:b` and a raw cut splits inside the key. The scan is by byte,
// which is safe because a separator is always printable ASCII and therefore
// cannot occur as a UTF-8 continuation byte.
func splitRecord(record string, separator byte) (key, value string, ok bool) {
	escaped := false
	for index := 0; index < len(record); index++ {
		char := record[index]
		if escaped {
			escaped = false
			continue
		}
		if char == escapeChar {
			escaped = true
			continue
		}
		if char == separator {
			return record[:index], record[index+1:], true
		}
	}
	return "", "", false
}
