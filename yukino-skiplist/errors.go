package skiplist

import (
	"errors"
	"fmt"
	"strconv"
	"unicode/utf8"
)

// recordSummaryLimit caps how much of an offending record ParseError quotes in
// its message, so that a single oversized line cannot flood a log.
const recordSummaryLimit = 32

var (
	// ErrMalformedRecord reports a line that does not contain the field
	// separator at all.
	ErrMalformedRecord = errors.New("malformed record")

	// ErrDuplicateKey reports a key that a single input stream defines twice.
	// A dump produced by Dump cannot contain duplicates, so the error usually
	// indicates a hand edited or concatenated file.
	ErrDuplicateKey = errors.New("duplicate key")

	// ErrInvalidEscape reports a backslash that is not followed by one of the
	// recognised escape characters.
	ErrInvalidEscape = errors.New("invalid escape sequence")
)

// ParseError reports the record that Load could not decode.
type ParseError struct {
	// Line is the one-based position of the record in the input stream.
	Line int
	// Record is the offending line, verbatim.
	Record string
	// Err is the underlying cause.
	Err error
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("skiplist: line %d (%s): %v", e.Line, summarise(e.Record), e.Err)
}

// Unwrap exposes the underlying cause to [errors.Is] and [errors.As].
func (e *ParseError) Unwrap() error { return e.Err }

// summarise quotes s, shortened to a bounded length on a rune boundary.
func summarise(s string) string {
	if len(s) <= recordSummaryLimit {
		return strconv.Quote(s)
	}
	cut := s[:recordSummaryLimit]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return strconv.Quote(cut) + "..."
}
