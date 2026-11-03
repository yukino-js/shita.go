package skiplist

import (
	"encoding"
	"encoding/base64"
	"strconv"
)

// TextCodec converts values of type T to and from the single-line textual
// representation stored by Dump and parsed back by Load.
//
// Encode must never emit a line break; the record encoder escapes the field
// separator and the backslash itself, so any other byte is safe. Decode is
// handed the unescaped field verbatim and should report a descriptive error
// for input it cannot parse.
//
// Both fields must be non-nil for the codec to be usable in a Format.
type TextCodec[T any] struct {
	Encode func(T) (string, error)
	Decode func(string) (T, error)
}

// textCodec builds a TextCodec from a formatting and a parsing function, for
// the types whose textual form cannot fail to render.
func textCodec[T any](format func(T) string, parse func(string) (T, error)) TextCodec[T] {
	return TextCodec[T]{
		Encode: func(value T) (string, error) { return format(value), nil },
		Decode: parse,
	}
}

// StringCodec returns a codec that stores strings verbatim.
func StringCodec() TextCodec[string] {
	return textCodec(func(value string) string { return value }, func(text string) (string, error) {
		return text, nil
	})
}

// BoolCodec returns a codec that stores booleans as "true" and "false".
func BoolCodec() TextCodec[bool] {
	return textCodec(strconv.FormatBool, strconv.ParseBool)
}

// IntCodec returns a codec for the platform int type in base ten.
func IntCodec() TextCodec[int] {
	return textCodec(strconv.Itoa, strconv.Atoi)
}

// Int64Codec returns a codec for signed 64-bit integers in base ten.
func Int64Codec() TextCodec[int64] {
	return textCodec(func(value int64) string { return strconv.FormatInt(value, 10) }, func(text string) (int64, error) {
		return strconv.ParseInt(text, 10, 64)
	})
}

// Uint64Codec returns a codec for unsigned 64-bit integers in base ten.
func Uint64Codec() TextCodec[uint64] {
	return textCodec(func(value uint64) string { return strconv.FormatUint(value, 10) }, func(text string) (uint64, error) {
		return strconv.ParseUint(text, 10, 64)
	})
}

// Float64Codec returns a codec for 64-bit floats using the shortest
// representation that round-trips exactly.
func Float64Codec() TextCodec[float64] {
	return textCodec(func(value float64) string { return strconv.FormatFloat(value, 'g', -1, 64) }, func(text string) (float64, error) {
		return strconv.ParseFloat(text, 64)
	})
}

// BytesCodec returns a codec that stores byte slices as unpadded base64, so
// that arbitrary binary payloads survive the line oriented record format.
func BytesCodec() TextCodec[[]byte] {
	return TextCodec[[]byte]{
		Encode: func(value []byte) (string, error) {
			return base64.RawStdEncoding.EncodeToString(value), nil
		},
		Decode: func(text string) ([]byte, error) {
			return base64.RawStdEncoding.DecodeString(text)
		},
	}
}

// TextCodecFor returns a codec that delegates to the [encoding.TextMarshaler]
// and [encoding.TextUnmarshaler] implementations of T. It makes the record
// format usable with standard library types such as time.Time, net/netip.Addr
// and net/url.URL without writing a codec by hand.
//
// The second type parameter is the pointer type of the first, which is where
// UnmarshalText usually lives:
//
//	skiplist.TextCodecFor[time.Time, *time.Time]()
func TextCodecFor[T encoding.TextMarshaler, PT interface {
	*T
	encoding.TextUnmarshaler
}]() TextCodec[T] {
	return TextCodec[T]{
		Encode: func(value T) (string, error) {
			text, err := value.MarshalText()
			if err != nil {
				return "", err
			}
			return string(text), nil
		},
		Decode: func(text string) (T, error) {
			var out T
			if err := PT(&out).UnmarshalText([]byte(text)); err != nil {
				return out, err
			}
			return out, nil
		},
	}
}
