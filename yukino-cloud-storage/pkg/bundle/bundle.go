// Package bundle provides a self-describing compression container used for the
// "deep storage" tier of the cloud storage service.
//
// It is a Go migration of the C++ project's use of the third-party "bundle"
// compression library. Rather than binding a C toolkit, the container is built
// entirely on the Go standard library compress packages (gzip, zlib and flate)
// while preserving the original idea: a small header records the codec and the
// original size so a container can be detected and decoded without any
// out-of-band metadata.
package bundle

import (
	"bufio"
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Format identifies the codec used inside a container.
type Format uint8

const (
	// FormatRaw stores the payload verbatim. Useful for already-compressed data.
	FormatRaw Format = iota
	// FormatGzip uses DEFLATE with gzip framing and a CRC-32 checksum.
	FormatGzip
	// FormatZlib uses DEFLATE with zlib framing and an Adler-32 checksum.
	FormatZlib
	// FormatFlate uses raw DEFLATE without framing or a checksum.
	FormatFlate
)

// DefaultLevel selects the codec's default compression level.
const DefaultLevel = flate.DefaultCompression

const (
	magic   = "YKBD"
	version = 1
)

// Errors reported by the container codec.
var (
	ErrNotPacked    = errors.New("bundle: data is not a bundle container")
	ErrBadVersion   = errors.New("bundle: unsupported container version")
	ErrBadFormat    = errors.New("bundle: unknown compression format")
	ErrSizeMismatch = errors.New("bundle: decoded size does not match header")
)

// String returns the lower-case canonical name of the format.
func (f Format) String() string {
	switch f {
	case FormatRaw:
		return "raw"
	case FormatGzip:
		return "gzip"
	case FormatZlib:
		return "zlib"
	case FormatFlate:
		return "flate"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(f))
	}
}

// ParseFormat maps a canonical name onto a Format. The names are case
// insensitive: raw, gzip, zlib and flate (deflate is an alias for flate).
//
// The original C++ configuration selected a codec by integer tied to the
// bundle library's own enum. Those integers have no meaning for the Go codecs,
// so the configuration now uses an explicit name instead.
func ParseFormat(s string) (Format, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "raw", "none", "store":
		return FormatRaw, nil
	case "gzip", "gz":
		return FormatGzip, nil
	case "zlib":
		return FormatZlib, nil
	case "flate", "deflate":
		return FormatFlate, nil
	default:
		return FormatRaw, fmt.Errorf("bundle: unknown format %q", s)
	}
}

// IsPacked reports whether data begins with a bundle container header.
func IsPacked(data []byte) bool {
	return len(data) >= len(magic)+2 && string(data[:len(magic)]) == magic
}

// encodeHeader builds the container header. An originalSize of zero means the
// size is unknown, which is the case for streaming writers.
func encodeHeader(format Format, originalSize uint64) []byte {
	buf := make([]byte, 0, len(magic)+2+binary.MaxVarintLen64)
	buf = append(buf, magic...)
	buf = append(buf, version, uint8(format))
	buf = binary.AppendUvarint(buf, originalSize)
	return buf
}

// decodeHeader parses the header and returns the codec, the declared original
// size and the remaining compressed body.
func decodeHeader(data []byte) (Format, uint64, []byte, error) {
	if !IsPacked(data) {
		return 0, 0, nil, ErrNotPacked
	}
	if data[len(magic)] != version {
		return 0, 0, nil, ErrBadVersion
	}
	format := Format(data[len(magic)+1])
	if format > FormatFlate {
		return 0, 0, nil, ErrBadFormat
	}
	rest := data[len(magic)+2:]
	originalSize, n := binary.Uvarint(rest)
	if n <= 0 {
		return 0, 0, nil, ErrNotPacked
	}
	return format, originalSize, rest[n:], nil
}

// Pack compresses data into a self-describing container. level is a
// compress/flate level; pass DefaultLevel for the codec default.
func Pack(format Format, level int, data []byte) ([]byte, error) {
	var body bytes.Buffer
	w, err := newWriter(&body, format, level)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	out := encodeHeader(format, uint64(len(data)))
	return append(out, body.Bytes()...), nil
}

// Unpack decodes a container produced by Pack. When the header declares a known
// original size the decoded length is verified against it.
func Unpack(container []byte) ([]byte, error) {
	format, originalSize, body, err := decodeHeader(container)
	if err != nil {
		return nil, err
	}
	r, err := newReader(bytes.NewReader(body), format)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	if originalSize == 0 {
		return io.ReadAll(r)
	}
	// Read at most one byte beyond the declared size so a corrupt or hostile
	// header cannot drive a large allocation.
	out, err := io.ReadAll(io.LimitReader(r, int64(originalSize)+1))
	if err != nil {
		return nil, err
	}
	if uint64(len(out)) != originalSize {
		return nil, ErrSizeMismatch
	}
	return out, nil
}

// NewWriter returns a WriteCloser that compresses into w using format. The
// container header is written immediately with an unknown original size, so the
// payload can be streamed. The caller must Close the returned writer to finish
// the compressed stream.
func NewWriter(w io.Writer, format Format, level int) (io.WriteCloser, error) {
	if _, err := w.Write(encodeHeader(format, 0)); err != nil {
		return nil, err
	}
	return newWriter(w, format, level)
}

// NewReader returns a ReadCloser that decodes a container read from r. It
// consumes the header first; the returned reader yields the decompressed
// payload until EOF.
func NewReader(r io.Reader) (io.ReadCloser, error) {
	br := bufio.NewReader(r)
	head := make([]byte, len(magic)+2)
	if _, err := io.ReadFull(br, head); err != nil {
		return nil, err
	}
	if string(head[:len(magic)]) != magic {
		return nil, ErrNotPacked
	}
	if head[len(magic)] != version {
		return nil, ErrBadVersion
	}
	format := Format(head[len(magic)+1])
	if format > FormatFlate {
		return nil, ErrBadFormat
	}
	// The declared size is informational for a streaming reader.
	if _, err := binary.ReadUvarint(br); err != nil {
		return nil, err
	}
	return newReader(br, format)
}

func newWriter(w io.Writer, format Format, level int) (io.WriteCloser, error) {
	switch format {
	case FormatRaw:
		return nopWriteCloser{w}, nil
	case FormatGzip:
		if level == DefaultLevel {
			return gzip.NewWriter(w), nil
		}
		return gzip.NewWriterLevel(w, level)
	case FormatZlib:
		if level == DefaultLevel {
			return zlib.NewWriter(w), nil
		}
		return zlib.NewWriterLevel(w, level)
	case FormatFlate:
		return flate.NewWriter(w, level)
	default:
		return nil, ErrBadFormat
	}
}

func newReader(r io.Reader, format Format) (io.ReadCloser, error) {
	switch format {
	case FormatRaw:
		return io.NopCloser(r), nil
	case FormatGzip:
		return gzip.NewReader(r)
	case FormatZlib:
		return zlib.NewReader(r)
	case FormatFlate:
		return flate.NewReader(r), nil
	default:
		return nil, ErrBadFormat
	}
}

// nopWriteCloser adapts a plain writer to the WriteCloser interface for the
// uncompressed format.
type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }
