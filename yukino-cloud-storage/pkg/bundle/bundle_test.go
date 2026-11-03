package bundle

import (
	"bytes"
	"io"
	"testing"
)

func TestPackUnpackRoundtrip(t *testing.T) {
	data := bytes.Repeat([]byte("yukino cloud storage "), 1000)
	for _, format := range []Format{FormatRaw, FormatGzip, FormatZlib, FormatFlate} {
		packed, err := Pack(format, DefaultLevel, data)
		if err != nil {
			t.Fatalf("%s: pack: %v", format, err)
		}
		if !IsPacked(packed) {
			t.Fatalf("%s: container not detected as packed", format)
		}
		unpacked, err := Unpack(packed)
		if err != nil {
			t.Fatalf("%s: unpack: %v", format, err)
		}
		if !bytes.Equal(unpacked, data) {
			t.Fatalf("%s: roundtrip mismatch", format)
		}
	}
}

func TestCompressedSmaller(t *testing.T) {
	data := bytes.Repeat([]byte("aaaaaaaaaa"), 10000)
	packed, err := Pack(FormatGzip, DefaultLevel, data)
	if err != nil {
		t.Fatal(err)
	}
	if len(packed) >= len(data) {
		t.Fatalf("gzip did not compress: %d >= %d", len(packed), len(data))
	}
}

func TestStreamingRoundtrip(t *testing.T) {
	data := bytes.Repeat([]byte("streaming data "), 5000)
	var buf bytes.Buffer
	w, err := NewWriter(&buf, FormatGzip, DefaultLevel)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(data); i += 1024 {
		end := i + 1024
		if end > len(data) {
			end = len(data)
		}
		if _, err := w.Write(data[i:end]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("streaming roundtrip mismatch")
	}
}

func TestParseFormat(t *testing.T) {
	cases := map[string]Format{
		"gzip": FormatGzip, "raw": FormatRaw, "zlib": FormatZlib,
		"flate": FormatFlate, "deflate": FormatFlate,
	}
	for in, want := range cases {
		got, err := ParseFormat(in)
		if err != nil || got != want {
			t.Fatalf("ParseFormat(%q) = %v, %v", in, got, err)
		}
	}
	if _, err := ParseFormat("bogus"); err == nil {
		t.Fatal("expected error for unknown format")
	}
}

func TestUnpackRejectsGarbage(t *testing.T) {
	if _, err := Unpack([]byte("not a bundle container")); err == nil {
		t.Fatal("expected error for non-container input")
	}
}
