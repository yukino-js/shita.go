package skiplist

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestScalarCodecs(t *testing.T) {
	assertCodec(t, BoolCodec(), true)
	assertCodec(t, BoolCodec(), false)
	if _, err := BoolCodec().Decode("maybe"); err == nil {
		t.Fatal("BoolCodec accepted maybe")
	}

	assertCodec(t, IntCodec(), -42)
	assertCodec(t, Int64Codec(), int64(math.MinInt64))
	assertCodec(t, Int64Codec(), int64(math.MaxInt64))
	assertCodec(t, Uint64Codec(), uint64(math.MaxUint64))
	if _, err := Uint64Codec().Decode("-1"); err == nil {
		t.Fatal("Uint64Codec accepted -1")
	}

	assertCodec(t, Float64Codec(), 1.25)
	negZero := math.Copysign(0, -1)
	text, err := Float64Codec().Encode(negZero)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Float64Codec().Decode(text)
	if err != nil || got != 0 || math.Signbit(got) != math.Signbit(negZero) {
		t.Fatalf("negative zero text %q decoded to %v signbit %t", text, got, math.Signbit(got))
	}
	nanText, err := Float64Codec().Encode(math.NaN())
	if err != nil {
		t.Fatal(err)
	}
	nan, err := Float64Codec().Decode(nanText)
	if err != nil || !math.IsNaN(nan) {
		t.Fatalf("NaN text %q decoded to %v", nanText, nan)
	}
	assertCodec(t, Float64Codec(), math.Inf(1))
	assertCodec(t, Float64Codec(), math.Inf(-1))

	payload := []byte{0, 1, 255}
	encoded, err := BytesCodec().Encode(payload)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := BytesCodec().Decode(encoded)
	if err != nil || !bytes.Equal(decoded, payload) {
		t.Fatalf("bytes = %v, %v", decoded, err)
	}

	when := time.Date(2026, 11, 3, 8, 30, 0, 0, time.UTC)
	codec := TextCodecFor[time.Time, *time.Time]()
	rendered, err := codec.Encode(when)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := codec.Decode(rendered)
	if err != nil || !restored.Equal(when) {
		t.Fatalf("time %s restored %s err %v", when, restored, err)
	}
}

func TestBytesRoundTripThroughList(t *testing.T) {
	list := New[int, []byte]()
	list.Set(1, []byte{0, 1, 255})
	list.Set(2, nil)
	var buf bytes.Buffer
	format := NewFormat(IntCodec(), BytesCodec())
	if err := list.Dump(&buf, format); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load[int, []byte](&buf, format)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := loaded.Get(1)
	if !ok || !bytes.Equal(got, []byte{0, 1, 255}) {
		t.Fatalf("Get(1) = %v, %t", got, ok)
	}
	got, ok = loaded.Get(2)
	if !ok || len(got) != 0 {
		t.Fatalf("Get(2) = %v, %t", got, ok)
	}
}

func TestDuplicateIntegerSpellings(t *testing.T) {
	// Atoi accepts both spellings of the same integer, so the second record is
	// a duplicate key rather than a second element.
	_, err := Load[int, string](strings.NewReader("1:a\n01:b\n"), NewFormat(IntCodec(), StringCodec()))
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("err %v", err)
	}
}

func assertCodec[T comparable](t *testing.T, codec TextCodec[T], value T) {
	t.Helper()
	text, err := codec.Encode(value)
	if err != nil {
		t.Fatalf("encode %v: %v", value, err)
	}
	if strings.ContainsAny(text, "\r\n") {
		t.Fatalf("codec emitted a line break: %q", text)
	}
	got, err := codec.Decode(text)
	if err != nil || got != value {
		t.Fatalf("decode %q = %v, %v; want %v", text, got, err, value)
	}
}
