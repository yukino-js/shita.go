package skiplist

import (
	"bufio"
	"bytes"
	"cmp"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRoundTripSeparatorInsideKey(t *testing.T) {
	list := New[string, string]()
	list.Set("a:b", "c:d")

	var buf bytes.Buffer
	format := NewFormat(StringCodec(), StringCodec())
	if err := list.Dump(&buf, format); err != nil {
		t.Fatalf("Dump: %v", err)
	}
	const want = "a\\:b:c\\:d\n"
	if buf.String() != want {
		t.Fatalf("dump = %q, want %q", buf.String(), want)
	}

	loaded, err := Load[string, string](bytes.NewReader(buf.Bytes()), format)
	if err != nil {
		t.Fatalf("Load %q: %v", buf.String(), err)
	}
	got, ok := loaded.Get("a:b")
	if !ok || got != "c:d" {
		t.Fatalf("Get = %q, %t", got, ok)
	}
}

func TestRecordRoundTrip(t *testing.T) {
	cases := []struct {
		key, value, want string
	}{
		{key: "1", value: "a", want: "1:a\n"},
		{key: "a:b", value: "c", want: "a\\:b:c\n"},
		{key: `a\b`, value: "c", want: "a\\\\b:c\n"},
		{key: "a\nb", value: "c\rd", want: "a\\nb:c\\rd\n"},
		{key: "", value: "", want: ":\n"},
		{key: ":", value: "", want: "\\::\n"},
		{key: "", value: ":", want: ":\\:\n"},
		{key: "你好", value: "世界", want: "你好:世界\n"},
		{key: "\xff", value: "\x00", want: "\xff:\x00\n"},
		{key: "a\tb", value: "c", want: "a\tb:c\n"},
		{key: `\t`, value: "c", want: "\\\\t:c\n"},
	}
	format := NewFormat(StringCodec(), StringCodec())
	for _, tc := range cases {
		list := New[string, string](WithMaxLevel(2))
		list.Set(tc.key, tc.value)
		var buf bytes.Buffer
		if err := list.Dump(&buf, format); err != nil {
			t.Fatalf("Dump %q: %v", tc.key, err)
		}
		if buf.String() != tc.want {
			t.Fatalf("key %q value %q: dump = %q, want %q", tc.key, tc.value, buf.String(), tc.want)
		}
		loaded, err := Load[string, string](&buf, format, WithMaxLevel(2))
		if err != nil {
			t.Fatalf("Load %q: %v", tc.want, err)
		}
		got, ok := loaded.Get(tc.key)
		if !ok || got != tc.value || loaded.Len() != 1 {
			t.Fatalf("round trip key %q = %q, %t, len %d", tc.key, got, ok, loaded.Len())
		}
	}
}

func TestDumpFollowsListOrder(t *testing.T) {
	list := New[int, string]()
	list.Set(2, "b")
	list.Set(1, "a")
	var buf bytes.Buffer
	format := NewFormat(IntCodec(), StringCodec())
	if err := list.Dump(&buf, format); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "1:a\n2:b\n" {
		t.Fatalf("dump = %q", buf.String())
	}

	reversed := NewWithComparator[string, string](
		func(a, b string) int { return cmp.Compare(b, a) },
	)
	reversed.Set("a", "1")
	reversed.Set("b", "2")
	buf.Reset()
	if err := reversed.Dump(&buf, NewFormat(StringCodec(), StringCodec())); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "b:2\na:1\n" {
		t.Fatalf("reverse dump = %q", buf.String())
	}
}

func TestLegacyDumpOrdersKeys(t *testing.T) {
	// Byte-for-byte sample of Skiplist-CPP/store/dumpFile. The values include
	// the semicolons that were stored, and the keys are not sorted.
	const legacy = "1:a;\n19:yang;\n3:b;\n7:c;\n8:sun;\n9:xiu;\n"
	format := NewFormat(IntCodec(), StringCodec())
	loaded, err := Load[int, string](strings.NewReader(legacy), format)
	if err != nil {
		t.Fatal(err)
	}
	want := []Entry[int, string]{
		{Key: 1, Value: "a;"},
		{Key: 3, Value: "b;"},
		{Key: 7, Value: "c;"},
		{Key: 8, Value: "sun;"},
		{Key: 9, Value: "xiu;"},
		{Key: 19, Value: "yang;"},
	}
	if !slices.Equal(loaded.Snapshot(), want) {
		t.Fatalf("snapshot = %#v", loaded.Snapshot())
	}
}

func TestEmptyValueRoundTrips(t *testing.T) {
	format := NewFormat(IntCodec(), StringCodec())
	loaded, err := Load[int, string](strings.NewReader("1:\n2:b\n"), format)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := loaded.Get(1)
	if !ok || got != "" {
		t.Fatalf("empty value = %q, %t", got, ok)
	}
	if got, ok = loaded.Get(2); !ok || got != "b" {
		t.Fatalf("Get(2) = %q, %t", got, ok)
	}
}

func TestValueMayContainUnescapedSeparator(t *testing.T) {
	// A legacy record has no escapes. The value is everything after the first
	// separator, which is how the C++ loader split the line.
	format := NewFormat(IntCodec(), StringCodec())
	loaded, err := Load[int, string](strings.NewReader("1:a:b\n"), format)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := loaded.Get(1)
	if !ok || got != "a:b" {
		t.Fatalf("Get(1) = %q, %t", got, ok)
	}
}

func TestMalformedLineDoesNotReplayPreviousRecord(t *testing.T) {
	format := NewFormat(IntCodec(), StringCodec())
	loaded, err := Load[int, string](strings.NewReader("1:a\nthis line has no separator\n2:b\n"), format)
	if loaded != nil {
		t.Fatal("Load returned a partial list")
	}
	var parse *ParseError
	if !errors.As(err, &parse) {
		t.Fatalf("error = %v", err)
	}
	if parse.Line != 2 || !errors.Is(err, ErrMalformedRecord) {
		t.Fatalf("line %d err %v", parse.Line, err)
	}
}

func TestLoadSkipsBlankLinesAndAcceptsCRLF(t *testing.T) {
	format := NewFormat(IntCodec(), StringCodec())
	loaded, err := Load[int, string](strings.NewReader("\r\n1:a\r\n\r\n2:b\r\n"), format)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Len() != 2 {
		t.Fatalf("Len = %d", loaded.Len())
	}
	if got, _ := loaded.Get(1); got != "a" {
		t.Fatalf("Get(1) = %q", got)
	}
}

func TestLoadRejectsBadRecords(t *testing.T) {
	intFormat := NewFormat(IntCodec(), StringCodec())
	stringFormat := NewFormat(StringCodec(), StringCodec())

	cases := []struct {
		name    string
		format  Format[string, string]
		intForm bool
		input   string
		line    int
		target  error
	}{
		{name: "invalid escape in value", intForm: true, input: "1:a\\x\n", line: 1, target: ErrInvalidEscape},
		{name: "invalid escape in key", input: "\\q:1\n", line: 1, target: ErrInvalidEscape},
		{name: "duplicate", intForm: true, input: "1:a\n1:b\n", line: 2, target: ErrDuplicateKey},
		{name: "syntax", intForm: true, input: "nope:value\n", line: 1, target: strconv.ErrSyntax},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.intForm {
				_, err = Load[int, string](strings.NewReader(tc.input), intFormat)
			} else {
				_, err = Load[string, string](strings.NewReader(tc.input), stringFormat)
			}
			var parse *ParseError
			if !errors.As(err, &parse) || parse.Line != tc.line || !errors.Is(err, tc.target) {
				t.Fatalf("line %d err %v", parseLine(parse), err)
			}
		})
	}
}

func TestLoadLineNumbersCountBlankLines(t *testing.T) {
	_, err := Load[int, string](strings.NewReader("\n1:a\n\nnope\n"), NewFormat(IntCodec(), StringCodec()))
	var parse *ParseError
	if !errors.As(err, &parse) || parse.Line != 4 {
		t.Fatalf("err %v", err)
	}
}

func TestParseErrorSummarisesLongAndInvalidRecords(t *testing.T) {
	key := strings.Repeat("k", 40)
	input := key + ":v\n" + key + ":w\n"
	_, err := Load[string, string](strings.NewReader(input), NewFormat(StringCodec(), StringCodec()))
	if !errors.Is(err, ErrDuplicateKey) || !strings.Contains(err.Error(), "...") {
		t.Fatalf("err %v", err)
	}

	long := strings.Repeat("a", 31) + "\xff\xff\xff\n"
	_, err = Load[string, string](strings.NewReader(long), NewFormat(StringCodec(), StringCodec()))
	if !errors.Is(err, ErrMalformedRecord) || !strings.Contains(err.Error(), "...") {
		t.Fatalf("err %v", err)
	}
}

func TestRecordTooLong(t *testing.T) {
	payload := "1:" + strings.Repeat("a", MaxRecordSize) + "\n"
	_, err := Load[int, string](strings.NewReader(payload), NewFormat(IntCodec(), StringCodec()))
	if !errors.Is(err, bufio.ErrTooLong) {
		t.Fatalf("err %v", err)
	}
}

func TestCustomSeparator(t *testing.T) {
	format := NewFormat(StringCodec(), StringCodec())
	format.Separator = '|'
	list := New[string, string]()
	list.Set("a|b", "c")
	var buf bytes.Buffer
	if err := list.Dump(&buf, format); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "a\\|b|c\n" {
		t.Fatalf("dump = %q", buf.String())
	}
	loaded, err := Load[string, string](&buf, format)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := loaded.Get("a|b"); !ok || got != "c" {
		t.Fatalf("Get = %q, %t", got, ok)
	}

	tilde := NewFormat(StringCodec(), StringCodec())
	tilde.Separator = '~'
	list = New[string, string]()
	list.Set("a", "b")
	buf.Reset()
	if err := list.Dump(&buf, tilde); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "a~b\n" {
		t.Fatalf("dump = %q", buf.String())
	}
}

func TestSeparatorAndCodecValidation(t *testing.T) {
	list := New[string, string]()
	list.Set("a", "b")
	for _, separator := range []byte{'n', 'r', '\\', ' ', 0x7f} {
		format := NewFormat(StringCodec(), StringCodec())
		format.Separator = separator
		if err := list.Dump(io.Discard, format); err == nil {
			t.Fatalf("separator %q was accepted", separator)
		}
	}

	broken := NewFormat(TextCodec[string]{}, StringCodec())
	if err := list.Dump(io.Discard, broken); err == nil {
		t.Fatal("missing key codec was accepted")
	}
	brokenValue := NewFormat(StringCodec(), TextCodec[string]{})
	if err := list.Dump(io.Discard, brokenValue); err == nil {
		t.Fatal("missing value codec was accepted")
	}
	failKey := NewFormat(TextCodec[string]{
		Encode: func(string) (string, error) { return "", errors.New("key") },
		Decode: func(string) (string, error) { return "", nil },
	}, StringCodec())
	if err := list.Dump(io.Discard, failKey); err == nil || !strings.Contains(err.Error(), "key") {
		t.Fatalf("key encode err = %v", err)
	}

	implicit := Format[string, string]{Key: StringCodec(), Value: StringCodec()}
	var buf bytes.Buffer
	if err := list.Dump(&buf, implicit); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "a:b\n" {
		t.Fatalf("zero separator dump = %q", buf.String())
	}
}

func TestLoadOptionAndComparatorErrors(t *testing.T) {
	format := NewFormat(IntCodec(), IntCodec())
	if _, err := Load[int, int](strings.NewReader(""), format, WithMaxLevel(0)); err == nil {
		t.Fatal("Load accepted an invalid option")
	}
	if _, err := LoadWithComparator[int, int](nil, strings.NewReader(""), format); err == nil {
		t.Fatal("LoadWithComparator accepted a nil comparator")
	}
	loaded, err := Load[int, int](strings.NewReader("2:20\n1:10\n"), format, WithMaxLevel(4))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.MaxLevel() != 4 || !slices.Equal(keysOf(loaded), []int{1, 2}) {
		t.Fatalf("loaded max=%d keys=%v", loaded.MaxLevel(), keysOf(loaded))
	}
}

func TestDumpFileReplacesAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "dump.txt")
	format := NewFormat(IntCodec(), StringCodec())

	list := New[int, string]()
	list.Set(1, "a")
	list.Set(2, "b")
	if err := list.DumpFile(path, format); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o644 {
		t.Fatalf("mode = %o", info.Mode().Perm())
	}
	loaded, err := LoadFile[int, string](path, format)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(list.Snapshot(), loaded.Snapshot()) {
		t.Fatalf("file snapshot = %#v", loaded.Snapshot())
	}
	if temps := tempDumpFiles(t, filepath.Dir(path)); len(temps) != 0 {
		t.Fatalf("temporary files left behind: %v", temps)
	}

	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	broken := NewFormat(TextCodec[int]{
		Encode: func(int) (string, error) { return "", errors.New("boom") },
		Decode: func(string) (int, error) { return 0, errors.New("unused") },
	}, StringCodec())
	if err := list.DumpFile(path, broken); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("DumpFile err = %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("failed dump changed %q into %q", original, got)
	}
	if temps := tempDumpFiles(t, filepath.Dir(path)); len(temps) != 0 {
		t.Fatalf("temporary files left after a failed dump: %v", temps)
	}

	missing := filepath.Join(dir, "missing", "dump.txt")
	if err := list.DumpFile(missing, broken); err == nil {
		t.Fatal("expected encode failure")
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat missing dump: %v", err)
	}
}

func TestLoadFileErrorsIncludePath(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "absent.txt")
	format := NewFormat(IntCodec(), StringCodec())
	if _, err := LoadFile[int, string](missing, format); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err %v", err)
	}

	path := filepath.Join(dir, "bad.txt")
	if err := os.WriteFile(path, []byte("not valid\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadFile[int, string](path, format)
	var parse *ParseError
	if !errors.As(err, &parse) || !errors.Is(err, ErrMalformedRecord) || !strings.Contains(err.Error(), path) {
		t.Fatalf("err %v", err)
	}
}

func TestDumpReentersList(t *testing.T) {
	list := New[int, string](WithMaxLevel(1))
	list.Set(1, "a")
	writer := &reenterWriter{list: list}
	done := make(chan error, 1)
	go func() {
		done <- list.Dump(writer, NewFormat(IntCodec(), StringCodec()))
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Dump held the list lock while writing")
	}
	if writer.buf.String() != "1:a\n" {
		t.Fatalf("dump observed by the re-entrant writer = %q", writer.buf.String())
	}
	if _, ok := list.Get(2); !ok {
		t.Fatal("re-entrant Set did not land")
	}
}

func TestDumpWriteAndEncodeErrors(t *testing.T) {
	format := NewFormat(IntCodec(), StringCodec())
	empty := New[int, string]()
	counter := &countingWriter{}
	if err := empty.Dump(counter, format); err != nil {
		t.Fatal(err)
	}
	if counter.n != 0 {
		t.Fatalf("empty dump wrote %d times", counter.n)
	}

	list := New[int, string]()
	list.Set(1, "a")
	if err := list.Dump(failWriter{}, format); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("err %v", err)
	}

	failing := NewFormat(TextCodec[int]{
		Encode: func(value int) (string, error) {
			if value == 2 {
				return "", errors.New("second")
			}
			return strconv.Itoa(value), nil
		},
		Decode: strconv.Atoi,
	}, StringCodec())
	list.Set(2, "b")
	counter = &countingWriter{}
	err := list.Dump(counter, failing)
	if err == nil || !strings.Contains(err.Error(), "second") {
		t.Fatalf("err %v", err)
	}
	if counter.n != 1 {
		t.Fatalf("writes before the encode failure = %d", counter.n)
	}
}

func TestSplitAndUnescape(t *testing.T) {
	key, value, ok := splitRecord(`a\:b:c\:d`, ':')
	if !ok || key != `a\:b` || value != `c\:d` {
		t.Fatalf("split = %q, %q, %t", key, value, ok)
	}
	if _, _, ok := splitRecord(`a\:b`, ':'); ok {
		t.Fatal("an escaped separator was treated as the field break")
	}

	got, err := unescapeField(`a\:b\\c\n\r`, ':')
	if err != nil || got != "a:b\\c\n\r" {
		t.Fatalf("unescape = %q, %v", got, err)
	}
	if _, err := unescapeField(`\`, ':'); !errors.Is(err, ErrInvalidEscape) {
		t.Fatalf("lone escape err %v", err)
	}
	if _, err := unescapeField(`\t`, ':'); !errors.Is(err, ErrInvalidEscape) {
		t.Fatalf("unknown escape err %v", err)
	}
	plain, err := unescapeField("plain", ':')
	if err != nil || plain != "plain" {
		t.Fatalf("plain = %q, %v", plain, err)
	}
}

func FuzzRoundTrip(f *testing.F) {
	f.Add("plain", "value")
	f.Add("a:b", "c:d")
	f.Add(`a\b`, "c")
	f.Add("a\nb", "c\r\nd")
	f.Add("", "")
	f.Add(":", ":")
	f.Add("你好", "世界")
	f.Add("\xff", "\x00")
	f.Fuzz(func(t *testing.T, key, value string) {
		list := New[string, string](WithMaxLevel(4))
		list.Set(key, value)
		var buf bytes.Buffer
		format := NewFormat(StringCodec(), StringCodec())
		if err := list.Dump(&buf, format); err != nil {
			t.Fatalf("Dump: %v", err)
		}
		loaded, err := Load[string, string](bytes.NewReader(buf.Bytes()), format, WithMaxLevel(4))
		if err != nil {
			t.Fatalf("Load %q: %v", buf.String(), err)
		}
		got, ok := loaded.Get(key)
		if !ok || got != value || loaded.Len() != 1 {
			t.Fatalf("Get(%q) = %q, %t len %d; dump %q", key, got, ok, loaded.Len(), buf.String())
		}
	})
}

func tempDumpFiles(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, ".dump.txt.tmp-*"))
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

func parseLine(err *ParseError) int {
	if err == nil {
		return 0
	}
	return err.Line
}

type reenterWriter struct {
	list *SkipList[int, string]
	buf  bytes.Buffer
}

func (w *reenterWriter) Write(p []byte) (int, error) {
	w.list.Set(2, "b")
	return w.buf.Write(p)
}

type countingWriter struct{ n int }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n++
	return len(p), nil
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
