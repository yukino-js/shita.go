package yukino_log

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestOpenRejectsBadOptions(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "app")
	cases := []struct {
		name string
		base string
		roll int64
		opts FileOptions
		want error
	}{
		{name: "empty", base: "", roll: 10, want: ErrBasename},
		{name: "slash", base: filepath.Join(dir, "logs") + string(filepath.Separator), roll: 10, want: ErrBasename},
		{name: "dot", base: ".", roll: 10, want: ErrBasename},
		{name: "zero roll", base: base, roll: 0, want: ErrRollSize},
		{name: "negative roll", base: base, roll: -1, want: ErrRollSize},
		{name: "negative flush", base: base, roll: 10, opts: FileOptions{FlushInterval: -time.Second}, want: ErrFlushInterval},
		{name: "negative check", base: base, roll: 10, opts: FileOptions{CheckEveryN: -1}, want: ErrCheckEvery},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Open(tc.base, tc.roll, tc.opts)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestOpenCreatesParentDirectory(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "nested", "app")
	file := openFile(t, base, 1<<20, FileOptions{})
	if _, err := os.Stat(filepath.Dir(file.Name())); err != nil {
		t.Fatal(err)
	}
}

func TestOpenParentIsFile(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(filepath.Join(blocker, "app"), 100, FileOptions{})
	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestExactRollBoundary(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "app")
	file := openFile(t, base, 10, FileOptions{Now: frozen(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)), Location: time.UTC})
	if _, err := file.Write([]byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	if got := fileCount(t, dir); got != 1 {
		t.Fatalf("exact roll size created %d files", got)
	}
	if _, err := file.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	files := readLogs(t, dir, base)
	if len(files) != 2 {
		t.Fatalf("files: %+v", names(files))
	}
	if string(files[0].data) != "0123456789" {
		t.Fatalf("first file %q", files[0].data)
	}
	if string(files[1].data) != "x" {
		t.Fatalf("successor %q", files[1].data)
	}
}

func TestRollKeepsRecordsIntact(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "app")
	when := time.Date(2024, 4, 5, 6, 7, 8, 0, time.UTC)
	file := openFile(t, base, 10, FileOptions{Now: frozen(when), Location: time.UTC})
	if _, err := file.Write([]byte("aaaa\nbbbb\nc\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("0123456789ABCDEF\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("z\n")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	files := readLogs(t, dir, base)
	got := make([]string, len(files))
	for i, file := range files {
		got[i] = string(file.data)
		if file.data == nil || !bytes.HasSuffix(file.data, []byte("\n")) {
			t.Fatalf("torn or empty file %s %q", file.name, file.data)
		}
	}
	want := []string{"aaaa\nbbbb\n", "c\n", "0123456789ABCDEF\n", "z\n"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("files %q", got)
	}
}

func TestSameSecondRollKeepsEveryByte(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "app")
	when := time.Date(2024, 6, 7, 8, 9, 10, 0, time.UTC)
	file := openFile(t, base, 8, FileOptions{Now: frozen(when), Location: time.UTC, CheckEveryN: 1})
	var want bytes.Buffer
	for i := 0; i < 5; i++ {
		chunk := []byte("0123456789")
		want.Write(chunk)
		if _, err := file.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	files := readLogs(t, dir, base)
	if len(files) < 2 {
		t.Fatalf("expected a same-second successor, got %d", len(files))
	}
	for _, file := range files[1:] {
		if file.seq == 0 && len(file.data) > 0 {
			t.Fatalf("successor reused the original name %s", file.name)
		}
	}
	if got := joinData(files); got != want.String() {
		t.Fatalf("got %q want %q", got, want.String())
	}
	for i, file := range files {
		if len(file.data) == 0 {
			t.Fatalf("empty roll %s", file.name)
		}
		if i > 0 && file.name < files[i-1].name {
			t.Fatalf("directory order %s then %s", files[i-1].name, file.name)
		}
	}
	for i, file := range files {
		if file.stamp != "20240607-080910" {
			t.Fatalf("file %d stamp %s", i, file.stamp)
		}
	}
}

func TestRestartCountsExistingBytes(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "app")
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	opts := FileOptions{Now: frozen(when), Location: time.UTC}
	first := openFile(t, base, 100, opts)
	if _, err := first.Write(bytes.Repeat([]byte("a"), 80)); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := openFile(t, base, 100, opts)
	if second.Written() != 80 {
		t.Fatalf("reopened size %d", second.Written())
	}
	if _, err := second.Write(bytes.Repeat([]byte("b"), 30)); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Write([]byte("c")); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	files := readLogs(t, dir, base)
	if len(files) != 2 {
		t.Fatalf("files %+v", names(files))
	}
	if string(files[0].data) != strings.Repeat("a", 80) {
		t.Fatalf("first %q", files[0].data)
	}
	if string(files[1].data) != strings.Repeat("b", 30)+"c" {
		t.Fatalf("second %q", files[1].data)
	}
}

func TestRestartSkipsFileAlreadyOverLimit(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "app")
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	opts := FileOptions{Now: frozen(when), Location: time.UTC}
	first := openFile(t, base, 1000, opts)
	payload := bytes.Repeat([]byte("a"), 150)
	if _, err := first.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := openFile(t, base, 100, opts)
	if _, err := second.Write([]byte("z")); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	files := readLogs(t, dir, base)
	if len(files) != 2 {
		t.Fatalf("files %+v", names(files))
	}
	if string(files[0].data) != string(payload) {
		t.Fatalf("original was rewritten: %d bytes", len(files[0].data))
	}
	if string(files[1].data) != "z" {
		t.Fatalf("successor %q", files[1].data)
	}
}

func TestDailyBoundaryUsesLocalMidnight(t *testing.T) {
	loc := time.FixedZone("CST", 8*3600)
	// 08:00 CST is UTC midnight. These two instants are the same local day
	// and different UTC days. A UTC-aligned period would roll here.
	early := time.Date(2024, 3, 1, 7, 30, 0, 0, loc)
	later := time.Date(2024, 3, 1, 8, 30, 0, 0, loc)
	dir := t.TempDir()
	base := filepath.Join(dir, "app")
	clock := &fakeClock{t: early}
	file := openFile(t, base, 1<<20, FileOptions{Now: clock.Now, Location: loc, CheckEveryN: 1})
	if _, err := file.Write([]byte("early")); err != nil {
		t.Fatal(err)
	}
	clock.Set(later)
	if _, err := file.Write([]byte("later")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	files := readLogs(t, dir, base)
	if len(files) != 1 {
		t.Fatalf("rolled across UTC midnight: %+v", names(files))
	}
	if string(files[0].data) != "earlylater" {
		t.Fatalf("body %q", files[0].data)
	}

	dir = t.TempDir()
	base = filepath.Join(dir, "app")
	before := time.Date(2024, 3, 1, 23, 30, 0, 0, loc)
	after := time.Date(2024, 3, 2, 0, 30, 0, 0, loc)
	clock = &fakeClock{t: before}
	file = openFile(t, base, 1<<20, FileOptions{Now: clock.Now, Location: loc, CheckEveryN: 1})
	if _, err := file.Write([]byte("old")); err != nil {
		t.Fatal(err)
	}
	clock.Set(after)
	if _, err := file.Write([]byte("cross")); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	files = readLogs(t, dir, base)
	if len(files) < 2 {
		t.Fatalf("expected a new local day, got %+v", names(files))
	}
	if files[0].stamp[:8] != "20240301" || files[1].stamp[:8] != "20240302" {
		t.Fatalf("stamps %s %s", files[0].stamp, files[1].stamp)
	}
	if joinData(files) != "oldcrossnew" {
		t.Fatalf("body %q", joinData(files))
	}
	if !bytes.Contains(files[1].data, []byte("new")) {
		t.Fatalf("new-day file %q", files[1].data)
	}
}

func TestFileFlushInterval(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "app")
	start := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: start}
	file := openFile(t, base, 1<<20, FileOptions{
		Now:           clock.Now,
		Location:      time.UTC,
		FlushInterval: 5 * time.Second,
		CheckEveryN:   100,
	})
	if _, err := file.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if size := fileSize(t, file.Name()); size != 0 {
		t.Fatalf("buffered bytes reached disk: %d", size)
	}
	clock.Set(start.Add(5 * time.Second))
	if _, err := file.Write([]byte("!")); err != nil {
		t.Fatal(err)
	}
	if size := fileSize(t, file.Name()); size != int64(len("hello!")) {
		t.Fatalf("size %d", size)
	}
}

func TestConcurrentFileAppend(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "app")
	file := openFile(t, base, 64, FileOptions{Location: time.UTC})
	const workers = 8
	const per = 40
	var group sync.WaitGroup
	group.Add(workers)
	for id := 0; id < workers; id++ {
		go func(id int) {
			defer group.Done()
			for i := 0; i < per; i++ {
				line := []byte(strconv.Itoa(id) + "-" + strconv.Itoa(i) + "\n")
				if _, err := file.Write(line); err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
		}(id)
	}
	group.Wait()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	body := joinData(readLogs(t, dir, base))
	for id := 0; id < workers; id++ {
		for i := 0; i < per; i++ {
			line := strconv.Itoa(id) + "-" + strconv.Itoa(i) + "\n"
			if !strings.Contains(body, line) {
				t.Fatalf("missing %s", line)
			}
		}
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	file := openFile(t, filepath.Join(dir, "app"), 100, FileOptions{})
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v", err)
	}
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Set(t time.Time) {
	c.mu.Lock()
	c.t = t
	c.mu.Unlock()
}

func frozen(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

func openFile(t *testing.T, basename string, roll int64, opts FileOptions) *LogFile {
	t.Helper()
	file, err := Open(basename, roll, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file
}

type rolledFile struct {
	stamp string
	seq   int
	name  string
	data  []byte
}

func readLogs(t *testing.T, dir, basename string) []rolledFile {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	prefix := filepath.Base(basename) + "."
	var files []rolledFile
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		stamp, seq, ok := parseLogName(prefix, entry.Name())
		if !ok {
			t.Fatalf("unexpected log name %s", entry.Name())
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, rolledFile{stamp: stamp, seq: seq, name: entry.Name(), data: data})
	}
	for i := 1; i < len(files); i++ {
		for j := i; j > 0; j-- {
			if lessLog(files[j], files[j-1]) {
				files[j], files[j-1] = files[j-1], files[j]
			}
		}
	}
	return files
}

func parseLogName(prefix, name string) (stamp string, seq int, ok bool) {
	if !strings.HasPrefix(name, prefix) {
		return "", 0, false
	}
	rest := strings.TrimPrefix(name, prefix)
	const stampLen = len("20060102-150405")
	if len(rest) < stampLen+len(".log") || rest[stampLen:stampLen+4] != ".log" {
		return "", 0, false
	}
	stamp = rest[:stampLen]
	tail := rest[stampLen+4:]
	if tail == "" {
		return stamp, 0, true
	}
	if len(tail) != 7 || tail[0] != '.' {
		return "", 0, false
	}
	n, err := strconv.Atoi(tail[1:])
	if err != nil {
		return "", 0, false
	}
	return stamp, n, true
}

func lessLog(a, b rolledFile) bool {
	if a.stamp != b.stamp {
		return a.stamp < b.stamp
	}
	return a.seq < b.seq
}

func joinData(files []rolledFile) string {
	var b strings.Builder
	for _, file := range files {
		b.Write(file.data)
	}
	return b.String()
}

func names(files []rolledFile) []string {
	out := make([]string, len(files))
	for i, file := range files {
		out[i] = file.name
	}
	return out
}

func fileCount(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			n++
		}
	}
	return n
}

func fileSize(t *testing.T, name string) int64 {
	t.Helper()
	info, err := os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}
