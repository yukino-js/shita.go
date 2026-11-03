package asynclog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBufferPushAndBytes(t *testing.T) {
	b := NewBuffer(Growth{Base: 8, Threshold: 64, Linear: 16})
	b.Push([]byte("hello"))
	if got := string(b.Bytes()); got != "hello" {
		t.Fatalf("Bytes() = %q, want %q", got, "hello")
	}
	if b.Len() != 5 || b.IsEmpty() {
		t.Fatalf("Len() = %d, IsEmpty() = %v", b.Len(), b.IsEmpty())
	}
}

func TestBufferGrowsToFit(t *testing.T) {
	b := NewBuffer(Growth{Base: 4, Threshold: 1 << 30, Linear: 4})
	// A record larger than the base capacity must be accommodated, not overflow.
	b.Push(make([]byte, 100))
	if b.Cap() < 100 {
		t.Fatalf("Cap() = %d, want >= 100", b.Cap())
	}
	if b.Len() != 100 {
		t.Fatalf("Len() = %d, want 100", b.Len())
	}
}

func TestBufferSwapAndReset(t *testing.T) {
	a := NewBuffer(DefaultGrowth())
	b := NewBuffer(DefaultGrowth())
	a.Push([]byte("abc"))
	a.Swap(b)
	if !a.IsEmpty() {
		t.Fatal("source buffer should be empty after swap")
	}
	if string(b.Bytes()) != "abc" {
		t.Fatalf("destination buffer = %q, want %q", b.Bytes(), "abc")
	}
	b.Reset()
	if !b.IsEmpty() {
		t.Fatal("buffer should be empty after reset")
	}
}

func TestWorkerDrainsOnStop(t *testing.T) {
	var mu sync.Mutex
	var got []string
	w := NewWorker(func(b *Buffer) {
		mu.Lock()
		got = append(got, string(b.Bytes()))
		mu.Unlock()
	}, AsyncUnsafe, DefaultGrowth())
	for i := 0; i < 10; i++ {
		w.Push([]byte(fmt.Sprintf("rec-%d\n", i)))
	}
	w.Stop()
	mu.Lock()
	all := strings.Join(got, "")
	mu.Unlock()
	for i := 0; i < 10; i++ {
		if !strings.Contains(all, fmt.Sprintf("rec-%d", i)) {
			t.Fatalf("missing rec-%d in drained output %q", i, all)
		}
	}
}

func TestWorkerSafeModeBackpressure(t *testing.T) {
	var mu sync.Mutex
	count := 0
	w := NewWorker(func(b *Buffer) {
		mu.Lock()
		count += strings.Count(string(b.Bytes()), "\n")
		mu.Unlock()
	}, AsyncSafe, Growth{Base: 64, Threshold: 1 << 20, Linear: 64})
	for i := 0; i < 100; i++ {
		w.Push([]byte("x\n"))
	}
	w.Stop()
	mu.Lock()
	defer mu.Unlock()
	if count != 100 {
		t.Fatalf("flushed %d records, want 100", count)
	}
}

func TestRollFileFlusherRotates(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "log")
	r := NewRollFileFlusher(base, 10, FlushBuffered)
	for i := 0; i < 5; i++ {
		if err := r.Flush([]byte("0123456789")); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(base + ".*.log")
	if len(files) < 2 {
		t.Fatalf("expected rotation to create multiple files, got %d", len(files))
	}
}

func TestFileFlusherCreatesDirectories(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a", "b", "out.log")
	f := NewFileFlusher(path, FlushBuffered)
	if err := f.Flush([]byte("line\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "line\n" {
		t.Fatalf("file content = %q", data)
	}
}

func TestLoggerLevelFilter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.log")
	l, err := NewBuilder().
		Name("t").
		Level(LevelWarn).
		Flusher(NewFileFlusher(path, FlushBuffered)).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	l.Debugf("debug-line")
	l.Infof("info-line")
	l.Warnf("warn-line")
	l.Errorf("error-line")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	s := string(data)
	if strings.Contains(s, "debug-line") || strings.Contains(s, "info-line") {
		t.Fatalf("level filter let low levels through: %q", s)
	}
	if !strings.Contains(s, "warn-line") || !strings.Contains(s, "error-line") {
		t.Fatalf("level filter dropped high levels: %q", s)
	}
}

func TestMessageFormat(t *testing.T) {
	m := &Message{
		Level:       LevelInfo,
		LoggerName:  "app",
		File:        "main.go",
		Line:        42,
		Payload:     "hello",
		Time:        time.Date(2026, 1, 2, 3, 4, 5, 600000000, time.UTC),
		GoroutineID: 7,
	}
	s := m.Format()
	for _, want := range []string{"[INFO]", "[app]", "[main.go:42]", "hello", "g:7", "2026-01-02 03:04:05.600"} {
		if !strings.Contains(s, want) {
			t.Fatalf("format missing %q: %q", want, s)
		}
	}
	if !strings.HasSuffix(s, "\n") {
		t.Fatal("format must end with a newline")
	}
}

func TestManagerAddGet(t *testing.T) {
	m := NewManager()
	defer m.Close()
	l, err := NewBuilder().Name("svc").Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Add(l); err != nil {
		t.Fatal(err)
	}
	if m.Get("svc") == nil {
		t.Fatal("registered logger not found")
	}
	if err := m.Add(l); err == nil {
		t.Fatal("expected duplicate-name error")
	}
	if m.Default() == nil {
		t.Fatal("default logger missing")
	}
}

func TestLoadConfigAppliesDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log.json")
	if err := os.WriteFile(path, []byte(`{"flush_log":2}`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FlushModeValue() != FlushSync {
		t.Fatalf("flush mode = %v, want FlushSync", cfg.FlushModeValue())
	}
	if cfg.BufferSize <= 0 || cfg.Threshold <= 0 {
		t.Fatal("defaults were not applied")
	}
	if cfg.MinLevel() != LevelDebug {
		t.Fatalf("min level = %v, want DEBUG", cfg.MinLevel())
	}
}

func TestParseLevel(t *testing.T) {
	for name, want := range map[string]Level{"debug": LevelDebug, "INFO": LevelInfo, "warning": LevelWarn, "error": LevelError, "fatal": LevelFatal} {
		got, err := ParseLevel(name)
		if err != nil || got != want {
			t.Fatalf("ParseLevel(%q) = %v, %v", name, got, err)
		}
	}
	if _, err := ParseLevel("bogus"); err == nil {
		t.Fatal("expected error for unknown level")
	}
}
