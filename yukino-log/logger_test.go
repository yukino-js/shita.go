package yukino_log

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFormatLine(t *testing.T) {
	now := time.Date(2024, 3, 1, 8, 30, 0, 123456000, time.UTC)
	var buf []byte
	buf = appendStamp(buf, now, time.UTC)
	buf = append(buf, LevelInfo.tag()...)
	buf = fmt.Append(buf, "Hello, ", 7, " abc")
	buf = appendSuffix(buf, "/tmp/dir/app.go", 42)
	want := "2024/03/01 08:30:00.123456 INFO  Hello, 7 abc - app.go:42\n"
	if string(buf) != want {
		t.Fatalf("format\n got %q\nwant %q", buf, want)
	}
}

func TestMicrosecondsAreZeroPadded(t *testing.T) {
	now := time.Date(2024, 3, 1, 8, 30, 0, 123000000, time.UTC)
	got := string(appendStamp(nil, now, time.UTC))
	if got != "2024/03/01 08:30:00.123000 " {
		t.Fatalf("got %q", got)
	}
	zero := time.Date(2024, 3, 1, 8, 30, 0, 0, time.UTC)
	got = string(appendStamp(nil, zero, time.UTC))
	if got != "2024/03/01 08:30:00.000000 " {
		t.Fatalf("got %q", got)
	}
}

func TestLevelTagsAreSixBytes(t *testing.T) {
	for level := LevelTrace; level <= LevelFatal; level++ {
		if len(level.tag()) != 6 {
			t.Fatalf("%s tag is %q", level, level.tag())
		}
		if level.String() == "" || level.String() == "UNKNOWN" {
			t.Fatalf("name %q", level.String())
		}
	}
	if Level(99).String() != "UNKNOWN" {
		t.Fatalf("unknown name %q", Level(99).String())
	}
	if Level(99).tag() != "UNK   " {
		t.Fatalf("unknown tag %q", Level(99).tag())
	}
}

func TestMinLevelAndCaller(t *testing.T) {
	var buf lockedBuffer
	logger := New(Options{
		MinLevel: LevelInfo,
		Output:   buf.Write,
		Now: func() time.Time {
			return time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)
		},
		Location: time.UTC,
	})
	logger.Debug("hidden")
	logger.Info("Hello, ", 7, " abc")
	logger.Infof("n=%d", 3)

	text := buf.String()
	if strings.Contains(text, "hidden") || strings.Contains(text, "DEBUG") {
		t.Fatalf("debug record was emitted: %s", text)
	}
	if !strings.Contains(text, "INFO  Hello, 7 abc - logger_test.go:") {
		t.Fatalf("info record: %s", text)
	}
	if strings.Contains(text, "logger.go:") {
		t.Fatalf("reported a library frame: %s", text)
	}
	if !strings.Contains(text, "INFO  n=3 - logger_test.go:") {
		t.Fatalf("infof record: %s", text)
	}
}

func TestFatalFlushesThenPanics(t *testing.T) {
	var order []string
	logger := New(Options{
		Output: func(p []byte) (int, error) {
			order = append(order, "out")
			if !bytes.Contains(p, []byte("FATAL ")) || !bytes.Contains(p, []byte("boom")) {
				t.Fatalf("record %q", p)
			}
			return len(p), nil
		},
		Flush: func() error {
			order = append(order, "flush")
			return nil
		},
		Now:      func() time.Time { return time.Unix(0, 0).UTC() },
		Location: time.UTC,
	})

	defer func() {
		recovered := recover()
		fatal, ok := recovered.(FatalError)
		if !ok {
			t.Fatalf("panic type %T value %v", recovered, recovered)
		}
		if fatal.Message != "boom" {
			t.Fatalf("message %q", fatal.Message)
		}
		if strings.Join(order, ",") != "out,flush" {
			t.Fatalf("order %v", order)
		}
	}()
	logger.Fatal("boom")
	t.Fatal("Fatal returned")
}

func TestFatalIsNotFiltered(t *testing.T) {
	var n int
	logger := New(Options{
		MinLevel: LevelFatal,
		Output: func(p []byte) (int, error) {
			n++
			return len(p), nil
		},
		Now:      func() time.Time { return time.Unix(0, 0).UTC() },
		Location: time.UTC,
	})
	logger.Error("nope")
	if n != 0 {
		t.Fatalf("error record passed the filter")
	}
	defer func() { _ = recover() }()
	logger.Fatalf("bad %d", 1)
	t.Fatal("Fatalf returned")
}

func TestOutputErrorIsReportedAndDoesNotPanic(t *testing.T) {
	stderr, err := os.CreateTemp(t.TempDir(), "stderr")
	if err != nil {
		t.Fatal(err)
	}
	original := os.Stderr
	os.Stderr = stderr
	t.Cleanup(func() {
		os.Stderr = original
		stderr.Close()
	})

	logger := New(Options{
		Output: func([]byte) (int, error) {
			return 0, errors.New("sink closed")
		},
		Now:      func() time.Time { return time.Unix(0, 0).UTC() },
		Location: time.UTC,
	})
	logger.Info("still delivered to the hook")

	if err := stderr.Sync(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(stderr.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("writing log output: sink closed")) {
		t.Fatalf("stderr %q", data)
	}
}

func TestNilAndExtremeValuesDoNotPanic(t *testing.T) {
	var buf lockedBuffer
	logger := New(Options{
		Output:   buf.Write,
		Now:      func() time.Time { return time.Unix(0, 0).UTC() },
		Location: time.UTC,
	})
	logger.Info(nil, math.MinInt64, uint64(math.MaxUint64), true, false)
	text := buf.String()
	for _, part := range []string{"<nil>", "-9223372036854775808", "18446744073709551615", "true", "false"} {
		if !strings.Contains(text, part) {
			t.Fatalf("missing %s in %s", part, text)
		}
	}
}

func TestConcurrentLogger(t *testing.T) {
	var buf lockedBuffer
	logger := New(Options{
		Output:   buf.Write,
		Now:      func() time.Time { return time.Unix(0, 0).UTC() },
		Location: time.UTC,
	})
	const workers = 8
	const per = 100
	var group sync.WaitGroup
	group.Add(workers + 1)
	go func() {
		defer group.Done()
		for i := 0; i < per; i++ {
			if i%2 == 0 {
				logger.SetMinLevel(LevelTrace)
			} else {
				logger.SetMinLevel(LevelInfo)
			}
		}
	}()
	for id := 0; id < workers; id++ {
		go func() {
			defer group.Done()
			for i := 0; i < per; i++ {
				logger.Infof("worker line %d", i)
			}
		}()
	}
	group.Wait()
	if got := strings.Count(buf.String(), "\n"); got != workers*per {
		t.Fatalf("lines = %d, want %d", got, workers*per)
	}
}

func TestAttachNilPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic")
		}
	}()
	New(Options{}).Attach(nil)
}

func TestPackageLogger(t *testing.T) {
	var buf lockedBuffer
	SetOutput(buf.Write)
	SetFlush(nil)
	SetMinLevel(LevelWarn)
	t.Cleanup(func() {
		SetOutput(nil)
		SetFlush(nil)
		SetMinLevel(LevelTrace)
	})
	Info("hidden")
	Warn("shown")
	text := buf.String()
	if strings.Contains(text, "hidden") || !strings.Contains(text, "WARN  shown - logger_test.go:") {
		t.Fatalf("package logger: %s", text)
	}
}

func TestBasename(t *testing.T) {
	if got := basename(`C:\src\app.go`); got != "app.go" {
		t.Fatalf("got %q", got)
	}
	if got := basename("/a/b/c.go"); got != "c.go" {
		t.Fatalf("got %q", got)
	}
	if got := basename(""); got != "???" {
		t.Fatalf("got %q", got)
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}
