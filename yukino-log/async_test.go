package yukino_log

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAsyncRejectsBadOptions(t *testing.T) {
	_, err := NewAsync(AsyncOptions{Basename: "", RollSize: 10})
	if !errors.Is(err, ErrBasename) {
		t.Fatalf("got %v", err)
	}
	_, err = NewAsync(AsyncOptions{Basename: "app", RollSize: 0})
	if !errors.Is(err, ErrRollSize) {
		t.Fatalf("got %v", err)
	}
	_, err = NewAsync(AsyncOptions{Basename: "app", RollSize: 10, BufferSize: -1})
	if !errors.Is(err, ErrBufferSize) {
		t.Fatalf("got %v", err)
	}
	_, err = NewAsync(AsyncOptions{Basename: "app", RollSize: 10, FlushInterval: -time.Second})
	if !errors.Is(err, ErrFlushInterval) {
		t.Fatalf("got %v", err)
	}
}

func TestAsyncRequiresStart(t *testing.T) {
	dir := t.TempDir()
	backend := newAsync(t, AsyncOptions{
		Basename: filepath.Join(dir, "app"),
		RollSize: 1 << 20,
	})
	if err := backend.Append([]byte("x")); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("append: %v", err)
	}
	if err := backend.Flush(); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("flush: %v", err)
	}
	if err := backend.Stop(); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("stop: %v", err)
	}
	if _, err := backend.Write(nil); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncStopDrains(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "app")
	backend := newAsync(t, AsyncOptions{
		Basename:      base,
		RollSize:      1 << 20,
		FlushInterval: time.Hour,
	})
	if err := backend.Start(); err != nil {
		t.Fatal(err)
	}
	const workers = 8
	const per = 50
	var group sync.WaitGroup
	group.Add(workers)
	for id := 0; id < workers; id++ {
		go func(id int) {
			defer group.Done()
			for i := 0; i < per; i++ {
				line := strconv.Itoa(id) + "-" + strconv.Itoa(i) + "\n"
				if _, err := backend.Write([]byte(line)); err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
		}(id)
	}
	group.Wait()
	if err := backend.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := backend.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := backend.Append([]byte("late")); !errors.Is(err, ErrClosed) {
		t.Fatalf("append after stop: %v", err)
	}
	if err := backend.Start(); !errors.Is(err, ErrClosed) {
		t.Fatalf("start after stop: %v", err)
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

func TestAsyncFlushBeforeStop(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "app")
	backend := newAsync(t, AsyncOptions{
		Basename:      base,
		RollSize:      1 << 20,
		FlushInterval: time.Hour,
	})
	if err := backend.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := backend.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := joinData(readLogs(t, dir, base)); got != "hello" {
		t.Fatalf("after flush %q", got)
	}
	if err := backend.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncIntervalWakesBackend(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "app")
	backend := newAsync(t, AsyncOptions{
		Basename:      base,
		RollSize:      1 << 20,
		FlushInterval: 20 * time.Millisecond,
		File:          FileOptions{FlushInterval: time.Hour},
	})
	if err := backend.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Write([]byte("tick")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if joinData(readLogs(t, dir, base)) == "tick" {
			if err := backend.Stop(); err != nil {
				t.Fatal(err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("interval did not flush the partial buffer")
}

func TestAsyncKeepsExactAndOversizedRecords(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "app")
	backend := newAsync(t, AsyncOptions{
		Basename:      base,
		RollSize:      1 << 20,
		BufferSize:    4,
		FlushInterval: time.Hour,
	})
	if err := backend.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Write([]byte("abcd")); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Write([]byte("e")); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Write([]byte("abcdefgh")); err != nil {
		t.Fatal(err)
	}
	if err := backend.Stop(); err != nil {
		t.Fatal(err)
	}
	if got := joinData(readLogs(t, dir, base)); got != "abcdeabcdefgh" {
		t.Fatalf("got %q", got)
	}
}

func TestAsyncStartFailureCanBeRetried(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	backend := newAsync(t, AsyncOptions{
		Basename: filepath.Join(blocker, "app"),
		RollSize: 100,
	})
	if err := backend.Start(); err == nil {
		t.Fatal("expected start to fail")
	}
	if err := backend.Append([]byte("x")); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("got %v", err)
	}
}

func TestLoggerThroughAsync(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "app")
	when := time.Date(2024, 3, 1, 8, 30, 0, 123456000, time.UTC)
	backend := newAsync(t, AsyncOptions{
		Basename:      base,
		RollSize:      1 << 20,
		FlushInterval: time.Hour,
		File:          FileOptions{Now: frozen(when), Location: time.UTC, FlushInterval: time.Hour},
	})
	if err := backend.Start(); err != nil {
		t.Fatal(err)
	}
	logger := New(Options{
		Now:      frozen(when),
		Location: time.UTC,
	})
	logger.Attach(backend)
	logger.Infof("hello %d", 7)
	if err := logger.Flush(); err != nil {
		t.Fatal(err)
	}
	body := joinData(readLogs(t, dir, base))
	if !strings.Contains(body, "2024/03/01 08:30:00.123456 INFO  hello 7 - async_test.go:") {
		t.Fatalf("body %q", body)
	}
	if !strings.HasSuffix(body, "\n") {
		t.Fatalf("body %q", body)
	}
	if err := backend.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncDoubleStart(t *testing.T) {
	dir := t.TempDir()
	backend := newAsync(t, AsyncOptions{
		Basename: filepath.Join(dir, "app"),
		RollSize: 100,
	})
	if err := backend.Start(); err != nil {
		t.Fatal(err)
	}
	if err := backend.Start(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentFlushAndAppend(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "app")
	backend := newAsync(t, AsyncOptions{
		Basename:      base,
		RollSize:      128,
		BufferSize:    32,
		FlushInterval: time.Hour,
	})
	if err := backend.Start(); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	group.Add(3)
	go func() {
		defer group.Done()
		for i := 0; i < 100; i++ {
			_, _ = backend.Write([]byte("abcdefghijklmnopqrstuvwxyz"))
		}
	}()
	go func() {
		defer group.Done()
		for i := 0; i < 20; i++ {
			_ = backend.Flush()
		}
	}()
	go func() {
		defer group.Done()
		for i := 0; i < 100; i++ {
			_, _ = backend.Write([]byte("0123456789"))
		}
	}()
	group.Wait()
	if err := backend.Stop(); err != nil {
		t.Fatal(err)
	}
	body := joinData(readLogs(t, dir, base))
	if strings.Count(body, "abcdefghijklmnopqrstuvwxyz") != 100 {
		t.Fatalf("letters %d", strings.Count(body, "abcdefghijklmnopqrstuvwxyz"))
	}
	if strings.Count(body, "0123456789") != 100 {
		t.Fatalf("digits %d", strings.Count(body, "0123456789"))
	}
}

func TestConcurrentStopWaitsForDrain(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "app")
	backend := newAsync(t, AsyncOptions{
		Basename:      base,
		RollSize:      1 << 20,
		FlushInterval: time.Hour,
	})
	if err := backend.Start(); err != nil {
		t.Fatal(err)
	}
	const records = 500
	for i := 0; i < records; i++ {
		if _, err := backend.Write([]byte("record-" + strconv.Itoa(i) + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	// Every concurrent Stop must block until the drain has finished; a Stop
	// that returned early could observe the file before the last records
	// were written.
	var group sync.WaitGroup
	group.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			defer group.Done()
			if err := backend.Stop(); err != nil {
				t.Errorf("stop: %v", err)
			}
		}()
	}
	group.Wait()
	body := joinData(readLogs(t, dir, base))
	for i := 0; i < records; i++ {
		if !strings.Contains(body, "record-"+strconv.Itoa(i)+"\n") {
			t.Fatalf("missing record %d", i)
		}
	}
}

func newAsync(t *testing.T, opts AsyncOptions) *Async {
	t.Helper()
	backend, err := NewAsync(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Stop() })
	return backend
}
