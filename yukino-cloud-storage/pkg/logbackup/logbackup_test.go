package logbackup

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestClientServerRoundtrip(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "backup.log")

	srv := NewServer(ServerOptions{Path: out})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	c := NewClient(ClientOptions{
		Addr:         lis.Addr().String(),
		Source:       "test",
		QueueSize:    64,
		BatchSize:    4,
		Timeout:      2 * time.Second,
		RetryBackoff: 50 * time.Millisecond,
	})
	for i := 0; i < 10; i++ {
		c.Backup(fmt.Sprintf("record %d\n", i))
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if c.Sent() != 10 {
		t.Fatalf("sent %d, want 10 (dropped %d)", c.Sent(), c.Dropped())
	}

	// Give the server a moment to flush the final batch to disk.
	var data []byte
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, _ = os.ReadFile(out)
		if strings.Count(string(data), "record ") >= 10 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		if !strings.Contains(string(data), fmt.Sprintf("record %d", i)) {
			t.Fatalf("missing record %d in %q", i, data)
		}
	}
	// Every stored record must carry the configured source prefix.
	if !strings.Contains(string(data), "[test] record 0") {
		t.Fatalf("stored records are missing the source prefix: %q", data)
	}
}

// TestServerStopBeforeServe covers the shutdown ordering that previously
// deadlocked: Stop is called before the Serve goroutine has started. Serve
// must observe the shutdown and return instead of blocking in Accept forever.
func TestServerStopBeforeServe(t *testing.T) {
	srv := NewServer(ServerOptions{Path: filepath.Join(t.TempDir(), "backup.log")})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.Stop()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(lis) }()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Serve after Stop returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after a prior Stop")
	}
}

// TestServerStopUnblocksServe covers the normal ordering: Serve is already
// accepting when Stop arrives, and must unblock promptly.
func TestServerStopUnblocksServe(t *testing.T) {
	srv := NewServer(ServerOptions{Path: filepath.Join(t.TempDir(), "backup.log")})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(lis) }()
	// Give the accept loop a moment to enter Accept.
	time.Sleep(50 * time.Millisecond)
	srv.Stop()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Serve returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not unblock Serve")
	}
}

func TestClientDropsWhenUnreachable(t *testing.T) {
	// Bind then close a port so the address is almost certainly refused.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	c := NewClient(ClientOptions{
		Addr:         addr,
		QueueSize:    8,
		BatchSize:    2,
		Timeout:      200 * time.Millisecond,
		RetryBackoff: 10 * time.Millisecond,
	})
	for i := 0; i < 4; i++ {
		c.Backup(fmt.Sprintf("lost %d\n", i))
	}
	_ = c.Close()
	if c.Sent() != 0 {
		t.Fatalf("sent %d to an unreachable server", c.Sent())
	}
	if c.Dropped() == 0 {
		t.Fatal("expected undeliverable records to be counted as dropped")
	}
}
