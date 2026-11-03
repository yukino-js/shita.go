package logbackup_test

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yukino-js/shita.go/yukino-cloud-storage/pkg/asynclog"
	"github.com/yukino-js/shita.go/yukino-cloud-storage/pkg/logbackup"
)

// TestAsyncLogBackupIntegration exercises the full path: an async logger
// forwards ERROR and FATAL records to a logbackup client, which delivers them
// over RPC to a receiver that appends them to a file.
func TestAsyncLogBackupIntegration(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "backup.log")

	srv := logbackup.NewServer(logbackup.ServerOptions{Path: out})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	client := logbackup.NewClient(logbackup.ClientOptions{
		Addr:         lis.Addr().String(),
		Source:       "itest",
		QueueSize:    64,
		BatchSize:    8,
		Timeout:      2 * time.Second,
		RetryBackoff: 50 * time.Millisecond,
	})

	logger, err := asynclog.NewBuilder().
		Name("itest").
		Flusher(asynclog.NewWriterFlusher(io.Discard, asynclog.FlushNone)).
		Backup(client).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	logger.Errorf("integration error record")
	logger.Fatalf("integration fatal record")
	// Closing the logger drains the backup client and delivers the records.
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}

	var data []byte
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, _ = os.ReadFile(out)
		if strings.Contains(string(data), "integration error record") &&
			strings.Contains(string(data), "integration fatal record") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	s := string(data)
	if !strings.Contains(s, "integration error record") || !strings.Contains(s, "integration fatal record") {
		t.Fatalf("backup file missing records: %q", s)
	}
	if !strings.Contains(s, "[ERROR]") || !strings.Contains(s, "[FATAL]") {
		t.Fatalf("backup file missing level markers: %q", s)
	}
}
