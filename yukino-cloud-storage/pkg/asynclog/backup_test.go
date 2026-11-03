package asynclog

import (
	"path/filepath"
	"sync"
	"testing"
)

// recordingSink is an in-memory BackupSink used to observe which records are
// forwarded for backup.
type recordingSink struct {
	mu      sync.Mutex
	records []string
	closed  bool
}

func (r *recordingSink) Backup(rec string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec)
}

func (r *recordingSink) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

func (r *recordingSink) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.records)
}

func TestLoggerForwardsErrorFatalToBackup(t *testing.T) {
	sink := &recordingSink{}
	dir := t.TempDir()
	l, err := NewBuilder().
		Name("bk").
		Flusher(NewFileFlusher(filepath.Join(dir, "o.log"), FlushBuffered)).
		Backup(sink).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	l.Debugf("d")
	l.Infof("i")
	l.Warnf("w")
	l.Errorf("e")
	l.Fatalf("f")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if sink.count() != 2 {
		t.Fatalf("backup received %d records, want 2 (ERROR and FATAL)", sink.count())
	}
	if !sink.closed {
		t.Fatal("backup sink was not closed")
	}
}
