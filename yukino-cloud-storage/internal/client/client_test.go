package client

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestManagerPersistReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.dat")
	m, err := NewManager(path)
	if err != nil {
		t.Fatal(err)
	}
	key := "dir/file with space.txt"
	val := "file with space.txt-10-123"
	if err := m.Set(key, val); err != nil {
		t.Fatal(err)
	}
	m2, err := NewManager(path)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := m2.Get(key)
	if !ok || got != val {
		t.Fatalf("Get = %q, %v; want %q, true", got, ok, val)
	}
}

func TestDurationUnmarshal(t *testing.T) {
	var c Config
	if err := json.Unmarshal([]byte(`{"scan_interval":"2s","quiet_period":"500ms","timeout":1500}`), &c); err != nil {
		t.Fatal(err)
	}
	if c.ScanInterval.Duration() != 2*time.Second {
		t.Fatalf("scan interval = %v", c.ScanInterval.Duration())
	}
	if c.QuietPeriod.Duration() != 500*time.Millisecond {
		t.Fatalf("quiet period = %v", c.QuietPeriod.Duration())
	}
	if c.Timeout.Duration() != 1500*time.Millisecond {
		t.Fatalf("timeout = %v", c.Timeout.Duration())
	}
}

func TestConfigValidateDefaults(t *testing.T) {
	c := &Config{ServerURL: "http://x"}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.ScanInterval.Duration() <= 0 || c.Timeout.Duration() <= 0 {
		t.Fatal("defaults not applied")
	}
	if err := (&Config{}).Validate(); err == nil {
		t.Fatal("expected error for missing server_url")
	}
}
