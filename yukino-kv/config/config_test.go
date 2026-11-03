package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadParsesNodeEntriesInOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.conf")
	content := `# cluster of two nodes
node0ip=127.0.0.1
node0port=29016

node1ip=127.0.0.2
node1port=29017
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	nodes, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("got %d nodes, want 2", len(nodes))
	}
	if nodes[0].Addr() != "127.0.0.1:29016" {
		t.Errorf("node0 addr = %q, want 127.0.0.1:29016", nodes[0].Addr())
	}
	if nodes[1].Addr() != "127.0.0.2:29017" {
		t.Errorf("node1 addr = %q, want 127.0.0.2:29017", nodes[1].Addr())
	}
}

func TestLoadRejectsBadInput(t *testing.T) {
	dir := t.TempDir()
	cases := map[string]string{
		"empty":        "",
		"missing-port": "node0ip=127.0.0.1\n",
		"bad-port":     "node0ip=127.0.0.1\nnode0port=notanumber\n",
		"no-separator": "node0ip:127.0.0.1\n",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, name+".conf")
			if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); err == nil {
				t.Fatalf("Load(%q) succeeded, want error", content)
			}
		})
	}
	if _, err := Load(filepath.Join(dir, "does-not-exist.conf")); err == nil {
		t.Fatal("Load of missing file succeeded, want error")
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "roundtrip.conf")
	want := []Node{{IP: "127.0.0.1", Port: 21000}, {IP: "10.0.0.5", Port: 80}, {IP: "::1", Port: 65535}}
	if err := Save(path, want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d nodes, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("node %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}
