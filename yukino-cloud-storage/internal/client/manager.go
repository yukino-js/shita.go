// Package client implements the upload agent that watches local directories
// and ships new or changed files to the cloud storage server.
//
// It is a Go migration of the C++ project's src/client component.
package client

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Manager tracks which local files have already been uploaded, keyed by file
// path, with a value that uniquely identifies the uploaded content.
//
// The original stored this as space-separated "key value" lines, which broke on
// any path containing a space and could index past the end of a malformed line.
// This implementation uses JSON and is safe for concurrent use.
type Manager struct {
	path string

	mu    sync.Mutex
	table map[string]string
}

// NewManager loads the upload state from path. A missing file yields an empty
// table rather than an error.
func NewManager(path string) (*Manager, error) {
	m := &Manager{path: path, table: make(map[string]string)}
	if err := m.load(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) load() error {
	data, err := os.ReadFile(m.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("client: read state file: %w", err)
	}
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, &m.table); err != nil {
		return fmt.Errorf("client: parse state file %s: %w", m.path, err)
	}
	if m.table == nil {
		m.table = make(map[string]string)
	}
	return nil
}

// Get returns the stored identifier for key.
func (m *Manager) Get(key string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.table[key]
	return v, ok
}

// Set records an identifier for key and persists the table atomically.
func (m *Manager) Set(key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.table[key] = value
	return m.persist()
}

// persist writes the table to disk. The caller must hold the lock.
func (m *Manager) persist() error {
	data, err := json.MarshalIndent(m.table, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(m.path, data)
}

// writeFileAtomic writes data to path through a temporary file and a rename so
// a crash mid-write cannot corrupt the existing state.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".yukino-agent-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	renamed := false
	defer func() {
		if !renamed {
			_ = os.Remove(name)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(name, 0o644); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	renamed = true
	return nil
}
