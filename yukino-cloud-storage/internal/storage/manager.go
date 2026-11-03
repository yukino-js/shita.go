package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
)

// Manager is the in-memory metadata table backed by a JSON file. It is safe for
// concurrent use, correcting the original which synchronised only part of its
// critical sections and persisted with a non-atomic in-place write.
type Manager struct {
	path string

	mu    sync.RWMutex
	table map[string]*StorageInfo // keyed by download URL
}

// NewManager loads the metadata table from path. A missing file yields an empty
// table rather than an error.
func NewManager(path string) (*Manager, error) {
	m := &Manager{path: path, table: make(map[string]*StorageInfo)}
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
		return fmt.Errorf("storage: read info file: %w", err)
	}
	if len(data) == 0 {
		return nil
	}
	var records []*StorageInfo
	if err := json.Unmarshal(data, &records); err != nil {
		return fmt.Errorf("storage: parse info file %s: %w", m.path, err)
	}
	for _, r := range records {
		if r == nil || r.URL == "" {
			continue
		}
		m.table[r.URL] = r
	}
	return nil
}

// persist serialises the table and writes it atomically. The caller must hold
// the write lock.
func (m *Manager) persist() error {
	records := make([]*StorageInfo, 0, len(m.table))
	for _, r := range m.table {
		records = append(records, r)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].URL < records[j].URL })
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return err
	}
	return WriteFileAtomic(m.path, data, 0o644)
}

// Insert adds or replaces a record and persists the table.
func (m *Manager) Insert(info *StorageInfo) error {
	if info == nil || info.URL == "" {
		return fmt.Errorf("storage: invalid storage info")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.table[info.URL] = info
	return m.persist()
}

// GetByURL returns a copy of the record for a download URL.
func (m *Manager) GetByURL(url string) (*StorageInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	info, ok := m.table[url]
	if !ok {
		return nil, false
	}
	cp := *info
	return &cp, true
}

// GetByStoragePath returns a copy of the record with the given on-disk path.
func (m *Manager) GetByStoragePath(path string) (*StorageInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, info := range m.table {
		if info.StoragePath == path {
			cp := *info
			return &cp, true
		}
	}
	return nil, false
}

// All returns copies of every record ordered by URL.
func (m *Manager) All() []*StorageInfo {
	m.mu.RLock()
	records := make([]*StorageInfo, 0, len(m.table))
	for _, r := range m.table {
		cp := *r
		records = append(records, &cp)
	}
	m.mu.RUnlock()
	sort.Slice(records, func(i, j int) bool { return records[i].URL < records[j].URL })
	return records
}

// Delete removes a record by URL and persists the table. Deleting an absent URL
// is a no-op.
func (m *Manager) Delete(url string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.table[url]; !ok {
		return nil
	}
	delete(m.table, url)
	return m.persist()
}
