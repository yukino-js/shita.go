package raft

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Persister stores the two pieces of durable state a Raft node needs to
// survive a restart: the encoded Raft state (current term, vote and log)
// and the latest state-machine snapshot.
//
// The original C++ implementation had two defects that defeated its own
// purpose:
//
//  1. Its constructor truncated both files on every startup, destroying
//     exactly the data persistence exists to protect, so a restarted node
//     could never recover its term, vote or log.
//  2. It read files back with `ifs >> str`, which stops at the first
//     whitespace token and therefore returned a corrupted prefix of every
//     boost text archive.
//
// This implementation loads whatever is already on disk at startup and
// rewrites files atomically (temporary file + rename), so a crash in the
// middle of a write can never leave a half-written state behind.
type Persister struct {
	mu            sync.Mutex
	raftStatePath string
	snapshotPath  string
	raftState     []byte
	snapshot      []byte
}

// NewPersister opens (or creates) the persistence files for node me inside
// dir. Existing content is loaded, never discarded.
func NewPersister(dir string, me int) (*Persister, error) {
	if dir == "" {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("persister: create data dir: %w", err)
	}
	ps := &Persister{
		raftStatePath: filepath.Join(dir, fmt.Sprintf("raftstatePersist%d.txt", me)),
		snapshotPath:  filepath.Join(dir, fmt.Sprintf("snapshotPersist%d.txt", me)),
	}
	raftState, err := os.ReadFile(ps.raftStatePath)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("persister: read raft state: %w", err)
	}
	snapshot, err := os.ReadFile(ps.snapshotPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("persister: read snapshot: %w", err)
	}
	ps.raftState = raftState
	ps.snapshot = snapshot
	return ps, nil
}

// SaveRaftState durably replaces the persisted Raft state.
func (ps *Persister) SaveRaftState(state []byte) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := writeFileAtomically(ps.raftStatePath, state); err != nil {
		return err
	}
	ps.raftState = bytes.Clone(state)
	return nil
}

// Save durably replaces the Raft state and the snapshot together.
//
// The snapshot file is written first. If the process dies between the two
// renames, the node restarts with an older Raft state whose log still
// contains everything the newer snapshot covers; the state machine filters
// the resulting duplicate applies through its deduplication table. The
// reverse order could instead strand the truncated log with a stale
// snapshot and lose committed data.
func (ps *Persister) Save(raftState, snapshot []byte) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := writeFileAtomically(ps.snapshotPath, snapshot); err != nil {
		return err
	}
	if err := writeFileAtomically(ps.raftStatePath, raftState); err != nil {
		return err
	}
	ps.snapshot = bytes.Clone(snapshot)
	ps.raftState = bytes.Clone(raftState)
	return nil
}

// ReadRaftState returns a copy of the persisted Raft state.
func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return bytes.Clone(ps.raftState)
}

// ReadSnapshot returns a copy of the persisted snapshot.
func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return bytes.Clone(ps.snapshot)
}

// RaftStateSize reports the byte size of the persisted Raft state. The KV
// layer uses it to decide when the log has grown large enough to warrant a
// snapshot.
func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftState)
}

// writeFileAtomically writes data to a temporary file, flushes it to disk
// and renames it over path. Readers therefore observe either the previous
// content or the new content, never a partial write.
func writeFileAtomically(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("persister: create %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("persister: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("persister: sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("persister: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("persister: rename %s: %w", path, err)
	}
	return nil
}
