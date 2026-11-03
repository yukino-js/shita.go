package raft

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestPersisterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ps, err := NewPersister(dir, 7)
	if err != nil {
		t.Fatalf("NewPersister: %v", err)
	}
	if got := ps.ReadRaftState(); len(got) != 0 {
		t.Fatalf("fresh raft state = %q, want empty", got)
	}
	if got := ps.ReadSnapshot(); len(got) != 0 {
		t.Fatalf("fresh snapshot = %q, want empty", got)
	}

	state := []byte("term:42 votedFor:3 logs:[...] with spaces and\nnewlines")
	if err := ps.SaveRaftState(state); err != nil {
		t.Fatalf("SaveRaftState: %v", err)
	}
	if got := ps.ReadRaftState(); !bytes.Equal(got, state) {
		t.Fatalf("ReadRaftState = %q, want %q", got, state)
	}
	if ps.RaftStateSize() != len(state) {
		t.Fatalf("RaftStateSize = %d, want %d", ps.RaftStateSize(), len(state))
	}

	snapshot := []byte("snapshot payload \x00\x01 with binary bytes")
	if err := ps.Save(state, snapshot); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if got := ps.ReadSnapshot(); !bytes.Equal(got, snapshot) {
		t.Fatalf("ReadSnapshot = %q, want %q", got, snapshot)
	}
}

// TestPersisterSurvivesRestart covers the defect fixed relative to the
// C++ original: reopening a persister must load the existing files, not
// truncate them, and payloads containing whitespace must survive intact.
func TestPersisterSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	state := []byte("currentTerm 9 votedFor 2 logs a b c")
	snapshot := []byte("db key1 value1 key2 value2")

	ps, err := NewPersister(dir, 1)
	if err != nil {
		t.Fatalf("NewPersister: %v", err)
	}
	if err := ps.Save(state, snapshot); err != nil {
		t.Fatalf("Save: %v", err)
	}

	reopened, err := NewPersister(dir, 1)
	if err != nil {
		t.Fatalf("reopen NewPersister: %v", err)
	}
	if got := reopened.ReadRaftState(); !bytes.Equal(got, state) {
		t.Errorf("raft state after restart = %q, want %q", got, state)
	}
	if got := reopened.ReadSnapshot(); !bytes.Equal(got, snapshot) {
		t.Errorf("snapshot after restart = %q, want %q", got, snapshot)
	}
}

func TestPersisterAtomicWriteLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	ps, err := NewPersister(dir, 2)
	if err != nil {
		t.Fatalf("NewPersister: %v", err)
	}
	if err := ps.Save([]byte("state"), []byte("snapshot")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Errorf("temp file %s left behind after successful write", e.Name())
		}
	}
	for _, want := range []string{"raftstatePersist2.txt", "snapshotPersist2.txt"} {
		if _, err := os.Stat(filepath.Join(dir, want)); err != nil {
			t.Errorf("expected file %s: %v", want, err)
		}
	}
}

func TestPersisterReturnsCopies(t *testing.T) {
	dir := t.TempDir()
	ps, err := NewPersister(dir, 3)
	if err != nil {
		t.Fatalf("NewPersister: %v", err)
	}
	state := []byte("original")
	if err := ps.SaveRaftState(state); err != nil {
		t.Fatalf("SaveRaftState: %v", err)
	}
	state[0] = 'X' // mutate the caller's buffer
	got := ps.ReadRaftState()
	got[0] = 'Y' // mutate the returned buffer
	if again := ps.ReadRaftState(); !bytes.Equal(again, []byte("original")) {
		t.Fatalf("persister state corrupted through shared buffers: %q", again)
	}
}
