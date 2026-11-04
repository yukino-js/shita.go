// Package kvserver implements the linearizable key-value state machine
// layered on top of the raft package: every client request is submitted to
// Raft as a log entry, executed exactly once through the deduplication
// table when it commits, and answered by matching the committed entry
// against the waiting RPC handler.
//
// The C++ original stored its data in a skip list; the README of that
// project explicitly treats the engine as a replaceable state machine, so
// this port uses a plain Go map guarded by a mutex — the idiomatic
// standard-library equivalent.
package kvserver

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/yukino-js/shita.go/yukino-kv/raft"
)

// ServiceName is the yukino_rpc service name exposing Get and PutAppend
// to clients.
const ServiceName = "KVServer"

// ConsensusTimeout bounds how long an RPC handler waits for its command
// to come back through the apply channel before giving up and letting the
// client retry elsewhere.
const ConsensusTimeout = 500 * time.Millisecond

// Error strings returned to clients. They mirror the C++ constants.
const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrInvalidOp   = "ErrInvalidOperation"
)

// Operation names accepted in Op.Operation / PutAppendArgs.Op.
const (
	OpPut    = "Put"
	OpAppend = "Append"
	OpGet    = "Get"
)

// snapshotNumerator/snapshotDenominator define the fraction of
// maxRaftState at which a new snapshot is taken. FIX: the C++ original
// accepted a "proportion" argument but ignored it, hard-coding a trigger
// at maxRaftState/10; the documented intent was the 9/10 proportion used
// here.
const (
	snapshotNumerator   = 9
	snapshotDenominator = 10
)

// Op is one state-machine command as it travels through the Raft log. It
// replaces the boost-serialized Op class of the C++ original; entries are
// gob-encoded into the raft log.
type Op struct {
	Operation string // OpGet, OpPut or OpAppend
	Key       string
	Value     string
	ClientId  string
	RequestId int
}

func (op Op) encode() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&op); err != nil {
		return nil, fmt.Errorf("kvserver: encode op: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeOp(data []byte) (Op, error) {
	var op Op
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&op); err != nil {
		return Op{}, fmt.Errorf("kvserver: decode op: %w", err)
	}
	return op, nil
}

// GetArgs is a linearizable read request.
type GetArgs struct {
	Key       string `json:"key"`
	ClientId  string `json:"clientId"`
	RequestId int    `json:"requestId"`
}

// GetReply carries the read result or an error string.
type GetReply struct {
	Err   string `json:"err"`
	Value string `json:"value"`
}

// PutAppendArgs is a write request; Op is OpPut or OpAppend.
type PutAppendArgs struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Op        string `json:"op"`
	ClientId  string `json:"clientId"`
	RequestId int    `json:"requestId"`
}

// PutAppendReply carries the write result.
type PutAppendReply struct {
	Err string `json:"err"`
}

// snapshotPayload is the gob-encoded KV snapshot: the database itself plus
// everything needed to keep execution exactly-once after the snapshot is
// installed.
type snapshotPayload struct {
	DB            map[string]string
	LastRequestId map[string]int
	// LastSnapshotIndex lets a restarted node keep skipping applied
	// commands at or below its snapshot point.
	LastSnapshotIndex int
}

// KVServer is the key-value state machine of one cluster node.
type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyChan    chan raft.ApplyMsg
	maxRaftState int // snapshot when the Raft state grows past this; -1 disables

	db            map[string]string
	waitApplyCh   map[int]chan Op // raft log index -> waiting RPC handler
	lastRequestId map[string]int  // client id -> highest executed request id
	// lastSnapShotRaftLogIndex is the raft index covered by the newest
	// snapshot this state machine installed.
	lastSnapShotRaftLogIndex int

	done     chan struct{}
	doneOnce sync.Once
	applyWG  sync.WaitGroup
	logger   *slog.Logger
}

// NewKVServer creates the state machine for node me and starts the apply
// loop. Initial state is restored from the persister's snapshot, if any.
func NewKVServer(me, maxRaftState int, rf *raft.Raft, persister *raft.Persister, applyChan chan raft.ApplyMsg, logger *slog.Logger) *KVServer {
	if logger == nil {
		logger = slog.Default()
	}
	kv := &KVServer{
		me:            me,
		rf:            rf,
		applyChan:     applyChan,
		maxRaftState:  maxRaftState,
		db:            make(map[string]string),
		waitApplyCh:   make(map[int]chan Op),
		lastRequestId: make(map[string]int),
		done:          make(chan struct{}),
		logger:        logger.With("component", "kvserver"),
	}
	if snapshot := persister.ReadSnapshot(); len(snapshot) > 0 {
		if err := kv.installSnapshotData(snapshot); err != nil {
			// A corrupt snapshot is fatal in principle; starting empty
			// would silently lose data, so surface it loudly.
			kv.logger.Error("failed to restore snapshot at startup", "err", err)
		}
	}
	kv.applyWG.Go(kv.readRaftApplyCommandLoop)
	return kv
}

// Stop terminates the apply loop and waits for it to exit, so no trailing
// command execution or snapshot write can outlive the call. It is
// idempotent. Committed-but-unprocessed messages are not lost: after a
// restart Raft replays everything beyond the persisted snapshot point and
// the deduplication table keeps execution exactly-once.
func (kv *KVServer) Stop() {
	kv.doneOnce.Do(func() { close(kv.done) })
	kv.applyWG.Wait()
}

// Get serves a linearizable read. Like the C++ original (and 6.824), the
// read is pushed through the Raft log so it is ordered against concurrent
// writes; the handler then waits for the committed entry to come back
// through the apply channel.
func (kv *KVServer) Get(ctx context.Context, args *GetArgs) (*GetReply, error) {
	reply := &GetReply{}
	op := Op{
		Operation: OpGet,
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	encoded, err := op.encode()
	if err != nil {
		return nil, err
	}
	index, _, isLeader := kv.rf.Start(encoded)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return reply, nil
	}
	kv.logger.Debug("Get submitted to raft", "key", args.Key,
		"clientId", args.ClientId, "requestId", args.RequestId, "index", index)

	ch := kv.getWaitChan(index)
	defer kv.releaseWaitChan(index, ch)

	select {
	case commitOp := <-ch:
		if commitOp.ClientId == op.ClientId && commitOp.RequestId == op.RequestId {
			value, exist := kv.executeGetOpOnKVDB(op)
			if exist {
				reply.Err, reply.Value = OK, value
			} else {
				reply.Err = ErrNoKey
			}
		} else {
			// A different command committed at this index: leadership
			// changed between Start and commit.
			reply.Err = ErrWrongLeader
		}
	case <-time.After(ConsensusTimeout):
		// The command may or may not have committed. A read is safe to
		// answer locally when this node is still the leader and the
		// request was already executed once (deduplication hit), which
		// preserves linearizability.
		if _, stillLeader := kv.rf.GetState(); stillLeader && kv.ifRequestDuplicate(op.ClientId, op.RequestId) {
			value, exist := kv.executeGetOpOnKVDB(op)
			if exist {
				reply.Err, reply.Value = OK, value
			} else {
				reply.Err = ErrNoKey
			}
		} else {
			reply.Err = ErrWrongLeader // let the clerk retry elsewhere
		}
	}
	return reply, nil
}

// PutAppend serves a write request. The command is submitted to Raft and
// the handler waits for the commit to come back through the apply
// channel; timeouts fall back to the deduplication table so a retried
// client never executes the same write twice.
func (kv *KVServer) PutAppend(ctx context.Context, args *PutAppendArgs) (*PutAppendReply, error) {
	reply := &PutAppendReply{}
	if args.Op != OpPut && args.Op != OpAppend {
		reply.Err = ErrInvalidOp
		return reply, nil
	}
	op := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	encoded, err := op.encode()
	if err != nil {
		return nil, err
	}
	index, _, isLeader := kv.rf.Start(encoded)
	if !isLeader {
		kv.logger.Debug("PutAppend rejected: not leader", "key", args.Key,
			"clientId", args.ClientId, "requestId", args.RequestId)
		reply.Err = ErrWrongLeader
		return reply, nil
	}
	kv.logger.Debug("PutAppend submitted to raft", "op", args.Op, "key", args.Key,
		"clientId", args.ClientId, "requestId", args.RequestId, "index", index)

	ch := kv.getWaitChan(index)
	defer kv.releaseWaitChan(index, ch)

	select {
	case commitOp := <-ch:
		if commitOp.ClientId == op.ClientId && commitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			// Leadership changed between Start and commit; the entry at
			// this index belongs to somebody else.
			reply.Err = ErrWrongLeader
		}
	case <-time.After(ConsensusTimeout):
		if kv.ifRequestDuplicate(op.ClientId, op.RequestId) {
			// The write executed before the timeout; only the reply was
			// lost. Acknowledging again is safe and idempotent.
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader // let the clerk retry elsewhere
		}
	}
	return reply, nil
}

// readRaftApplyCommandLoop consumes the raft apply channel forever,
// executing committed commands and installing snapshots.
func (kv *KVServer) readRaftApplyCommandLoop() {
	for {
		// Prefer termination when Stop and a pending message are both
		// ready, so a busy apply channel cannot delay shutdown.
		select {
		case <-kv.done:
			return
		default:
		}
		select {
		case msg := <-kv.applyChan:
			if msg.CommandValid {
				kv.getCommandFromRaft(msg)
			}
			if msg.SnapshotValid {
				kv.getSnapshotFromRaft(msg)
			}
		case <-kv.done:
			return
		}
	}
}

// getCommandFromRaft executes one committed command against the state
// machine, checks whether a snapshot is due and wakes the RPC handler
// waiting for this log index.
func (kv *KVServer) getCommandFromRaft(msg raft.ApplyMsg) {
	op, err := decodeOp(msg.Command)
	if err != nil {
		kv.logger.Error("dropping undecodable command", "index", msg.CommandIndex, "err", err)
		return
	}
	kv.logger.Debug("applying committed command", "index", msg.CommandIndex,
		"op", op.Operation, "key", op.Key, "clientId", op.ClientId, "requestId", op.RequestId)

	kv.mu.Lock()
	stale := msg.CommandIndex <= kv.lastSnapShotRaftLogIndex
	kv.mu.Unlock()
	if stale {
		// Already covered by an installed snapshot; this message was
		// queued before the snapshot landed.
		return
	}

	if !kv.ifRequestDuplicate(op.ClientId, op.RequestId) {
		switch op.Operation {
		case OpPut:
			kv.executePutOpOnKVDB(op)
		case OpAppend:
			kv.executeAppendOpOnKVDB(op)
		case OpGet:
			// Reads never mutate the database. The waiting RPC handler
			// executes and records the read locally once the commit is
			// signalled below.
		default:
			kv.logger.Warn("unknown operation in committed log", "op", op.Operation)
		}
	}
	if kv.maxRaftState != -1 {
		kv.ifNeedToSendSnapShotCommand(msg.CommandIndex)
	}
	kv.sendMessageToWaitChan(op, msg.CommandIndex)
}

// getSnapshotFromRaft installs a snapshot received from the leader.
func (kv *KVServer) getSnapshotFromRaft(msg raft.ApplyMsg) {
	if !kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		return
	}
	if err := kv.installSnapshotData(msg.Snapshot); err != nil {
		kv.logger.Error("failed to install snapshot from leader", "err", err)
		return
	}
	kv.mu.Lock()
	kv.lastSnapShotRaftLogIndex = max(kv.lastSnapShotRaftLogIndex, msg.SnapshotIndex)
	kv.mu.Unlock()
}

// installSnapshotData replaces the state machine with the snapshot
// content.
func (kv *KVServer) installSnapshotData(snapshot []byte) error {
	var payload snapshotPayload
	if err := gob.NewDecoder(bytes.NewReader(snapshot)).Decode(&payload); err != nil {
		return fmt.Errorf("kvserver: decode snapshot: %w", err)
	}
	if payload.DB == nil {
		payload.DB = make(map[string]string)
	}
	if payload.LastRequestId == nil {
		payload.LastRequestId = make(map[string]int)
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.db = payload.DB
	kv.lastRequestId = payload.LastRequestId
	kv.lastSnapShotRaftLogIndex = max(kv.lastSnapShotRaftLogIndex, payload.LastSnapshotIndex)
	kv.logger.Info("state machine restored from snapshot",
		"snapshotIndex", payload.LastSnapshotIndex, "keys", len(kv.db))
	return nil
}

// executePutOpOnKVDB stores the value, replacing any previous one.
func (kv *KVServer) executePutOpOnKVDB(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.db[op.Key] = op.Value
	kv.lastRequestId[op.ClientId] = op.RequestId
}

// executeAppendOpOnKVDB appends to the current value of the key.
//
// FIX: the C++ original routed Append through the skip list's
// insert_set_element, which overwrote the value and made Append behave
// exactly like Put; concatenation only existed in a commented-out map
// implementation. Append concatenates again here, matching both the
// operation's name and the 6.824 semantics the project follows.
func (kv *KVServer) executeAppendOpOnKVDB(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.db[op.Key] += op.Value
	kv.lastRequestId[op.ClientId] = op.RequestId
}

// executeGetOpOnKVDB reads the key and records the request id.
func (kv *KVServer) executeGetOpOnKVDB(op Op) (value string, exist bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exist = kv.db[op.Key]
	kv.lastRequestId[op.ClientId] = op.RequestId
	return value, exist
}

// ifRequestDuplicate reports whether requestId has already been executed
// for this client. Request ids increase monotonically per client, so
// anything at or below the last executed id is a retry.
func (kv *KVServer) ifRequestDuplicate(clientId string, requestId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	last, ok := kv.lastRequestId[clientId]
	if !ok {
		return false
	}
	return requestId <= last
}

// ifNeedToSendSnapShotCommand snapshots the state machine and compacts
// the raft log once the persisted Raft state exceeds the configured
// proportion of maxRaftState.
func (kv *KVServer) ifNeedToSendSnapShotCommand(raftIndex int) {
	if kv.rf.GetRaftStateSize()*snapshotDenominator <= kv.maxRaftState*snapshotNumerator {
		return
	}
	snapshot, err := kv.makeSnapShot(raftIndex)
	if err != nil {
		kv.logger.Error("failed to build snapshot", "err", err)
		return
	}
	kv.rf.Snapshot(raftIndex, snapshot)
}

// makeSnapShot gob-encodes the current database and deduplication table.
func (kv *KVServer) makeSnapShot(raftIndex int) ([]byte, error) {
	kv.mu.Lock()
	payload := snapshotPayload{
		DB:                kv.db,
		LastRequestId:     kv.lastRequestId,
		LastSnapshotIndex: raftIndex,
	}
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(&payload)
	kv.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("kvserver: encode snapshot: %w", err)
	}
	return buf.Bytes(), nil
}

// getWaitChan returns the channel on which the handler for log index
// waits, creating it on first use.
func (kv *KVServer) getWaitChan(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.waitApplyCh[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.waitApplyCh[index] = ch
	}
	return ch
}

// releaseWaitChan removes the waiting channel for index, but only if it
// is still the one this request created.
//
// FIX: after a leader change the same log index can be reused by a
// different in-flight request in a new term; the C++ original erased the
// map entry unconditionally and could therefore delete the new request's
// channel, forcing it to time out.
func (kv *KVServer) releaseWaitChan(index int, ch chan Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if cur, ok := kv.waitApplyCh[index]; ok && cur == ch {
		delete(kv.waitApplyCh, index)
	}
}

// sendMessageToWaitChan wakes the RPC handler waiting on this log index,
// if any. Followers (and timed-out leaders) have no waiter, which is
// normal and not an error.
func (kv *KVServer) sendMessageToWaitChan(op Op, index int) {
	kv.mu.Lock()
	ch, ok := kv.waitApplyCh[index]
	kv.mu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- op:
		kv.logger.Debug("delivered applied command to waiting handler",
			"index", index, "clientId", op.ClientId, "requestId", op.RequestId)
	default:
		// A waiter consumes at most one result; a full channel means
		// this index was already signalled.
	}
}
