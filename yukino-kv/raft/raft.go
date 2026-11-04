// Package raft implements the Raft consensus algorithm: leader election,
// log replication and log compaction through snapshots.
//
// The control flow deliberately mirrors the original so the two
// implementations can be read side by side, with a few deliberate
// corrections where the C++ code was buggy (each is marked with a FIX
// comment). C++ threads and the bundled fiber library become goroutines,
// boost serialization becomes encoding/gob, and the custom mprpc framework
// becomes yukino_rpc.
package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// Timing parameters ported from the C++ config.h. The heartbeat interval
// must stay an order of magnitude below the election timeout so a leader
// can assert itself well before followers start new elections.
const (
	// HeartBeatTimeout is the interval between leader heartbeats.
	HeartBeatTimeout = 25 * time.Millisecond
	// ApplyInterval is the polling interval of the applier goroutine that
	// forwards committed entries to the state machine.
	ApplyInterval = 10 * time.Millisecond
	// MinElectionTimeout and MaxElectionTimeout bound the randomized
	// follower election timeout.
	MinElectionTimeout = 300 * time.Millisecond
	MaxElectionTimeout = 500 * time.Millisecond
	// applyChanBuffer sizes the channel between Raft and the state
	// machine. A briefly stalled consumer should not block Raft.
	applyChanBuffer = 256
)

// Status is the role of a node in the current term.
type Status int

// The three Raft roles.
const (
	Follower Status = iota
	Candidate
	Leader
)

// String renders the role for logs.
func (s Status) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("status(%d)", int(s))
	}
}

// ApplyMsg carries one committed log entry, or one installed snapshot,
// from the Raft node up to the state machine.
type ApplyMsg struct {
	CommandValid  bool
	Command       []byte
	CommandIndex  int
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Progress is a point-in-time view of a node's state, useful for
// monitoring and tests.
type Progress struct {
	Term          int
	Status        Status
	IsLeader      bool
	VotedFor      int
	CommitIndex   int
	LastApplied   int
	LastLogIndex  int
	SnapshotIndex int
	SnapshotTerm  int
}

// persistedState is the gob-encoded durable Raft state: everything a node
// must remember across restarts to preserve safety.
type persistedState struct {
	CurrentTerm              int
	VotedFor                 int
	LastSnapshotIncludeIndex int
	LastSnapshotIncludeTerm  int
	Logs                     []LogEntry
}

// Raft is a single Raft node. All mutable state is guarded by mu; the
// network calls run in their own goroutines and never hold mu while
// performing I/O.
type Raft struct {
	mu        sync.Mutex
	peers     []*Peer // peers[me] is nil; a node never RPCs itself
	persister *Persister
	me        int
	logger    *slog.Logger

	currentTerm int
	votedFor    int
	logs        []LogEntry // entries after the snapshot point

	commitIndex int // highest entry known to be committed
	lastApplied int // highest entry handed to the state machine

	// Leader-only volatile state (reinitialized after each election):
	nextIndex  []int // index of the next entry to send to peer i
	matchIndex []int // highest entry known to be replicated on peer i

	status Status

	applyChan chan ApplyMsg

	lastResetElectionTime  time.Time
	lastResetHeartBeatTime time.Time

	lastSnapshotIncludeIndex int
	lastSnapshotIncludeTerm  int

	shutdown     chan struct{}
	shutdownOnce sync.Once
	// tickers tracks the three background goroutines so Kill can wait
	// for them to exit and no ticker can persist state afterwards.
	tickers sync.WaitGroup
}

// NewRaft creates a Raft node and starts its background goroutines.
//
// peers holds one stub per cluster member and must be nil at index me.
// applyCh delivers committed commands and installed snapshots to the state
// machine and should be buffered. The node restores any state previously
// written through persister before joining the cluster.
func NewRaft(peers []*Peer, me int, persister *Persister, applyCh chan ApplyMsg, logger *slog.Logger) (*Raft, error) {
	if me < 0 || me >= len(peers) {
		return nil, fmt.Errorf("raft: node id %d out of range for %d peers", me, len(peers))
	}
	if logger == nil {
		logger = slog.Default()
	}
	now := time.Now()
	rf := &Raft{
		peers:                  peers,
		persister:              persister,
		me:                     me,
		logger:                 logger.With("component", "raft"),
		currentTerm:            0,
		votedFor:               -1,
		status:                 Follower,
		applyChan:              applyCh,
		nextIndex:              make([]int, len(peers)),
		matchIndex:             make([]int, len(peers)),
		lastResetElectionTime:  now,
		lastResetHeartBeatTime: now,
		shutdown:               make(chan struct{}),
	}
	if err := rf.readPersist(persister.ReadRaftState()); err != nil {
		return nil, fmt.Errorf("raft: restore persisted state: %w", err)
	}
	if rf.lastSnapshotIncludeIndex > 0 {
		// Everything up to the snapshot point is committed and applied by
		// definition. FIX: the C++ original restored lastApplied here but
		// left commitIndex at 0, so a restarted node briefly believed
		// committed data was uncommitted and could re-apply entries its
		// state machine had already absorbed.
		rf.lastApplied = rf.lastSnapshotIncludeIndex
		rf.commitIndex = rf.lastSnapshotIncludeIndex
	}
	rf.logger.Info("raft node initialized",
		"term", rf.currentTerm,
		"votedFor", rf.votedFor,
		"snapshotIndex", rf.lastSnapshotIncludeIndex,
		"snapshotTerm", rf.lastSnapshotIncludeTerm,
		"logs", len(rf.logs))

	rf.tickers.Go(rf.electionTicker)
	rf.tickers.Go(rf.heartbeatTicker)
	rf.tickers.Go(rf.applierTicker)
	return rf, nil
}

// GetState returns the current term and whether this node believes it is
// the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
}

// Progress returns a consistent snapshot of the node's key state.
func (rf *Raft) Progress() Progress {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIndex, _ := rf.getLastLogIndexAndTermLocked()
	return Progress{
		Term:          rf.currentTerm,
		Status:        rf.status,
		IsLeader:      rf.status == Leader,
		VotedFor:      rf.votedFor,
		CommitIndex:   rf.commitIndex,
		LastApplied:   rf.lastApplied,
		LastLogIndex:  lastLogIndex,
		SnapshotIndex: rf.lastSnapshotIncludeIndex,
		SnapshotTerm:  rf.lastSnapshotIncludeTerm,
	}
}

// GetRaftStateSize reports the persisted Raft state size in bytes. The KV
// layer compares it against its snapshot threshold.
func (rf *Raft) GetRaftStateSize() int { return rf.persister.RaftStateSize() }

// Start proposes command to the Raft log. It returns immediately with the
// log index and term assigned to the command and whether this node is the
// leader; the command is only committed once it comes back through the
// apply channel. Callers that are not the leader should redirect their
// client to retry elsewhere.
func (rf *Raft) Start(command []byte) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Leader {
		return -1, -1, false
	}
	entry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   rf.getLastLogIndexLocked() + 1,
	}
	rf.logs = append(rf.logs, entry)
	rf.persistLocked()
	rf.logger.Debug("accepted new command", "index", entry.Index, "term", entry.Term)
	// Replication rides the next heartbeat, exactly like the C++ original.
	// Triggering an immediate heartbeat was found to be unstable there and
	// is left as a possible latency optimization.
	return entry.Index, entry.Term, true
}

// Snapshot tells Raft that the state machine has captured all state
// through the given (already committed) log index, so Raft may discard
// that log prefix and store snapshot as the authoritative state image.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastSnapshotIncludeIndex || index > rf.commitIndex {
		rf.logger.Debug("rejected snapshot request",
			"index", index,
			"snapshotIndex", rf.lastSnapshotIncludeIndex,
			"commitIndex", rf.commitIndex)
		return
	}
	lastLogIndex := rf.getLastLogIndexLocked()
	cut := rf.getSlicesIndexFromLogIndexLocked(index)
	newTerm := rf.logs[cut].Term
	// Keep only the entries after the snapshot point.
	remaining := make([]LogEntry, len(rf.logs)-cut-1)
	copy(remaining, rf.logs[cut+1:])
	rf.lastSnapshotIncludeIndex = index
	rf.lastSnapshotIncludeTerm = newTerm
	rf.logs = remaining
	rf.commitIndex = max(rf.commitIndex, index)
	rf.lastApplied = max(rf.lastApplied, index)
	if err := rf.persister.Save(rf.persistDataLocked(), snapshot); err != nil {
		rf.logger.Error("failed to persist snapshot", "err", err)
	}
	if len(rf.logs)+rf.lastSnapshotIncludeIndex != lastLogIndex {
		panic(fmt.Sprintf("raft %d: snapshot corrupted the log: len(logs)=%d + snapshotIndex=%d != lastLogIndex=%d",
			rf.me, len(rf.logs), rf.lastSnapshotIncludeIndex, lastLogIndex))
	}
	rf.logger.Info("compacted log into snapshot",
		"snapshotIndex", index, "snapshotTerm", newTerm, "remainingLogs", len(rf.logs))
}

// CondInstallSnapshot is retained for API parity with the original code
// base. Modern Raft (6.824 since 2021) installs leader snapshots
// unconditionally, so it always returns true.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// Kill stops all background goroutines and waits for them to exit, so no
// ticker can persist state or touch the apply channel after Kill returns.
// In-flight RPC replies are ignored afterwards. Kill is idempotent; the
// wait is bounded because every blocking select also observes shutdown.
func (rf *Raft) Kill() {
	rf.shutdownOnce.Do(func() {
		close(rf.shutdown)
		rf.tickers.Wait()
	})
}

// killed reports whether Kill has been called.
func (rf *Raft) killed() bool {
	select {
	case <-rf.shutdown:
		return true
	default:
		return false
	}
}

// electionTicker watches the election timer. When it expires without any
// heartbeat or granted vote having reset it, the node starts an election.
func (rf *Raft) electionTicker() {
	for {
		rf.mu.Lock()
		wakeTime := time.Now()
		// Leaders have no election timer of their own; idle briefly and
		// re-check, mirroring the C++ inner status loop.
		sleepFor := HeartBeatTimeout
		if rf.status != Leader {
			sleepFor = randomizedElectionTimeout() - wakeTime.Sub(rf.lastResetElectionTime)
		}
		rf.mu.Unlock()

		if sleepFor > time.Millisecond {
			if !rf.sleepUntilShutdown(sleepFor) {
				return
			}
		}

		rf.mu.Lock()
		// If the timer was reset while we slept (heartbeat received, vote
		// granted, election started) or the node became the leader, the
		// timeout did not actually expire.
		expired := rf.status != Leader && !rf.lastResetElectionTime.After(wakeTime)
		killed := rf.killed()
		rf.mu.Unlock()
		if killed {
			return
		}
		if !expired {
			continue
		}
		rf.doElection()
	}
}

// heartbeatTicker drives the leader's periodic AppendEntries broadcast.
func (rf *Raft) heartbeatTicker() {
	for {
		rf.mu.Lock()
		isLeader := rf.status == Leader
		wakeTime := time.Now()
		sleepFor := HeartBeatTimeout - wakeTime.Sub(rf.lastResetHeartBeatTime)
		rf.mu.Unlock()

		if !isLeader {
			// Nothing to do while not leader; re-check soon.
			if !rf.sleepUntilShutdown(HeartBeatTimeout) {
				return
			}
			continue
		}
		if sleepFor > time.Millisecond {
			if !rf.sleepUntilShutdown(sleepFor) {
				return
			}
		}

		rf.mu.Lock()
		expired := rf.status == Leader && !rf.lastResetHeartBeatTime.After(wakeTime)
		killed := rf.killed()
		rf.mu.Unlock()
		if killed {
			return
		}
		if !expired {
			continue
		}
		rf.doHeartBeat()
	}
}

// applierTicker forwards committed-but-not-yet-applied log entries to the
// state machine, in index order.
func (rf *Raft) applierTicker() {
	for {
		rf.mu.Lock()
		msgs := rf.getApplyLogsLocked()
		rf.mu.Unlock()
		for _, msg := range msgs {
			if !rf.pushMsgToApplyChan(msg) {
				return
			}
		}
		if !rf.sleepUntilShutdown(ApplyInterval) {
			return
		}
	}
}

// pushMsgToApplyChan delivers one message without holding the Raft mutex.
// It reports false once Kill has been called.
func (rf *Raft) pushMsgToApplyChan(msg ApplyMsg) bool {
	select {
	case rf.applyChan <- msg:
		return true
	case <-rf.shutdown:
		return false
	}
}

// sleepUntilShutdown sleeps for d, returning early (false) on Kill.
func (rf *Raft) sleepUntilShutdown(d time.Duration) bool {
	if d <= 0 {
		return !rf.killed()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-rf.shutdown:
		return false
	case <-timer.C:
		return true
	}
}

// getApplyLogsLocked collects the messages for all entries between
// lastApplied and commitIndex and advances lastApplied.
func (rf *Raft) getApplyLogsLocked() []ApplyMsg {
	if rf.commitIndex > rf.getLastLogIndexLocked() {
		panic(fmt.Sprintf("raft %d: commitIndex %d > lastLogIndex %d",
			rf.me, rf.commitIndex, rf.getLastLogIndexLocked()))
	}
	var msgs []ApplyMsg
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		si := rf.getSlicesIndexFromLogIndexLocked(rf.lastApplied)
		if rf.logs[si].Index != rf.lastApplied {
			panic(fmt.Sprintf("raft %d: log entry at %d has index %d",
				rf.me, rf.lastApplied, rf.logs[si].Index))
		}
		msgs = append(msgs, ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[si].Command,
			CommandIndex: rf.lastApplied,
		})
	}
	return msgs
}

// doElection transitions the node to candidate and requests votes from
// every peer. Each RPC runs in its own goroutine; the shared votedNum
// counter is only touched under the Raft mutex.
func (rf *Raft) doElection() {
	rf.mu.Lock()
	if rf.killed() || rf.status == Leader {
		rf.mu.Unlock()
		return
	}
	rf.status = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me // vote for self; also blocks voting for peers this term
	rf.persistLocked()
	rf.lastResetElectionTime = time.Now()

	votedNum := 1 // the candidate always votes for itself
	lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTermLocked()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	term := rf.currentTerm
	rf.mu.Unlock()

	rf.logger.Info("starting election", "term", term,
		"lastLogIndex", lastLogIndex, "lastLogTerm", lastLogTerm)
	for i := range rf.peers {
		if i == rf.me || rf.peers[i] == nil {
			continue
		}
		go rf.sendRequestVote(i, args, &votedNum)
	}
}

// sendRequestVote issues one RequestVote RPC and processes the reply.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, votedNum *int) {
	start := time.Now()
	reply, ok := rf.peers[server].RequestVote(context.Background(), args)
	if !ok {
		rf.logger.Debug("RequestVote RPC failed", "peer", server, "term", args.Term)
		return
	}
	rf.logger.Debug("RequestVote RPC completed", "peer", server,
		"term", args.Term, "granted", reply.VoteGranted, "elapsed", time.Since(start))

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	// Whatever the reply says, a newer term forces the three changes:
	// revert to follower, adopt the term, clear the vote.
	if reply.Term > rf.currentTerm {
		rf.logger.Debug("stepping down after RequestVote reply",
			"peer", server, "replyTerm", reply.Term, "currentTerm", rf.currentTerm)
		rf.becomeFollowerLocked(reply.Term)
		return
	}
	if reply.Term < rf.currentTerm {
		return // stale reply from an older election round
	}
	// FIX: the node may have stopped being the candidate of this term
	// while the RPC was in flight (it saw the real leader or a newer
	// term). The C++ original lacked this guard and could promote a
	// follower back to "leader" from a stale vote reply.
	if rf.status != Candidate || rf.currentTerm != args.Term {
		return
	}
	if !reply.VoteGranted {
		return
	}
	*votedNum++
	if *votedNum < len(rf.peers)/2+1 {
		return
	}
	if rf.status == Leader {
		// Already promoted by an earlier reply in this term.
		return
	}
	*votedNum = 0
	rf.status = Leader
	lastLogIndex := rf.getLastLogIndexLocked()
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.logger.Info("election won; became leader", "term", rf.currentTerm,
		"lastLogIndex", lastLogIndex)
	// Announce leadership immediately instead of waiting for the first
	// heartbeat tick. Runs in a goroutine because doHeartBeat needs the
	// mutex held here.
	go rf.doHeartBeat()
}

// doHeartBeat broadcasts an AppendEntries RPC (or an InstallSnapshot RPC
// for peers that fell behind the snapshot point) to every other node. It
// is called by the heartbeat ticker and right after winning an election.
func (rf *Raft) doHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.status != Leader {
		return
	}
	rf.logger.Debug("heartbeat tick; broadcasting AppendEntries", "term", rf.currentTerm)
	for i := range rf.peers {
		if i == rf.me || rf.peers[i] == nil {
			continue
		}
		if rf.nextIndex[i] < 1 {
			panic(fmt.Sprintf("raft %d: nextIndex[%d] = %d", rf.me, i, rf.nextIndex[i]))
		}
		// The peer needs log we already compacted away: send the
		// snapshot instead of AppendEntries.
		if rf.nextIndex[i] <= rf.lastSnapshotIncludeIndex {
			go rf.leaderSendSnapshot(i)
			continue
		}
		prevIndex, prevTerm := rf.getPrevLogInfoLocked(i)
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			LeaderCommit: rf.commitIndex,
		}
		// Send everything after prevIndex. logs[0].Index is always
		// lastSnapshotIncludeIndex+1, so the slice start is uniform.
		start := prevIndex - rf.lastSnapshotIncludeIndex
		args.Entries = append(args.Entries, rf.logs[start:]...)
		if args.PrevLogIndex+len(args.Entries) != rf.getLastLogIndexLocked() {
			panic(fmt.Sprintf("raft %d: prevLogIndex %d + %d entries != lastLogIndex %d",
				rf.me, args.PrevLogIndex, len(args.Entries), rf.getLastLogIndexLocked()))
		}
		go rf.sendAppendEntries(i, args)
	}
	rf.lastResetHeartBeatTime = time.Now()
}

// getPrevLogInfoLocked returns the index and term of the entry immediately
// preceding the next one to be sent to server.
func (rf *Raft) getPrevLogInfoLocked(server int) (prevIndex, prevTerm int) {
	if rf.nextIndex[server] == rf.lastSnapshotIncludeIndex+1 {
		// The first entry we can still send sits right after the
		// snapshot, so the consistency check refers to the snapshot's
		// last included entry.
		return rf.lastSnapshotIncludeIndex, rf.lastSnapshotIncludeTerm
	}
	prevIndex = rf.nextIndex[server] - 1
	prevTerm = rf.logs[rf.getSlicesIndexFromLogIndexLocked(prevIndex)].Term
	return prevIndex, prevTerm
}

// sendAppendEntries issues one AppendEntries RPC and processes the reply.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	reply, ok := rf.peers[server].AppendEntries(context.Background(), args)
	if !ok {
		rf.logger.Debug("AppendEntries RPC failed", "peer", server, "term", args.Term)
		return
	}
	if reply.AppState == Disconnected {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.logger.Debug("stepping down after AppendEntries reply",
			"peer", server, "replyTerm", reply.Term, "currentTerm", rf.currentTerm)
		// FIX: becomeFollowerLocked persists the new term. The C++
		// original updated currentTerm and votedFor here without
		// persisting, so a crash right after could resurrect the stale
		// term.
		rf.becomeFollowerLocked(reply.Term)
		return
	}
	if rf.status != Leader || reply.Term < rf.currentTerm {
		return // stale reply; no longer relevant to this node
	}
	if !reply.Success {
		// Log mismatch: the follower tells us where to resume. Never
		// move nextIndex blindly — a delayed reply from an earlier
		// round must not undo newer progress, a hazard the C++ original
		// noted in a comment but did not guard against.
		if reply.UpdateNextIndex > 0 && reply.UpdateNextIndex < rf.nextIndex[server] {
			rf.logger.Debug("log mismatch; rewinding nextIndex",
				"peer", server, "from", rf.nextIndex[server], "to", reply.UpdateNextIndex)
			rf.nextIndex[server] = reply.UpdateNextIndex
		}
		return
	}
	rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	rf.leaderUpdateCommitIndexLocked()
}

// leaderUpdateCommitIndexLocked advances commitIndex to the highest
// current-term entry replicated on a majority. Only current-term entries
// may be committed by count (Raft Figure 8); entries from older terms
// commit indirectly once a current-term entry above them commits.
//
// FIX: the C++ original implemented this scan but never called it,
// committing instead from a shared per-heartbeat-round ack counter whose
// threshold could be crossed by an empty heartbeat reply, delaying commits
// by a full round and entangling commit with RPC bookkeeping. Scanning
// matchIndex is both simpler and robust.
func (rf *Raft) leaderUpdateCommitIndexLocked() {
	for index := rf.getLastLogIndexLocked(); index > rf.commitIndex; index-- {
		if rf.getLogTermFromLogIndexLocked(index) != rf.currentTerm {
			continue
		}
		replicated := 1 // ourselves
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= index {
				replicated++
			}
		}
		if replicated >= len(rf.peers)/2+1 {
			rf.logger.Debug("advancing commitIndex", "from", rf.commitIndex, "to", index)
			rf.commitIndex = index
			return
		}
	}
}

// leaderSendSnapshot ships the persisted snapshot to one lagging peer.
func (rf *Raft) leaderSendSnapshot(server int) {
	rf.mu.Lock()
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
	args := &InstallSnapshotRequest{
		LeaderId:                 rf.me,
		Term:                     rf.currentTerm,
		LastSnapShotIncludeIndex: rf.lastSnapshotIncludeIndex,
		LastSnapShotIncludeTerm:  rf.lastSnapshotIncludeTerm,
		Data:                     rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	reply, ok := rf.peers[server].InstallSnapshot(context.Background(), args)
	if !ok {
		rf.logger.Debug("InstallSnapshot RPC failed", "peer", server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	// Leadership may have changed while the RPC was in flight.
	if rf.status != Leader || rf.currentTerm != args.Term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		return
	}
	// matchIndex must never move backwards, even from a snapshot.
	rf.matchIndex[server] = max(rf.matchIndex[server], args.LastSnapShotIncludeIndex)
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	rf.logger.Debug("installed snapshot on peer", "peer", server,
		"snapshotIndex", args.LastSnapShotIncludeIndex)
}

// becomeFollowerLocked performs the "three changes" required whenever a
// node observes a newer term: revert to follower, adopt the term and
// clear the vote. The new term is persisted before returning, as Raft
// requires.
func (rf *Raft) becomeFollowerLocked(newTerm int) {
	rf.status = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.lastResetElectionTime = time.Now()
	rf.persistLocked()
}

// handleRequestVote is the follower-side RequestVote logic (2A).
func (rf *Raft) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// The candidate is out of date; do not reset the election timer.
		reply.VoteState = VoteExpired
		reply.VoteGranted = false
		return
	}
	changed := false
	if args.Term > rf.currentTerm {
		// Figure 2: on seeing a newer term, revert to follower.
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.status = Follower
		reply.Term = rf.currentTerm
		changed = true
		// Deliberately no return: the vote may still be granted in the
		// new term.
	}
	switch {
	case !rf.upToDateLocked(args.LastLogIndex, args.LastLogTerm):
		// The candidate's log is older than ours.
		reply.VoteState = VoteRefused
		reply.VoteGranted = false
	case rf.votedFor != -1 && rf.votedFor != args.CandidateId:
		// Already voted for somebody else this term. (Re-granting to the
		// same candidate keeps retried RPCs idempotent.)
		reply.VoteState = VoteRefused
		reply.VoteGranted = false
	default:
		rf.votedFor = args.CandidateId
		// Only reset the election timer when actually casting a vote.
		rf.lastResetElectionTime = time.Now()
		reply.VoteState = VoteGranted
		reply.VoteGranted = true
		changed = true
		rf.logger.Debug("granted vote", "candidate", args.CandidateId, "term", args.Term)
	}
	if changed {
		// Term changes and granted votes must be durable before the
		// reply leaves the node, or a crash could let us vote twice in
		// one term.
		rf.persistLocked()
	}
}

// handleAppendEntries is the follower-side AppendEntries logic (2B).
func (rf *Raft) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Producing this reply at all proves the network path is healthy.
	reply.AppState = AppNormal
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.UpdateNextIndex = staleLeaderHint
		// Do not reset the election timer for a stale leader.
		return
	}
	changed := false
	defer func() {
		if changed {
			rf.persistLocked()
		}
	}()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		changed = true
	}
	// A candidate that hears from a same-term leader converts to
	// follower; so does any node whose term just moved.
	rf.status = Follower
	rf.lastResetElectionTime = time.Now()
	reply.Term = rf.currentTerm

	lastLogIndex := rf.getLastLogIndexLocked()
	switch {
	case args.PrevLogIndex > lastLogIndex:
		// The leader assumes we have log we do not have.
		reply.Success = false
		reply.UpdateNextIndex = lastLogIndex + 1
	case args.PrevLogIndex < rf.lastSnapshotIncludeIndex:
		// The leader is behind our snapshot point and must send the
		// snapshot instead.
		//
		// FIX: the C++ original filled in the reply here but forgot to
		// return, falling through into matchLog(), whose assertion
		// aborted the whole process on this rare but legal input.
		reply.Success = false
		reply.UpdateNextIndex = rf.lastSnapshotIncludeIndex + 1
	case !rf.matchLogLocked(args.PrevLogIndex, args.PrevLogTerm):
		// Consistency check failed. Hand the leader the first index of
		// the conflicting term block so it can skip the whole block in
		// one round instead of walking back entry by entry.
		reply.Success = false
		reply.UpdateNextIndex = args.PrevLogIndex
		conflictTerm := rf.getLogTermFromLogIndexLocked(args.PrevLogIndex)
		for index := args.PrevLogIndex; index >= rf.lastSnapshotIncludeIndex; index-- {
			if rf.getLogTermFromLogIndexLocked(index) != conflictTerm {
				reply.UpdateNextIndex = index + 1
				break
			}
		}
	default:
		// Append or overwrite entries one by one instead of truncating
		// eagerly: a delayed RPC may carry entries this node already
		// has, and blindly truncating could "take back" committed log.
		for _, entry := range args.Entries {
			if entry.Index > rf.getLastLogIndexLocked() {
				rf.logs = append(rf.logs, entry)
				changed = true
				continue
			}
			si := rf.getSlicesIndexFromLogIndexLocked(entry.Index)
			if rf.logs[si].Term == entry.Term {
				if !bytes.Equal(rf.logs[si].Command, entry.Command) {
					// Same index and term but different commands
					// violates the Log Matching property. Overwrite and
					// leave a loud trace instead of aborting the
					// process as the C++ assertion did.
					rf.logger.Error("log matching property violated; overwriting entry",
						"index", entry.Index, "term", entry.Term, "leader", args.LeaderId)
					rf.logs[si] = entry
					changed = true
				}
				continue
			}
			// Conflict: drop this entry and everything after it, then
			// take the leader's version. Later loop iterations hit the
			// append branch above.
			rf.logs = append(rf.logs[:si], entry)
			changed = true
		}
		if args.LeaderCommit > rf.commitIndex {
			// Never commit past the log we actually hold.
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndexLocked())
		}
		reply.Success = true
	}
}

// handleInstallSnapshot is the follower-side InstallSnapshot logic (2D).
func (rf *Raft) handleInstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.status = Follower
		// Persist the term bump immediately: the stale-snapshot early
		// return below must not skip it.
		rf.persistLocked()
	}
	rf.status = Follower
	rf.lastResetElectionTime = time.Now()
	reply.Term = rf.currentTerm

	// Ignore snapshots we have already moved past. Comparing against
	// commitIndex as well (the C++ original only compared the previous
	// snapshot point) also covers a follower whose applied state is
	// ahead of the leader's snapshot.
	if args.LastSnapShotIncludeIndex <= rf.lastSnapshotIncludeIndex ||
		args.LastSnapShotIncludeIndex <= rf.commitIndex {
		rf.logger.Debug("ignoring stale snapshot",
			"snapshotIndex", args.LastSnapShotIncludeIndex,
			"ownSnapshotIndex", rf.lastSnapshotIncludeIndex,
			"commitIndex", rf.commitIndex)
		return
	}

	// Discard everything the snapshot covers. If our log reaches beyond
	// the snapshot point, keep the tail; otherwise drop the whole log.
	if lastLogIndex := rf.getLastLogIndexLocked(); lastLogIndex > args.LastSnapShotIncludeIndex {
		cut := rf.getSlicesIndexFromLogIndexLocked(args.LastSnapShotIncludeIndex)
		remaining := make([]LogEntry, len(rf.logs)-cut-1)
		copy(remaining, rf.logs[cut+1:])
		rf.logs = remaining
	} else {
		rf.logs = nil
	}
	rf.lastSnapshotIncludeIndex = args.LastSnapShotIncludeIndex
	rf.lastSnapshotIncludeTerm = args.LastSnapShotIncludeTerm
	rf.commitIndex = max(rf.commitIndex, args.LastSnapShotIncludeIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastSnapShotIncludeIndex)
	if err := rf.persister.Save(rf.persistDataLocked(), args.Data); err != nil {
		rf.logger.Error("failed to persist installed snapshot", "err", err)
	}
	rf.logger.Info("installed snapshot from leader",
		"leader", args.LeaderId,
		"snapshotIndex", args.LastSnapShotIncludeIndex,
		"snapshotTerm", args.LastSnapShotIncludeTerm)

	// Hand the snapshot to the state machine asynchronously: the apply
	// channel may briefly block and the Raft mutex must not be held
	// while it does. Stale command messages that were already queued
	// ahead of this snapshot are filtered by the state machine through
	// its own snapshot index.
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastSnapShotIncludeTerm,
		SnapshotIndex: args.LastSnapShotIncludeIndex,
	}
	go rf.pushMsgToApplyChan(msg)
}

// upToDateLocked implements the election restriction: the candidate's log
// must be at least as current as ours.
func (rf *Raft) upToDateLocked(lastLogIndex, lastLogTerm int) bool {
	index, term := rf.getLastLogIndexAndTermLocked()
	return lastLogTerm > term || (lastLogTerm == term && lastLogIndex >= index)
}

// matchLogLocked reports whether the log holds an entry with the given
// index and term. The index must lie inside the range this node still
// keeps, snapshot point included.
func (rf *Raft) matchLogLocked(logIndex, logTerm int) bool {
	if logIndex < rf.lastSnapshotIncludeIndex || logIndex > rf.getLastLogIndexLocked() {
		panic(fmt.Sprintf("raft %d: matchLog index %d out of range [%d, %d]",
			rf.me, logIndex, rf.lastSnapshotIncludeIndex, rf.getLastLogIndexLocked()))
	}
	return logTerm == rf.getLogTermFromLogIndexLocked(logIndex)
}

// getLastLogIndexAndTermLocked returns the index and term of the newest
// log entry, falling back to the snapshot point when the log is empty.
func (rf *Raft) getLastLogIndexAndTermLocked() (int, int) {
	if len(rf.logs) == 0 {
		return rf.lastSnapshotIncludeIndex, rf.lastSnapshotIncludeTerm
	}
	last := rf.logs[len(rf.logs)-1]
	return last.Index, last.Term
}

func (rf *Raft) getLastLogIndexLocked() int {
	index, _ := rf.getLastLogIndexAndTermLocked()
	return index
}

// getLogTermFromLogIndexLocked returns the term of the entry at the given
// logical index, which must lie in [snapshotIndex, lastLogIndex].
func (rf *Raft) getLogTermFromLogIndexLocked(logIndex int) int {
	if logIndex < rf.lastSnapshotIncludeIndex || logIndex > rf.getLastLogIndexLocked() {
		panic(fmt.Sprintf("raft %d: log term index %d out of range [%d, %d]",
			rf.me, logIndex, rf.lastSnapshotIncludeIndex, rf.getLastLogIndexLocked()))
	}
	if logIndex == rf.lastSnapshotIncludeIndex {
		return rf.lastSnapshotIncludeTerm
	}
	return rf.logs[rf.getSlicesIndexFromLogIndexLocked(logIndex)].Term
}

// getSlicesIndexFromLogIndexLocked converts a logical log index into the
// physical position inside rf.logs. The logical index must be strictly
// greater than the snapshot point and at most the last log index.
func (rf *Raft) getSlicesIndexFromLogIndexLocked(logIndex int) int {
	if logIndex <= rf.lastSnapshotIncludeIndex || logIndex > rf.getLastLogIndexLocked() {
		panic(fmt.Sprintf("raft %d: slice index for %d out of range (%d, %d]",
			rf.me, logIndex, rf.lastSnapshotIncludeIndex, rf.getLastLogIndexLocked()))
	}
	return logIndex - rf.lastSnapshotIncludeIndex - 1
}

// persistLocked writes the durable Raft state through the persister.
func (rf *Raft) persistLocked() {
	if err := rf.persister.SaveRaftState(rf.persistDataLocked()); err != nil {
		rf.logger.Error("failed to persist raft state", "err", err)
	}
}

// persistDataLocked gob-encodes the durable Raft state. It replaces the
// boost text archive of the C++ original.
func (rf *Raft) persistDataLocked() []byte {
	state := persistedState{
		CurrentTerm:              rf.currentTerm,
		VotedFor:                 rf.votedFor,
		LastSnapshotIncludeIndex: rf.lastSnapshotIncludeIndex,
		LastSnapshotIncludeTerm:  rf.lastSnapshotIncludeTerm,
		Logs:                     rf.logs,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&state); err != nil {
		panic(fmt.Sprintf("raft %d: encode persisted state: %v", rf.me, err))
	}
	return buf.Bytes()
}

// readPersist restores the durable Raft state written by persistData.
// Empty input means "first boot" and is not an error.
func (rf *Raft) readPersist(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var state persistedState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&state); err != nil {
		return fmt.Errorf("decode raft state: %w", err)
	}
	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	rf.lastSnapshotIncludeIndex = state.LastSnapshotIncludeIndex
	rf.lastSnapshotIncludeTerm = state.LastSnapshotIncludeTerm
	rf.logs = state.Logs
	return nil
}

// randomizedElectionTimeout returns a random timeout in
// [MinElectionTimeout, MaxElectionTimeout). Randomization splits the vote
// when several followers time out at the same instant.
func randomizedElectionTimeout() time.Duration {
	return MinElectionTimeout + rand.N(MaxElectionTimeout-MinElectionTimeout)
}
