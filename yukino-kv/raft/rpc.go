package raft

import (
	"context"
	"time"

	"github.com/hangtiancheng/yukino.go/libs/yukino_rpc/pkg/rpc"
)

// ServiceName is the yukino_rpc service name under which every Raft node
// exposes RequestVote, AppendEntries and InstallSnapshot.
const ServiceName = "Raft"

// PeerRPCTimeout bounds a single inter-node Raft RPC. Calls run in their
// own goroutines, so a slow or unreachable peer only wastes its own
// goroutine; Raft timers keep running meanwhile.
const PeerRPCTimeout = 2 * time.Second

// Network state markers carried in AppendEntriesReply.AppState. They exist
// purely to make partition debugging easier: a reply that was actually
// produced by a peer always carries AppNormal, while the default value
// before the call is Disconnected.
const (
	Disconnected = 0
	AppNormal    = 1
)

// Vote states reported in RequestVoteReply.VoteState. They mirror the
// constants of the original C++ implementation.
const (
	VoteKilled  = 0 // unused; kept for parity with the C++ constants
	VoteRefused = 1 // already voted in this term, or the candidate log is stale
	VoteExpired = 2 // the candidate's term is older than ours
	VoteGranted = 3 // vote granted
)

// staleLeaderHint is the UpdateNextIndex value returned by a follower that
// rejects an AppendEntries RPC from a stale term. It tells the leader that
// this rejection carries no information about log positions, so nextIndex
// must not be adjusted.
const staleLeaderHint = -100

// LogEntry is one replicated state-machine command in the Raft log. Index
// is the logical position in the whole log (snapshot point included), not
// the position inside the in-memory slice.
type LogEntry struct {
	Command []byte `json:"command"`
	Term    int    `json:"term"`
	Index   int    `json:"index"`
}

// RequestVoteArgs is the RPC sent by a candidate to collect votes.
type RequestVoteArgs struct {
	Term         int `json:"term"`
	CandidateId  int `json:"candidateId"`
	LastLogIndex int `json:"lastLogIndex"`
	LastLogTerm  int `json:"lastLogTerm"`
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
	VoteState   int  `json:"voteState"`
}

// AppendEntriesArgs is the log-replication RPC. It doubles as the
// heartbeat: an entry-less AppendEntriesArgs carries no log payload.
type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderId     int        `json:"leaderId"`
	PrevLogIndex int        `json:"prevLogIndex"`
	PrevLogTerm  int        `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leaderCommit"`
}

// AppendEntriesReply is the response to an AppendEntries RPC.
type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
	// UpdateNextIndex hints the leader where to resume replication after a
	// mismatch. It is only meaningful when Success is false and the value
	// is positive.
	UpdateNextIndex int `json:"updateNextIndex"`
	// AppState reports whether the peer actually processed the request.
	AppState int `json:"appState"`
}

// InstallSnapshotRequest ships a compacted state-machine image to a
// follower whose required log prefix has already been discarded.
type InstallSnapshotRequest struct {
	LeaderId                 int    `json:"leaderId"`
	Term                     int    `json:"term"`
	LastSnapShotIncludeIndex int    `json:"lastSnapShotIncludeIndex"`
	LastSnapShotIncludeTerm  int    `json:"lastSnapShotIncludeTerm"`
	Data                     []byte `json:"data"`
}

// InstallSnapshotResponse only carries the receiver's term: a snapshot
// whose term checks out is accepted unconditionally.
type InstallSnapshotResponse struct {
	Term int `json:"term"`
}

// Peer is the client stub used to reach one remote Raft node over
// yukino_rpc. Connections are established lazily, so a peer may be dialed
// before its server process listens; failed calls simply report ok ==
// false and the normal Raft retry machinery takes over. This removes the
// fixed startup sleeps the C++ original needed to sequence process
// bring-up.
type Peer struct {
	addr string
	conn *rpc.ClientConn
}

// DialPeer creates a lazy client connection to the Raft node at addr
// ("host:port").
func DialPeer(addr string) (*Peer, error) {
	conn, err := rpc.Dial(addr, rpc.WithTimeout(PeerRPCTimeout))
	if err != nil {
		return nil, err
	}
	return &Peer{addr: addr, conn: conn}, nil
}

// Addr returns the "host:port" of the remote node.
func (p *Peer) Addr() string { return p.addr }

// RequestVote invokes the remote RequestVote handler. The boolean is
// false when the RPC itself failed (transport error or timeout); the
// reply content then must not be trusted.
func (p *Peer) RequestVote(ctx context.Context, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	reply := &RequestVoteReply{}
	if err := p.conn.Invoke(ctx, ServiceName, "RequestVote", args, reply); err != nil {
		return nil, false
	}
	return reply, true
}

// AppendEntries invokes the remote AppendEntries handler.
func (p *Peer) AppendEntries(ctx context.Context, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	reply := &AppendEntriesReply{}
	if err := p.conn.Invoke(ctx, ServiceName, "AppendEntries", args, reply); err != nil {
		return nil, false
	}
	return reply, true
}

// InstallSnapshot invokes the remote InstallSnapshot handler.
func (p *Peer) InstallSnapshot(ctx context.Context, args *InstallSnapshotRequest) (*InstallSnapshotResponse, bool) {
	reply := &InstallSnapshotResponse{}
	if err := p.conn.Invoke(ctx, ServiceName, "InstallSnapshot", args, reply); err != nil {
		return nil, false
	}
	return reply, true
}

// Close releases the underlying connection pool.
func (p *Peer) Close() error { return p.conn.Close() }

// Service exposes exactly the Raft RPC surface (RequestVote,
// AppendEntries and InstallSnapshot) for registration with a yukino_rpc
// server. Keeping it separate from Raft means administrative methods such
// as Start, Snapshot or Kill are not reachable from the network.
type Service struct {
	rf *Raft
}

// Service returns the network-facing wrapper for this node.
func (rf *Raft) Service() *Service { return &Service{rf: rf} }

// RequestVote handles a remote vote request.
func (s *Service) RequestVote(ctx context.Context, args *RequestVoteArgs) (*RequestVoteReply, error) {
	reply := &RequestVoteReply{}
	s.rf.handleRequestVote(args, reply)
	return reply, nil
}

// AppendEntries handles a remote log-replication request or heartbeat.
func (s *Service) AppendEntries(ctx context.Context, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	reply := &AppendEntriesReply{}
	s.rf.handleAppendEntries(args, reply)
	return reply, nil
}

// InstallSnapshot handles a remote snapshot installation.
func (s *Service) InstallSnapshot(ctx context.Context, args *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	reply := &InstallSnapshotResponse{}
	s.rf.handleInstallSnapshot(args, reply)
	return reply, nil
}
