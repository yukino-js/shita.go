package raft_test

import (
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/hangtiancheng/yukino.go/libs/yukino_rpc/pkg/rpc"
	"github.com/yukino-js/shita.go/yukino-kv/raft"
)

// raftTestNode bundles a Raft instance with the RPC server that exposes it.
type raftTestNode struct {
	rf       *raft.Raft
	server   *rpc.Server
	listener net.Listener
	peers    []*raft.Peer
	draining chan struct{}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// freeAddrs reserves n loopback ports and releases them again; callers
// must bind them immediately afterwards.
func freeAddrs(t *testing.T, n int) []string {
	t.Helper()
	listeners := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve port: %v", err)
		}
		listeners[i] = ln
		addrs[i] = ln.Addr().String()
	}
	for _, ln := range listeners {
		ln.Close()
	}
	return addrs
}

func startRaftNode(t *testing.T, addrs []string, id int, dir string) *raftTestNode {
	t.Helper()
	persister, err := raft.NewPersister(dir, id)
	if err != nil {
		t.Fatalf("persister: %v", err)
	}
	peers := make([]*raft.Peer, len(addrs))
	for i, addr := range addrs {
		if i == id {
			continue
		}
		peer, err := raft.DialPeer(addr)
		if err != nil {
			t.Fatalf("dial peer %d: %v", i, err)
		}
		peers[i] = peer
	}
	applyChan := make(chan raft.ApplyMsg, 64)
	draining := make(chan struct{})
	go func() {
		for {
			select {
			case <-applyChan:
			case <-draining:
				return
			}
		}
	}()
	rf, err := raft.NewRaft(peers, id, persister, applyChan, discardLogger())
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	server := rpc.NewServer()
	server.Register(raft.ServiceName, rf.Service())
	listener, err := net.Listen("tcp", addrs[id])
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go server.Serve(listener)
	return &raftTestNode{
		rf:       rf,
		server:   server,
		listener: listener,
		peers:    peers,
		draining: draining,
	}
}

func (n *raftTestNode) shutdown() {
	n.rf.Kill()
	n.server.GracefulStop()
	close(n.draining)
	for _, p := range n.peers {
		if p != nil {
			p.Close()
		}
	}
}

// leaders returns the ids of nodes that currently believe they are the
// leader, along with their terms.
func leaders(nodes []*raftTestNode) []int {
	var ids []int
	for i, n := range nodes {
		if n == nil {
			continue
		}
		if _, isLeader := n.rf.GetState(); isLeader {
			ids = append(ids, i)
		}
	}
	return ids
}

// waitForLeader polls until exactly one leader exists or the deadline
// passes.
func waitForLeader(t *testing.T, nodes []*raftTestNode, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ids := leaders(nodes)
		if len(ids) == 1 {
			return ids[0]
		}
		if len(ids) > 1 {
			t.Fatalf("multiple leaders elected: %v", ids)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %v", timeout)
	return -1
}

func TestLeaderElection(t *testing.T) {
	addrs := freeAddrs(t, 3)
	dir := t.TempDir()
	nodes := make([]*raftTestNode, 3)
	for i := range nodes {
		nodes[i] = startRaftNode(t, addrs, i, dir)
	}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.shutdown()
			}
		}
	}()

	leader := waitForLeader(t, nodes, 5*time.Second)
	t.Logf("initial leader: node %d", leader)

	// Kill the leader; one of the remaining nodes must take over.
	nodes[leader].shutdown()
	nodes[leader] = nil
	newLeader := waitForLeader(t, nodes, 5*time.Second)
	if newLeader == leader {
		t.Fatalf("old leader %d still reported as leader after shutdown", leader)
	}
	t.Logf("new leader after failover: node %d", newLeader)

	// The new leader's term must be at least as high as the old one's.
	if term, _ := nodes[newLeader].rf.GetState(); term < 2 {
		t.Errorf("new leader term = %d, want >= 2", term)
	}
}

func TestElectionWithSlowStart(t *testing.T) {
	// Peers come up staggered, as in a real deployment; the early nodes
	// must keep retrying elections until the cluster is complete.
	addrs := freeAddrs(t, 3)
	dir := t.TempDir()
	nodes := make([]*raftTestNode, 3)
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.shutdown()
			}
		}
	}()

	nodes[0] = startRaftNode(t, addrs, 0, dir)
	time.Sleep(150 * time.Millisecond)
	nodes[1] = startRaftNode(t, addrs, 1, dir)
	time.Sleep(150 * time.Millisecond)
	nodes[2] = startRaftNode(t, addrs, 2, dir)

	leader := waitForLeader(t, nodes, 5*time.Second)
	t.Logf("leader with staggered start: node %d", leader)
}
