// Package test contains end-to-end tests that boot real yukino-kv
// clusters (Raft + KV state machine + yukino_rpc transport) on loopback
// and drive them through a clerk, covering basic operations, leader
// failover, snapshotting and crash-restart recovery.
package test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/yukino-js/shita.go/yukino-kv/clerk"
	"github.com/yukino-js/shita.go/yukino-kv/kvserver"
)

const (
	testTimeout     = 30 * time.Second
	waitLeaderStep  = 25 * time.Millisecond
	waitLeaderLimit = 10 * time.Second
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// cluster is an in-process yukino-kv cluster.
type cluster struct {
	t     *testing.T
	nodes []*kvserver.Node
	addrs []string
	dir   string
	max   int
}

// freeAddrs reserves n loopback ports and releases them immediately; the
// nodes bind them right afterwards.
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

// startCluster boots n nodes sharing dir for persistence.
func startCluster(t *testing.T, n int, dir string, maxRaftState int) *cluster {
	t.Helper()
	c := &cluster{t: t, dir: dir, max: maxRaftState}
	c.addrs = freeAddrs(t, n)
	c.nodes = make([]*kvserver.Node, n)
	for i := 0; i < n; i++ {
		c.startNode(i)
	}
	t.Cleanup(c.shutdownAll)
	return c
}

func (c *cluster) startNode(i int) {
	c.t.Helper()
	node, err := kvserver.NewNode(kvserver.NodeConfig{
		ID:           i,
		Addrs:        c.addrs,
		MaxRaftState: c.max,
		DataDir:      c.dir,
		Logger:       discardLogger(),
	})
	if err != nil {
		c.t.Fatalf("start node %d: %v", i, err)
	}
	go node.Serve()
	c.nodes[i] = node
}

func (c *cluster) shutdownAll() {
	for i, n := range c.nodes {
		if n != nil {
			n.Shutdown()
			c.nodes[i] = nil
		}
	}
}

func (c *cluster) shutdownNode(i int) {
	c.t.Helper()
	if c.nodes[i] == nil {
		c.t.Fatalf("node %d already down", i)
	}
	c.nodes[i].Shutdown()
	c.nodes[i] = nil
}

// leaderId returns the id of the unique leader, waiting for an election
// to settle if necessary.
func (c *cluster) leaderId(t *testing.T) int {
	t.Helper()
	deadline := time.Now().Add(waitLeaderLimit)
	for time.Now().Before(deadline) {
		leader := -1
		for i, n := range c.nodes {
			if n == nil {
				continue
			}
			if _, isLeader := n.Raft().GetState(); isLeader {
				if leader != -1 {
					t.Fatalf("multiple leaders: %d and %d", leader, i)
				}
				leader = i
			}
		}
		if leader != -1 {
			return leader
		}
		time.Sleep(waitLeaderStep)
	}
	t.Fatal("no leader elected in time")
	return -1
}

func (c *cluster) newClerk(t *testing.T) *clerk.Clerk {
	t.Helper()
	client, err := clerk.NewClerk(c.addrs, discardLogger())
	if err != nil {
		t.Fatalf("NewClerk: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func TestBasicPutGetAppend(t *testing.T) {
	c := startCluster(t, 3, t.TempDir(), -1)
	_ = c.leaderId(t)
	client := c.newClerk(t)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if err := client.Put(ctx, "x", "1"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	value, err := client.Get(ctx, "x")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if value != "1" {
		t.Fatalf("Get(x) = %q, want %q", value, "1")
	}

	// Append concatenates (the C++ port silently overwrote instead).
	if err := client.Append(ctx, "x", "2"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	value, err = client.Get(ctx, "x")
	if err != nil {
		t.Fatalf("Get after Append: %v", err)
	}
	if value != "12" {
		t.Fatalf("Get(x) after Append = %q, want %q", value, "12")
	}

	// Missing keys report ErrNoKey.
	if _, err := client.Get(ctx, "missing"); !errors.Is(err, clerk.ErrNoKey) {
		t.Fatalf("Get(missing) err = %v, want ErrNoKey", err)
	}

	// A batch of keys survives.
	for i := 0; i < 10; i++ {
		if err := client.Put(ctx, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("Put k%d: %v", i, err)
		}
	}
	for i := 0; i < 10; i++ {
		got, err := client.Get(ctx, fmt.Sprintf("k%d", i))
		if err != nil {
			t.Fatalf("Get k%d: %v", i, err)
		}
		if got != fmt.Sprintf("v%d", i) {
			t.Fatalf("Get(k%d) = %q, want %q", i, got, fmt.Sprintf("v%d", i))
		}
	}
}

func TestLeaderFailover(t *testing.T) {
	c := startCluster(t, 3, t.TempDir(), -1)
	leader := c.leaderId(t)
	client := c.newClerk(t)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if err := client.Put(ctx, "survivor", "before"); err != nil {
		t.Fatalf("Put before failover: %v", err)
	}

	// Kill the leader; the two remaining nodes must elect a new one and
	// keep serving. The clerk retries transparently.
	c.shutdownNode(leader)
	newLeader := c.leaderId(t)
	if newLeader == leader {
		t.Fatalf("node %d still leader after shutdown", leader)
	}

	if err := client.Put(ctx, "survivor", "after"); err != nil {
		t.Fatalf("Put after failover: %v", err)
	}
	value, err := client.Get(ctx, "survivor")
	if err != nil {
		t.Fatalf("Get after failover: %v", err)
	}
	if value != "after" {
		t.Fatalf("Get(survivor) = %q, want %q", value, "after")
	}
}

func TestSnapshotAndFullRestartRecovery(t *testing.T) {
	// A small maxRaftState forces frequent snapshots while the cluster
	// runs; after a full crash-restart cycle every committed key must
	// still be readable. This exercises exactly the path the C++
	// persister bug (truncate-on-startup) broke.
	dir := t.TempDir()
	c := startCluster(t, 3, dir, 300)
	_ = c.leaderId(t)
	client := c.newClerk(t)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	const keys = 30
	for i := 0; i < keys; i++ {
		if err := client.Put(ctx, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)); err != nil {
			t.Fatalf("Put key%d: %v", i, err)
		}
	}

	// Snapshotting must have kicked in: the persisted Raft state stays
	// bounded instead of growing with every operation.
	for i, n := range c.nodes {
		size := n.Raft().GetRaftStateSize()
		if size > 300*2 {
			t.Errorf("node %d raft state size %d grew far beyond the 300-byte snapshot threshold", i, size)
		}
	}

	// Crash the whole cluster and bring it back from disk.
	c.shutdownAll()
	for i := range c.nodes {
		c.startNode(i)
	}
	_ = c.leaderId(t)
	client2 := c.newClerk(t)

	for i := 0; i < keys; i++ {
		got, err := client2.Get(ctx, fmt.Sprintf("key%d", i))
		if err != nil {
			t.Fatalf("Get key%d after restart: %v", i, err)
		}
		if got != fmt.Sprintf("value%d", i) {
			t.Fatalf("after restart Get(key%d) = %q, want %q", i, got, fmt.Sprintf("value%d", i))
		}
	}

	// The restarted cluster still accepts writes.
	if err := client2.Put(ctx, "post-restart", "ok"); err != nil {
		t.Fatalf("Put after restart: %v", err)
	}
	if got, err := client2.Get(ctx, "post-restart"); err != nil || got != "ok" {
		t.Fatalf("Get(post-restart) = %q, %v; want ok, nil", got, err)
	}
}

func TestFollowerRestartCatchesUp(t *testing.T) {
	dir := t.TempDir()
	c := startCluster(t, 3, dir, 300)
	leader := c.leaderId(t)
	client := c.newClerk(t)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	for i := 0; i < 10; i++ {
		if err := client.Put(ctx, fmt.Sprintf("a%d", i), fmt.Sprintf("x%d", i)); err != nil {
			t.Fatalf("Put a%d: %v", i, err)
		}
	}

	// Restart one follower; it must catch up through log replication or
	// a snapshot and the cluster must stay writable.
	follower := (leader + 1) % 3
	c.shutdownNode(follower)
	for i := 10; i < 20; i++ {
		if err := client.Put(ctx, fmt.Sprintf("a%d", i), fmt.Sprintf("x%d", i)); err != nil {
			t.Fatalf("Put a%d while follower down: %v", i, err)
		}
	}
	c.startNode(follower)

	// Wait until the restarted node has applied everything the cluster
	// has committed.
	deadline := time.Now().Add(waitLeaderLimit)
	for time.Now().Before(deadline) {
		p := c.nodes[follower].Raft().Progress()
		if leaderP := c.nodes[c.leaderId(t)].Raft().Progress(); p.LastApplied >= leaderP.CommitIndex && leaderP.CommitIndex >= 20 {
			break
		}
		time.Sleep(waitLeaderStep)
	}
	p := c.nodes[follower].Raft().Progress()
	if p.LastApplied < 20 {
		t.Fatalf("restarted follower only applied up to %d, want >= 20", p.LastApplied)
	}
}
