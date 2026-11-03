package kvserver

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/hangtiancheng/yukino.go/libs/yukino_rpc/pkg/rpc"
	"github.com/yukino-js/shita.go/yukino-kv/raft"
)

// NodeConfig describes one cluster member.
type NodeConfig struct {
	// ID is this node's index in Addrs.
	ID int
	// Addrs lists the "host:port" of every cluster member, ordered by
	// node id. All nodes must use the same ordering.
	Addrs []string
	// MaxRaftState is the persisted-Raft-state size (bytes) above which
	// a snapshot is taken. -1 disables snapshotting.
	MaxRaftState int
	// DataDir is where the persister keeps its files.
	DataDir string
	// Logger is optional; slog.Default() is used when nil.
	Logger *slog.Logger
}

// Node wires together the RPC server, the Raft node and the KV state
// machine — the Go equivalent of the C++ KvServer constructor plus the
// RpcProvider bootstrap. Unlike the C++ constructor it needs no startup
// sleeps: yukino_rpc dials lazily, so peers may be contacted before they
// listen and the usual Raft retries bridge the gap.
type Node struct {
	cfg       NodeConfig
	listener  net.Listener
	rpcServer *rpc.Server
	raft      *raft.Raft
	kv        *KVServer
	peers     []*raft.Peer
	applyChan chan raft.ApplyMsg
	logger    *slog.Logger
}

// NewNode builds the full node: persister, peer stubs, Raft, KV state
// machine and the yukino_rpc server exposing both services. The node does
// not accept connections until Serve is called.
func NewNode(cfg NodeConfig) (*Node, error) {
	if cfg.ID < 0 || cfg.ID >= len(cfg.Addrs) {
		return nil, fmt.Errorf("kvserver: node id %d out of range for %d addresses", cfg.ID, len(cfg.Addrs))
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("node", cfg.ID)

	persister, err := raft.NewPersister(cfg.DataDir, cfg.ID)
	if err != nil {
		return nil, err
	}

	peers := make([]*raft.Peer, len(cfg.Addrs))
	closePeers := func() {
		for _, p := range peers {
			if p != nil {
				p.Close()
			}
		}
	}
	for i, addr := range cfg.Addrs {
		if i == cfg.ID {
			continue
		}
		peer, err := raft.DialPeer(addr)
		if err != nil {
			closePeers()
			return nil, fmt.Errorf("kvserver: dial peer %d (%s): %w", i, addr, err)
		}
		peers[i] = peer
	}

	applyChan := make(chan raft.ApplyMsg, 256)
	rf, err := raft.NewRaft(peers, cfg.ID, persister, applyChan, logger)
	if err != nil {
		closePeers()
		return nil, err
	}
	kv := NewKVServer(cfg.ID, cfg.MaxRaftState, rf, persister, applyChan, logger)

	rpcServer := rpc.NewServer()
	rpcServer.Register(raft.ServiceName, rf.Service())
	rpcServer.Register(ServiceName, kv)

	listener, err := net.Listen("tcp", cfg.Addrs[cfg.ID])
	if err != nil {
		rf.Kill()
		kv.Stop()
		closePeers()
		return nil, fmt.Errorf("kvserver: listen on %s: %w", cfg.Addrs[cfg.ID], err)
	}

	logger.Info("kv node ready", "addr", cfg.Addrs[cfg.ID], "clusterSize", len(cfg.Addrs))
	return &Node{
		cfg:       cfg,
		listener:  listener,
		rpcServer: rpcServer,
		raft:      rf,
		kv:        kv,
		peers:     peers,
		applyChan: applyChan,
		logger:    logger,
	}, nil
}

// Serve accepts RPC connections until Shutdown. It returns nil after a
// graceful stop.
func (n *Node) Serve() error {
	return n.rpcServer.Serve(n.listener)
}

// Shutdown stops the node gracefully: the apply loop and Raft goroutines
// exit first, then the RPC server drains in-flight handlers (bounded by
// the consensus timeout) and the peer connections close.
func (n *Node) Shutdown() {
	n.logger.Info("shutting down node")
	n.kv.Stop()
	n.raft.Kill()
	n.rpcServer.GracefulStop()
	for _, p := range n.peers {
		if p != nil {
			p.Close()
		}
	}
}

// Raft exposes the underlying Raft node for monitoring and tests.
func (n *Node) Raft() *raft.Raft { return n.raft }

// KV exposes the underlying state machine for monitoring and tests.
func (n *Node) KV() *KVServer { return n.kv }

// Addr returns the local address the node listens on.
func (n *Node) Addr() string { return n.listener.Addr().String() }
