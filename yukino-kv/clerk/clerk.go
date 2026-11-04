// Package clerk implements the yukino-kv client: a clerk remembers the last
// server that acted as leader, retries failed requests on the next server
// in ring order, and keeps one request id per logical operation so the KV
// layer can deduplicate retries and preserve linearizability.
package clerk

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hangtiancheng/yukino.go/libs/yukino_rpc/pkg/rpc"
	"github.com/yukino-js/shita.go/yukino-kv/kvserver"
)

// ErrNoKey is returned by Get when the key does not exist in the cluster.
var ErrNoKey = errors.New("clerk: key does not exist")

const (
	// retryDelay paces leader probing while no server can serve the
	// request. FIX: the C++ clerk spun through the server ring without
	// any delay, burning CPU during leader elections.
	retryDelay = 50 * time.Millisecond
	// requestTimeout bounds a single Get/PutAppend RPC. It must exceed
	// the server-side consensus timeout so a slow commit is not cut off
	// by the transport.
	requestTimeout = 3 * time.Second
)

// Clerk is a KV client. All exported methods are safe for concurrent use.
type Clerk struct {
	mu             sync.Mutex
	servers        []*rpc.ClientConn
	clientId       string
	requestId      int
	recentLeaderId int
	logger         *slog.Logger
}

// NewClerk creates a client for the KV servers at addrs ("host:port").
// Connections are lazy: servers may still be starting up.
//
// The client id comes from crypto/rand. FIX: the C++ clerk built its id
// from four unseeded rand() calls, so every run — and every process on a
// machine — produced the same "unique" id, which could make independent
// clients deduplicate each other's requests.
func NewClerk(addrs []string, logger *slog.Logger) (*Clerk, error) {
	if len(addrs) == 0 {
		return nil, errors.New("clerk: no server addresses")
	}
	if logger == nil {
		logger = slog.Default()
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return nil, fmt.Errorf("clerk: generate client id: %w", err)
	}
	conns := make([]*rpc.ClientConn, 0, len(addrs))
	for _, addr := range addrs {
		conn, err := rpc.Dial(addr, rpc.WithTimeout(requestTimeout))
		if err != nil {
			for _, c := range conns {
				c.Close()
			}
			return nil, fmt.Errorf("clerk: dial %s: %w", addr, err)
		}
		conns = append(conns, conn)
	}
	return &Clerk{
		servers:        conns,
		clientId:       hex.EncodeToString(id),
		recentLeaderId: 0,
		logger:         logger.With("component", "clerk"),
	}, nil
}

// ClientId returns the stable identity this clerk presents to servers.
func (c *Clerk) ClientId() string { return c.clientId }

// Close releases all client connections.
func (c *Clerk) Close() error {
	var firstErr error
	for _, conn := range c.servers {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Get returns the value of key, or ErrNoKey if it does not exist. It
// retries across servers until the cluster answers, so it only returns an
// error for a missing key, a cancelled context or a protocol-level
// failure.
func (c *Clerk) Get(ctx context.Context, key string) (string, error) {
	requestId := c.nextRequestId()
	server := c.leaderHint()
	for {
		args := &kvserver.GetArgs{Key: key, ClientId: c.clientId, RequestId: requestId}
		var reply kvserver.GetReply
		err := c.servers[server].Invoke(ctx, kvserver.ServiceName, "Get", args, &reply)
		if err == nil {
			switch reply.Err {
			case kvserver.OK:
				c.noteLeader(server)
				return reply.Value, nil
			case kvserver.ErrNoKey:
				c.noteLeader(server)
				return "", ErrNoKey
			case kvserver.ErrWrongLeader:
				// Probe the next server. The request id stays the same
				// across retries so the KV layer can deduplicate.
			default:
				return "", fmt.Errorf("clerk: server %d returned unknown error %q", server, reply.Err)
			}
		} else {
			c.logger.Debug("Get RPC failed", "server", server, "key", key, "err", err)
		}
		server = (server + 1) % len(c.servers)
		if !sleepCtx(ctx, retryDelay) {
			return "", ctx.Err()
		}
	}
}

// Put sets key to value, retrying across servers until the cluster
// acknowledges the write.
func (c *Clerk) Put(ctx context.Context, key, value string) error {
	return c.PutAppend(ctx, key, value, kvserver.OpPut)
}

// Append adds value to the end of key's current value, retrying across
// servers until the cluster acknowledges the write.
func (c *Clerk) Append(ctx context.Context, key, value string) error {
	return c.PutAppend(ctx, key, value, kvserver.OpAppend)
}

// PutAppend is the shared implementation of Put and Append.
func (c *Clerk) PutAppend(ctx context.Context, key, value, op string) error {
	requestId := c.nextRequestId()
	server := c.leaderHint()
	for {
		args := &kvserver.PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			ClientId:  c.clientId,
			RequestId: requestId,
		}
		var reply kvserver.PutAppendReply
		err := c.servers[server].Invoke(ctx, kvserver.ServiceName, "PutAppend", args, &reply)
		if err == nil {
			switch reply.Err {
			case kvserver.OK:
				c.noteLeader(server)
				return nil
			case kvserver.ErrWrongLeader:
				c.logger.Debug("server is not the leader; retrying on next server",
					"server", server, "next", (server+1)%len(c.servers), "op", op)
			default:
				return fmt.Errorf("clerk: server %d returned unknown error %q", server, reply.Err)
			}
		} else {
			c.logger.Debug("PutAppend RPC failed", "server", server, "op", op, "key", key, "err", err)
		}
		server = (server + 1) % len(c.servers)
		if !sleepCtx(ctx, retryDelay) {
			return ctx.Err()
		}
	}
}

func (c *Clerk) nextRequestId() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requestId++
	return c.requestId
}

func (c *Clerk) leaderHint() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recentLeaderId
}

func (c *Clerk) noteLeader(server int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recentLeaderId = server
}

// sleepCtx waits for d, returning false early if ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
