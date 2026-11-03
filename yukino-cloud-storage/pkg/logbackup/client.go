package logbackup

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hangtiancheng/yukino.go/libs/yukino_rpc/pkg/rpc"
)

// ClientOptions configures a backup sender.
type ClientOptions struct {
	// Addr is the receiver address in host:port form.
	Addr string
	// Source identifies this process to the receiver.
	Source string
	// QueueSize bounds the number of pending records. When the queue is full,
	// new records are dropped rather than blocking the caller.
	QueueSize int
	// BatchSize is the maximum number of records sent per RPC.
	BatchSize int
	// Timeout bounds each RPC.
	Timeout time.Duration
	// RetryBackoff is the base delay after a failed send before retrying.
	RetryBackoff time.Duration
	// Logf, when non-nil, receives diagnostic messages. It must not itself log
	// through the logger that owns this client, or it would recurse.
	Logf func(format string, args ...any)
}

// Client is an asynchronous, non-blocking log backup sender. Its method set
// satisfies the asynclog.BackupSink interface.
type Client struct {
	opts  ClientOptions
	queue chan string

	conn    atomic.Pointer[rpc.ClientConn]
	dropped atomic.Uint64
	sent    atomic.Uint64

	wg       sync.WaitGroup
	stop     chan struct{}
	stopOnce sync.Once
}

// NewClient starts a sender goroutine and returns the client.
func NewClient(opts ClientOptions) *Client {
	if opts.QueueSize <= 0 {
		opts.QueueSize = 4096
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 64
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 3 * time.Second
	}
	if opts.RetryBackoff <= 0 {
		opts.RetryBackoff = 200 * time.Millisecond
	}
	c := &Client{
		opts:  opts,
		queue: make(chan string, opts.QueueSize),
		stop:  make(chan struct{}),
	}
	c.wg.Add(1)
	go c.run()
	return c
}

// Backup enqueues a record without ever blocking. If the queue is full the
// record is dropped and counted. This satisfies asynclog.BackupSink.
func (c *Client) Backup(record string) {
	select {
	case c.queue <- record:
	default:
		c.dropped.Add(1)
	}
}

// Dropped returns the number of records discarded because the queue was full or
// delivery kept failing.
func (c *Client) Dropped() uint64 { return c.dropped.Load() }

// Sent returns the number of records successfully delivered.
func (c *Client) Sent() uint64 { return c.sent.Load() }

// Close stops the sender, drains queued records on a best-effort basis and
// closes the RPC connection. It is idempotent.
func (c *Client) Close() error {
	c.stopOnce.Do(func() { close(c.stop) })
	c.wg.Wait()
	if conn := c.conn.Swap(nil); conn != nil {
		return conn.Close()
	}
	return nil
}

func (c *Client) run() {
	defer c.wg.Done()
	for {
		select {
		case <-c.stop:
			c.drainRemaining()
			return
		case rec := <-c.queue:
			c.sendBatch(c.collect(rec))
		}
	}
}

// collect gathers up to BatchSize records, starting with first, without
// blocking once the queue is momentarily empty.
func (c *Client) collect(first string) []string {
	batch := make([]string, 0, c.opts.BatchSize)
	batch = append(batch, first)
	for len(batch) < c.opts.BatchSize {
		select {
		case rec := <-c.queue:
			batch = append(batch, rec)
		default:
			return batch
		}
	}
	return batch
}

// sendBatch delivers a batch, retrying a bounded number of times before giving
// up and counting the batch as dropped.
func (c *Client) sendBatch(batch []string) {
	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), c.opts.Timeout)
		err := c.invoke(ctx, batch)
		cancel()
		if err == nil {
			c.sent.Add(uint64(len(batch)))
			return
		}
		if c.opts.Logf != nil {
			c.opts.Logf("logbackup: send attempt %d/%d failed: %v", attempt, maxAttempts, err)
		}
		select {
		case <-c.stop:
			c.dropped.Add(uint64(len(batch)))
			return
		case <-time.After(c.opts.RetryBackoff * time.Duration(attempt)):
		}
	}
	c.dropped.Add(uint64(len(batch)))
}

func (c *Client) invoke(ctx context.Context, batch []string) error {
	conn := c.conn.Load()
	if conn == nil {
		dialed, err := rpc.Dial(c.opts.Addr,
			rpc.WithTimeout(c.opts.Timeout),
			rpc.WithDialCodec(rpc.CodecJSON),
		)
		if err != nil {
			return err
		}
		c.conn.Store(dialed)
		conn = dialed
	}
	var reply AppendReply
	req := &AppendRequest{Source: c.opts.Source, Records: batch}
	return conn.Invoke(ctx, ServiceName, AppendMethod, req, &reply)
}

// drainRemaining flushes whatever is left in the queue when the client stops.
func (c *Client) drainRemaining() {
	var batch []string
	for {
		select {
		case rec := <-c.queue:
			batch = append(batch, rec)
			if len(batch) >= c.opts.BatchSize {
				c.sendBatch(batch)
				batch = batch[:0]
			}
		default:
			if len(batch) > 0 {
				c.sendBatch(batch)
			}
			return
		}
	}
}
