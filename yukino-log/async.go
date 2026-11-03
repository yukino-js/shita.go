package yukino_log

import (
	"fmt"
	"sync"
	"time"
)

const (
	// DefaultBufferSize is the async staging buffer. It matches the C++
	// kLargeBufferSize of 4,000,000 bytes. A single record larger than this
	// is queued whole instead of being discarded.
	DefaultBufferSize = 4_000_000
)

// AsyncOptions configures an [Async] backend.
type AsyncOptions struct {
	// Basename is the path prefix passed to [Open]. It is not a directory.
	Basename string
	// RollSize is the size in bytes past which the current file rolls.
	RollSize int64
	// BufferSize is the staging buffer capacity. Zero selects
	// [DefaultBufferSize]. A record larger than this is still kept.
	BufferSize int
	// FlushInterval is how long a partly filled buffer may wait before the
	// backend writes it. Zero selects [DefaultFlushInterval].
	FlushInterval time.Duration
	// File is passed to [Open]. Its zero values select that function's defaults.
	File FileOptions
}

// Async accepts records from any goroutine and writes them from one backend
// goroutine. Start must return before Append, and Stop must be called to
// flush and close the file. A nil or empty append is a no-op.
type Async struct {
	mu        sync.Mutex
	dataWait  *sync.Cond
	flushWait *sync.Cond

	basename   string
	rollSize   int64
	bufferSize int
	flushEvery time.Duration
	fileOpts   FileOptions

	current *chunk
	spare1  *chunk
	spare2  *chunk
	pending []*chunk

	file    *LogFile
	started bool
	stop    bool
	epoch   uint64
	acked   uint64
	err     error
	done    chan struct{}
}

// NewAsync validates options and allocates the two staging buffers. It does
// not create a file; [Async.Start] does.
func NewAsync(opts AsyncOptions) (*Async, error) {
	if err := checkBasename(opts.Basename); err != nil {
		return nil, err
	}
	if opts.RollSize <= 0 {
		return nil, ErrRollSize
	}
	if opts.BufferSize < 0 {
		return nil, ErrBufferSize
	}
	if opts.BufferSize == 0 {
		opts.BufferSize = DefaultBufferSize
	}
	flush, err := positiveDuration(opts.FlushInterval, DefaultFlushInterval)
	if err != nil {
		return nil, err
	}
	a := &Async{
		basename:   opts.Basename,
		rollSize:   opts.RollSize,
		bufferSize: opts.BufferSize,
		flushEvery: flush,
		fileOpts:   opts.File,
		current:    newChunk(opts.BufferSize),
		spare1:     newChunk(opts.BufferSize),
		done:       make(chan struct{}),
	}
	a.dataWait = sync.NewCond(&a.mu)
	a.flushWait = sync.NewCond(&a.mu)
	return a, nil
}

// Start opens the log file and starts the backend goroutine. A second call
// while the backend is running returns nil. After [Async.Stop], Start returns
// [ErrClosed].
func (a *Async) Start() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.stop {
		return ErrClosed
	}
	if a.started {
		return nil
	}
	file, err := Open(a.basename, a.rollSize, a.fileOpts)
	if err != nil {
		return err
	}
	a.file = file
	a.started = true
	go a.loop()
	return nil
}

// Append copies p into the staging buffers. It returns [ErrNotStarted] before
// [Async.Start] and [ErrClosed] after [Async.Stop]. A successful return means
// the bytes will be written by the time Stop returns, or that Flush has
// already observed them.
func (a *Async) Append(p []byte) error {
	if len(p) == 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.stop {
		return ErrClosed
	}
	if !a.started {
		return ErrNotStarted
	}
	a.ingestLocked(p)
	return nil
}

// Write implements [io.Writer].
func (a *Async) Write(p []byte) (int, error) {
	if err := a.Append(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Flush makes the backend write every byte accepted before this call and
// waits for that write to finish. Bytes accepted while Flush is waiting are
// not included; call Flush again for those.
func (a *Async) Flush() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.started {
		return ErrNotStarted
	}
	if a.stop {
		return a.err
	}
	a.epoch++
	want := a.epoch
	a.dataWait.Signal()
	for a.acked < want {
		a.flushWait.Wait()
	}
	return a.err
}

// Stop tells the backend to write every accepted byte, close the file, and
// exit, then waits for it to do so. A second call returns the same error as
// the first. Stop before Start returns [ErrNotStarted].
func (a *Async) Stop() error {
	a.mu.Lock()
	if !a.started {
		a.mu.Unlock()
		return ErrNotStarted
	}
	if a.stop {
		err := a.err
		a.mu.Unlock()
		return err
	}
	a.stop = true
	a.dataWait.Signal()
	a.mu.Unlock()

	<-a.done

	a.mu.Lock()
	err := a.err
	a.mu.Unlock()
	return err
}

// ingestLocked queues p. A record that fits in the free space of the current
// buffer is copied there. A record that fills that buffer is handed to the
// backend immediately. A record larger than a whole buffer is queued by
// itself so it is not dropped and is not split.
func (a *Async) ingestLocked(p []byte) {
	if a.current.avail() >= len(p) {
		a.current.write(p)
		if a.current.avail() == 0 {
			a.handOffLocked()
		}
		return
	}
	if len(a.current.b) > 0 {
		a.handOffLocked()
		if a.current.avail() >= len(p) {
			a.current.write(p)
			if a.current.avail() == 0 {
				a.handOffLocked()
			}
			return
		}
	}
	owned := &chunk{b: append([]byte(nil), p...)}
	a.pending = append(a.pending, owned)
	a.dataWait.Signal()
}

func (a *Async) handOffLocked() {
	a.pending = append(a.pending, a.current)
	a.current = a.takeSpareLocked()
	a.dataWait.Signal()
}

func (a *Async) takeSpareLocked() *chunk {
	if a.spare1 != nil {
		c := a.spare1
		a.spare1 = nil
		c.reset()
		return c
	}
	if a.spare2 != nil {
		c := a.spare2
		a.spare2 = nil
		c.reset()
		return c
	}
	return newChunk(a.bufferSize)
}

func (a *Async) loop() {
	defer close(a.done)
	defer func() {
		if recovered := recover(); recovered != nil {
			a.noteErr(errPanic(recovered))
		}
	}()
	defer a.finish()

	a.mu.Lock()
	file := a.file
	a.mu.Unlock()

	for {
		batch, epoch, stopping := a.detach()
		if len(batch) > 0 {
			a.writeBatch(file, batch)
			a.recycle(batch)
		}
		a.ack(epoch)
		if stopping && !a.pendingBytes() {
			return
		}
	}
}

// detach waits until there is data, a flush request, or a stop, then moves
// the current buffer onto the returned batch. The caller writes the batch
// without holding a.mu.
func (a *Async) detach() (batch []*chunk, epoch uint64, stopping bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for !a.stop && len(a.pending) == 0 && len(a.current.b) == 0 && a.epoch == a.acked {
		a.waitIntervalLocked()
	}
	if len(a.current.b) > 0 {
		a.pending = append(a.pending, a.current)
		a.current = a.takeSpareLocked()
	}
	batch = a.pending
	a.pending = nil
	return batch, a.epoch, a.stop
}

// waitIntervalLocked waits for a signal or for flushEvery to elapse.
// The caller holds a.mu. AfterFunc cannot acquire that lock until Wait
// releases it, so the timeout cannot signal before anyone is waiting.
func (a *Async) waitIntervalLocked() {
	timer := time.AfterFunc(a.flushEvery, func() {
		a.mu.Lock()
		a.dataWait.Signal()
		a.mu.Unlock()
	})
	a.dataWait.Wait()
	timer.Stop()
}

func (a *Async) writeBatch(file *LogFile, batch []*chunk) {
	for _, c := range batch {
		if len(c.b) == 0 {
			continue
		}
		if err := file.Append(c.b); err != nil {
			a.noteErr(err)
		}
	}
	if err := file.Flush(); err != nil {
		a.noteErr(err)
	}
}

func (a *Async) recycle(batch []*chunk) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, c := range batch {
		if cap(c.b) != a.bufferSize {
			continue
		}
		c.reset()
		switch {
		case a.spare1 == nil:
			a.spare1 = c
		case a.spare2 == nil:
			a.spare2 = c
		}
	}
}

func (a *Async) ack(epoch uint64) {
	a.mu.Lock()
	if epoch > a.acked {
		a.acked = epoch
	}
	a.flushWait.Broadcast()
	a.mu.Unlock()
}

func (a *Async) pendingBytes() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.pending) > 0 || len(a.current.b) > 0
}

func (a *Async) noteErr(err error) {
	if err == nil {
		return
	}
	a.mu.Lock()
	first := a.err == nil
	if first {
		a.err = err
	}
	a.mu.Unlock()
	if first {
		report("writing log file", err)
	}
}

func (a *Async) finish() {
	a.mu.Lock()
	file := a.file
	a.file = nil
	if a.epoch > a.acked {
		a.acked = a.epoch
	}
	a.flushWait.Broadcast()
	a.mu.Unlock()

	if file == nil {
		return
	}
	if err := file.Close(); err != nil {
		a.noteErr(err)
	}
}

type chunk struct {
	b []byte
}

func newChunk(size int) *chunk {
	return &chunk{b: make([]byte, 0, size)}
}

func (c *chunk) avail() int { return cap(c.b) - len(c.b) }

func (c *chunk) write(p []byte) { c.b = append(c.b, p...) }

func (c *chunk) reset() { c.b = c.b[:0] }

type panicError struct{ value any }

func errPanic(value any) error { return panicError{value: value} }

func (e panicError) Error() string {
	return "yukino_log: backend panicked: " + stringify(e.value)
}

func stringify(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case error:
		return v.Error()
	default:
		return fmt.Sprint(v)
	}
}
