package asynclog

import "sync"

// AsyncType selects the back-pressure behaviour of the async worker.
type AsyncType int

const (
	// AsyncSafe uses a bounded buffer and blocks producers when it is full,
	// guaranteeing that no record is dropped at the cost of possible blocking.
	AsyncSafe AsyncType = iota
	// AsyncUnsafe uses a growing buffer so producers never block, at the cost
	// of potentially unbounded memory use under sustained load.
	AsyncUnsafe
)

// Worker moves records from a producer buffer to a consumer buffer on a
// dedicated goroutine and hands the consumer buffer to a callback for flushing.
//
// The double-buffer design keeps the producer's critical section limited to a
// short copy plus a cursor swap. That is the property which keeps logging off
// the hot path of the business logic.
type Worker struct {
	asyncType AsyncType
	callback  func(*Buffer)

	mu       sync.Mutex
	prodCond *sync.Cond // signalled when the producer buffer gains free space
	consCond *sync.Cond // signalled when the producer buffer gains data or on stop

	producer *Buffer
	consumer *Buffer

	stopped bool
	dropped uint64

	done     chan struct{}
	stopOnce sync.Once
}

// NewWorker creates a worker that delivers full buffers to callback and starts
// its background goroutine immediately.
func NewWorker(callback func(*Buffer), asyncType AsyncType, g Growth) *Worker {
	w := &Worker{
		asyncType: asyncType,
		callback:  callback,
		producer:  NewBuffer(g),
		consumer:  NewBuffer(g),
		done:      make(chan struct{}),
	}
	w.prodCond = sync.NewCond(&w.mu)
	w.consCond = sync.NewCond(&w.mu)
	go w.run()
	return w
}

// run is the consumer loop. It drains every buffered record before exiting so
// that Stop never loses data, correcting a defect in the original
// implementation where records pushed after the final swap were discarded.
func (w *Worker) run() {
	defer close(w.done)
	for {
		w.mu.Lock()
		for !w.stopped && w.producer.IsEmpty() {
			w.consCond.Wait()
		}
		if w.stopped && w.producer.IsEmpty() {
			w.mu.Unlock()
			return
		}
		w.producer.Swap(w.consumer)
		if w.asyncType == AsyncSafe {
			// Only a bounded buffer needs to wake blocked producers.
			w.prodCond.Broadcast()
		}
		w.mu.Unlock()

		if !w.consumer.IsEmpty() {
			w.callback(w.consumer)
		}
		w.consumer.Reset()
	}
}

// Push appends a formatted record to the producer buffer.
//
// In AsyncSafe mode the call blocks while the buffer is full; in AsyncUnsafe
// mode the buffer grows instead. Records submitted after Stop are dropped and
// counted.
func (w *Worker) Push(p []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		w.dropped++
		return
	}
	if w.asyncType == AsyncSafe {
		// A single record larger than the whole buffer can never fit, so grow
		// once instead of deadlocking. This repairs a deadlock present in the
		// original implementation.
		if w.producer.Cap() < len(p) {
			w.producer.Push(p)
			w.consCond.Signal()
			return
		}
		for w.producer.writeable() < len(p) && !w.stopped {
			w.prodCond.Wait()
		}
		if w.stopped {
			w.dropped++
			return
		}
	}
	w.producer.Push(p)
	w.consCond.Signal()
}

// Stop signals the worker to flush any buffered records and exit. It is
// idempotent and blocks until the worker goroutine has returned.
func (w *Worker) Stop() {
	w.stopOnce.Do(func() {
		w.mu.Lock()
		w.stopped = true
		w.consCond.Broadcast()
		w.prodCond.Broadcast()
		w.mu.Unlock()
	})
	<-w.done
}

// Dropped returns the number of records discarded because they arrived after
// Stop was called.
func (w *Worker) Dropped() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.dropped
}
