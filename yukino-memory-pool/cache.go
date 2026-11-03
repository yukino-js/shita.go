package yukino_mempool

import "sync"

// threadCache is one shard of the fast path. The pool keeps a fixed set of
// shards, about one per processor, instead of a sync.Pool.
//
// sync.Pool cannot host this cache. Its private slot is not stolen by other
// processors, and a goroutine that is rescheduled between Get and the next
// Put leaves a full batch stranded there. The pool also drops items at
// garbage-collection time. Either effect looks like an empty cache, so the
// next allocation cuts a fresh span while the previous span still holds
// blocks. Under the race detector, where preemption and collection are
// frequent, that allocated a new region for almost every batch.
//
// A shard is always addressable and is only ever touched under mu. Lock
// order is mu, then the central mutex, then the page mutex.
type threadCache struct {
	mu   sync.Mutex
	free [][][]byte
}

func (t *threadCache) init() {
	t.free = make([][][]byte, len(classSizes))
}

// pop returns one full size-class block, refilling from the central cache
// when the local list is empty. The caller holds t.mu.
func (t *threadCache) pop(p *Pool, class int) []byte {
	if len(t.free[class]) == 0 {
		want := batchFor(classSizes[class])
		// clear the whole backing array, not just len. clear on a zero-length
		// slice would leave headers past len pinning regions the cache already
		// gave back.
		buf := t.free[class]
		if cap(buf) > 0 {
			clear(buf[:cap(buf)])
		}
		t.free[class] = p.classes[class].take(buf[:0], want)
		if len(t.free[class]) == 0 {
			panic("yukino_mempool: central cache returned an empty batch")
		}
	}
	last := len(t.free[class]) - 1
	full := t.free[class][last]
	t.free[class][last] = nil
	t.free[class] = t.free[class][:last]
	return full
}

// push stores a full block and, once the list is larger than the class
// allows, returns the surplus to the central cache. The C++ path decremented
// its counter before knowing whether a block was present, wrapped it through
// zero, and then refused to return anything because the "length" exceeded
// the real list. The length here is len of the slice, so the two cannot
// diverge. The caller holds t.mu.
func (t *threadCache) push(p *Pool, class int, full []byte) {
	t.free[class] = append(t.free[class], full)
	limit := flushLimit(classSizes[class])
	if len(t.free[class]) > limit {
		keep := len(t.free[class]) / 4
		if keep < 1 {
			keep = 1
		}
		t.flushLocked(p, class, keep)
	}
}

// flushAll returns every cached block. The caller does not hold t.mu.
func (t *threadCache) flushAll(p *Pool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for class := range t.free {
		t.flushLocked(p, class, 0)
	}
}

// flushLocked returns every block past the first keep. The caller holds t.mu.
// The returned slice is a copy, and the cache's own backing array is cleared
// past keep, so a region released by giveBack is not pinned by an abandoned
// slice header.
func (t *threadCache) flushLocked(p *Pool, class, keep int) {
	list := t.free[class]
	if len(list) <= keep {
		return
	}
	returned := make([][]byte, len(list)-keep)
	copy(returned, list[keep:])
	clear(list[keep:cap(list)])
	t.free[class] = list[:keep]
	p.classes[class].giveBack(returned)
}
