package yukino_mempool

import (
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Pool is a size-class allocator. The zero value is not usable; construct
// one with New.
//
// Pool must not be copied after New. Pass it by pointer.
type Pool struct {
	classes []centralCache
	pages   pageCache

	// shards is the fast path, sized to GOMAXPROCS at construction. shardNext
	// spreads callers across shards so a single goroutine does not serialise
	// on one mutex and a migrated goroutine does not lose its batch.
	shards    []threadCache
	shardNext atomic.Uint64

	smallAllocs atomic.Uint64
	largeAllocs atomic.Uint64
	frees       atomic.Uint64
	outstanding atomic.Int64
	held        atomic.Int64

	large sync.Map // uintptr(data) -> *largeBlock
}

// largeBlock keeps a large allocation reachable until Free. The caller's
// slice is a second reference; deleting the map entry does not free the
// bytes while the caller still holds them, and dropping the caller's slice
// without Free leaks the block for as long as the pool lives. That matches
// malloc, which also requires an explicit free.
type largeBlock struct {
	mem []byte
}

// Stats is a point-in-time view of a pool. Counters are monotonically
// increasing except Outstanding, HeldBytes and the cache gauges.
type Stats struct {
	// SmallAllocs is the number of Alloc calls served from a size class.
	SmallAllocs uint64
	// LargeAllocs is the number of Alloc calls served with make.
	LargeAllocs uint64
	// Frees is the number of successful Free calls, excluding Free of a nil
	// or zero-capacity slice.
	Frees uint64
	// Outstanding is the number of blocks currently in caller hands.
	Outstanding int64
	// HeldBytes is the number of bytes in those blocks. Small blocks
	// contribute their size-class size, not the requested length.
	HeldBytes int64
	// ActiveSpans is the number of spans currently carved into blocks.
	ActiveSpans int
	// LiveRegions is the number of backing allocations still referenced by
	// the pool, including idle ones kept for reuse.
	LiveRegions int
	// ReleasedRegions is the number of backing allocations dropped so the
	// garbage collector can reuse them.
	ReleasedRegions int
	// CachedFreeSpans is the number of idle spans waiting to be reused.
	CachedFreeSpans int
	// CachedFreePages is the total page count of those idle spans.
	CachedFreePages int
}

// New returns an empty pool.
func New() *Pool {
	p := &Pool{
		classes: make([]centralCache, len(classSizes)),
	}
	p.pages.init()
	for i := range p.classes {
		p.classes[i].pool = p
		p.classes[i].class = i
	}
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		n = 1
	}
	p.shards = make([]threadCache, n)
	for i := range p.shards {
		p.shards[i].init()
	}
	return p
}

// Alloc returns a zeroed slice of length n. For n in 1..MaxSmall the
// capacity is the size class (see BlockSize) and the caller must pass the
// slice, or another slice with the same data pointer, to Free. A request of
// zero returns a non-nil empty slice that Free ignores. A negative request
// panics.
//
// The capacity stops at the end of the block. Appending past it allocates a
// new array with make; that array is not owned by the pool, and Free of it
// panics. Free the slice Alloc returned.
func (p *Pool) Alloc(n int) []byte {
	p.ready()
	if n < 0 {
		panic("yukino_mempool: negative allocation size")
	}
	if n == 0 {
		return []byte{}
	}
	if n > MaxSmall {
		return p.allocLarge(n)
	}

	class := int(classIndex[n])
	shard := p.shard()
	shard.mu.Lock()
	full := shard.pop(p, class)
	shard.mu.Unlock()
	if err := p.mark(class, full); err != nil {
		// A freshly popped block must be unmarked. Anything else means the
		// span's ownership bits disagree with the free stack; putting the
		// block back would publish that disagreement. A foreign block is not
		// owned either, so it goes back onto the shard that produced it
		// rather than being dropped.
		if !errors.Is(err, ErrDoubleFree) {
			shard.mu.Lock()
			shard.push(p, class, full)
			shard.mu.Unlock()
		}
		p.fault(err, unsafe.Pointer(unsafe.SliceData(full)))
	}
	clear(full)
	p.smallAllocs.Add(1)
	p.outstanding.Add(1)
	p.held.Add(int64(len(full)))
	return full[:n]
}

// Free returns a slice to the pool. Free of a nil slice, or of any slice
// with zero capacity, is a no-op so that Free(Alloc(0)) and Free(nil) are
// safe. Any other slice must start at an address Alloc returned from this
// pool. Misuse logs the address and panics with ErrDoubleFree,
// ErrForeignPointer or ErrInteriorPointer.
//
// Free is safe to call from a goroutine other than the one that called Alloc.
func (p *Pool) Free(b []byte) {
	p.ready()
	if cap(b) == 0 {
		return
	}
	ptr := unsafe.Pointer(unsafe.SliceData(b))
	if ptr == nil {
		return
	}

	if s := p.pages.spanOf(b); s != nil {
		full, err := s.unmark(b)
		if err != nil {
			p.fault(err, ptr)
		}
		p.outstanding.Add(-1)
		p.held.Add(-int64(len(full)))
		p.frees.Add(1)
		shard := p.shard()
		shard.mu.Lock()
		shard.push(p, s.class, full)
		shard.mu.Unlock()
		return
	}

	if value, ok := p.large.LoadAndDelete(uintptr(ptr)); ok {
		block := value.(*largeBlock)
		p.outstanding.Add(-1)
		p.held.Add(-int64(len(block.mem)))
		p.frees.Add(1)
		return
	}
	p.fault(ErrForeignPointer, ptr)
}

// Flush returns every block sitting in a shard to the central cache, and
// from there to the page cache when a span is completely free.
//
// Alloc and Free may run concurrently with Flush. A block popped by an Alloc
// that is still between the shard and mark is not visible to this call; the
// caller of Alloc will return it through Free, or the next Flush will see it
// if it was pushed back. Tests and shutdown paths that have already stopped
// allocators observe a complete flush.
func (p *Pool) Flush() {
	p.ready()
	for i := range p.shards {
		p.shards[i].flushAll(p)
	}
}

// Stats returns counters for tests and diagnostics. It does not stop
// concurrent Alloc and Free, so the fields are not one atomic snapshot.
func (p *Pool) Stats() Stats {
	p.ready()
	active, live, released, cachedSpans, cachedPages := p.pages.snapshot()
	return Stats{
		SmallAllocs:     p.smallAllocs.Load(),
		LargeAllocs:     p.largeAllocs.Load(),
		Frees:           p.frees.Load(),
		Outstanding:     p.outstanding.Load(),
		HeldBytes:       p.held.Load(),
		ActiveSpans:     active,
		LiveRegions:     live,
		ReleasedRegions: released,
		CachedFreeSpans: cachedSpans,
		CachedFreePages: cachedPages,
	}
}

func (p *Pool) allocLarge(n int) []byte {
	block := &largeBlock{mem: make([]byte, n)}
	addr := uintptr(unsafe.Pointer(unsafe.SliceData(block.mem)))
	p.large.Store(addr, block)
	p.largeAllocs.Add(1)
	p.outstanding.Add(1)
	p.held.Add(int64(n))
	return block.mem
}

func (p *Pool) mark(class int, full []byte) error {
	s := p.pages.spanOf(full)
	if s == nil || s.class != class {
		return ErrForeignPointer
	}
	return s.mark(full)
}

func (p *Pool) shard() *threadCache {
	n := uint64(len(p.shards))
	return &p.shards[p.shardNext.Add(1)%n]
}

func (p *Pool) ready() {
	if p == nil || p.classes == nil {
		panic("yukino_mempool: nil pool")
	}
}

// fault records a rejected Free and panics. The log line is emitted even
// when the caller recovers the panic, which is what a long-running process
// wants from an allocator misuse that would otherwise be silent.
func (p *Pool) fault(err error, ptr unsafe.Pointer) {
	addr := uintptr(ptr)
	slog.Error("rejected free", "addr", fmt.Sprintf("%#x", addr), "err", err)
	panic(fmt.Errorf("yukino_mempool: %w at %#x", err, addr))
}

func sliceData(b []byte) unsafe.Pointer {
	if cap(b) == 0 {
		return nil
	}
	return unsafe.Pointer(unsafe.SliceData(b))
}
