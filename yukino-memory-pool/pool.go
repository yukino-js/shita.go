package yukino_mempool

import (
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"unsafe"
)

// Pool is a size-class allocator. The zero value is not usable; construct
// one with New.
//
// Pool must not be copied after New. Pass it by pointer.
type Pool struct {
	classes []centralCache
	pages   pageCache

	// shards is the fast path. The count is GOMAXPROCS rounded up to a power
	// of two, so a shard is selected with a mask rather than a division and
	// there are never fewer shards than processors. Each goroutine maps to one
	// shard from the address of its own stack, so choosing a shard touches
	// nothing shared; see homeIndex. The counters that Stats reports are per
	// shard for the same reason.
	shards    []threadCache
	shardMask uintptr

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
//
// The per-shard counters are summed one shard at a time, so a value observed
// while other goroutines allocate is a mix of instants. It is exact once the
// pool is quiescent.
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
	count := 1
	for count < n {
		count <<= 1
	}
	p.shardMask = uintptr(count - 1)
	p.shards = make([]threadCache, count)
	for i := range p.shards {
		p.shards[i].init()
	}
	return p
}

// defaultPool backs the package-level Alloc and Free. It is created on first
// use rather than at init, so a program that only ever calls New pays nothing
// for it.
var (
	defaultOnce sync.Once
	defaultPool *Pool
)

// Default returns the process-wide pool behind the package-level Alloc and
// Free. It stands in for the C++ MemoryPool singleton, whose allocate and
// deallocate were static and shared one thread cache, central cache and page
// cache across the whole program. It lives for the lifetime of the process.
//
// Prefer New when a caller wants a pool it can Flush, inspect with Stats, or
// drop on its own; everything that shares Default shares its caches and its
// counters.
func Default() *Pool {
	defaultOnce.Do(func() { defaultPool = New() })
	return defaultPool
}

// Alloc allocates n bytes from the default pool. It is Default().Alloc, and
// the package-level equivalent of the C++ static MemoryPool::allocate. See
// Pool.Alloc for the contract, including the size-class capacity and the
// requirement to pass the returned slice to Free.
func Alloc(n int) []byte { return Default().Alloc(n) }

// Free returns b to the default pool. It is Default().Free, and the
// package-level equivalent of the C++ static MemoryPool::deallocate. The C++
// call also took a size, which selected the free list; here the address alone
// identifies the block, so a wrong size cannot misfile it. See Pool.Free for
// the contract.
func Free(b []byte) { Default().Free(b) }

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
	shard := p.shardLocked()
	full := shard.pop(p, class)
	shard.smallAllocs++
	shard.outstanding++
	shard.held += int64(len(full))
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
		shard := p.shardLocked()
		shard.push(p, s.class, full)
		shard.outstanding--
		shard.held -= int64(len(full))
		shard.frees++
		shard.mu.Unlock()
		return
	}

	if value, ok := p.large.LoadAndDelete(uintptr(ptr)); ok {
		block := value.(*largeBlock)
		shard := p.shardLocked()
		shard.outstanding--
		shard.held -= int64(len(block.mem))
		shard.frees++
		shard.mu.Unlock()
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
	var counters Stats
	for i := range p.shards {
		shard := &p.shards[i]
		shard.mu.Lock()
		counters.SmallAllocs += shard.smallAllocs
		counters.LargeAllocs += shard.largeAllocs
		counters.Frees += shard.frees
		counters.Outstanding += shard.outstanding
		counters.HeldBytes += shard.held
		shard.mu.Unlock()
	}
	active, live, released, cachedSpans, cachedPages := p.pages.snapshot()
	return Stats{
		SmallAllocs:     counters.SmallAllocs,
		LargeAllocs:     counters.LargeAllocs,
		Frees:           counters.Frees,
		Outstanding:     counters.Outstanding,
		HeldBytes:       counters.HeldBytes,
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
	shard := p.shardLocked()
	shard.largeAllocs++
	shard.outstanding++
	shard.held += int64(n)
	shard.mu.Unlock()
	return block.mem
}

func (p *Pool) mark(class int, full []byte) error {
	s := p.pages.spanOf(full)
	if s == nil || s.class != class {
		return ErrForeignPointer
	}
	return s.mark(full)
}

// shardMultiplier is the 64-bit golden ratio. Multiplying an address by it
// moves the entropy in the low bits, which stack allocation leaves nearly
// constant, up into the bits homeIndex selects on. It is typed uint64, and the
// hash is computed in uint64, so the package still builds where uintptr is
// only 32 bits wide.
const shardMultiplier uint64 = 0x9E3779B97F4A7C15

// homeIndex returns the shard the calling goroutine prefers.
//
// The C++ pool selected its cache with thread_local, which costs nothing. Go
// has no goroutine-local storage, and the obvious substitute, a shared
// round-robin counter, puts every Alloc and Free on one cache line. Measured
// on eight cores that ran at 373 ns/op against 32 ns/op on one, so the pool
// got slower as callers were added.
//
// The address of a local variable is used instead. It lies on the caller's
// stack, so it differs between goroutines and stays put while that stack
// lives, and taking it neither allocates nor lets the variable escape. A
// goroutine whose stack grows moves to another shard, which only changes
// which cache it refills from: any shard may hold a block of any span, so
// correctness does not depend on the choice.
func (p *Pool) homeIndex() int {
	var probe uintptr
	hash := uint64(uintptr(unsafe.Pointer(&probe))) * shardMultiplier
	return int((hash >> 51) & uint64(p.shardMask))
}

// shardLocked returns a locked cache for the caller. It prefers the home
// shard, but rather than block behind another goroutine it tries the next
// shard over, and the next. Blocking on the shard mutex is what made the
// scheduler park and wake goroutines, and the parking cost more than the lock
// itself; TryLock turns a collision into a redirect. Only when every shard is
// held does it fall back to a blocking lock.
func (p *Pool) shardLocked() *threadCache {
	n := len(p.shards)
	home := p.homeIndex()
	i := home
	for k := 0; k < n; k++ {
		shard := &p.shards[i]
		if shard.mu.TryLock() {
			return shard
		}
		if i++; i == n {
			i = 0
		}
	}
	shard := &p.shards[home]
	shard.mu.Lock()
	return shard
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
