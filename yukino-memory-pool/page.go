package mempool

import (
	"fmt"
	"log/slog"
	"sync"
	"unsafe"
)

// pageCache allocates, splits, coalesces and releases spans. The mutex guards
// the free-span indexes and the active set. The page map is a sync.Map so a
// free can find the owning span without taking that mutex; publication and
// removal still happen while the allocating central cache holds its own lock,
// and the page mutex is acquired only after that lock (never before it).
type pageCache struct {
	mu sync.Mutex

	lookup sync.Map // page address (uintptr) -> *span

	freeHead  map[int]*span
	freeCount map[int]int
	byBase    map[uintptr]*span
	byEnd     map[uintptr]*span
	active    map[*span]struct{}

	maxFreePages int
	cachedPages  int
	live         int
	released     int
}

func (p *pageCache) init() {
	p.freeHead = make(map[int]*span)
	p.freeCount = make(map[int]int)
	p.byBase = make(map[uintptr]*span)
	p.byEnd = make(map[uintptr]*span)
	p.active = make(map[*span]struct{})
}

// allocate returns a span of exactly npages. The span is not yet published
// and not yet carved; the caller prepares and publishes it. The page mutex
// is not held on return.
func (p *pageCache) allocate(npages int) *span {
	p.mu.Lock()
	defer p.mu.Unlock()

	for n := npages; n <= p.maxFreePages; n++ {
		s := p.freeHead[n]
		if s == nil {
			continue
		}
		p.removeFree(s)
		if s.npages > npages {
			p.splitOff(s, npages)
		}
		return s
	}
	return p.systemAlloc(npages)
}

// splitOff keeps the leading npages of s and caches the tail. s is not on
// the free list. Both pieces share the region, so a later free can merge
// them; the tail is not published, because no block in it has been handed out.
func (p *pageCache) splitOff(s *span, npages int) {
	if len(s.mem) != s.npages*pageSize {
		panic("mempool: idle span length does not match its page count")
	}
	cut := npages * pageSize
	total := len(s.mem)
	restMem := s.mem[cut:total:total]
	s.mem = s.mem[:cut:cut]

	restBase := s.base + uintptr(cut)
	if uintptr(unsafe.Pointer(unsafe.SliceData(restMem))) != restBase {
		panic("mempool: split produced a non-contiguous tail")
	}
	rest := &span{
		mem:    restMem,
		raw:    s.raw,
		region: s.region,
		base:   restBase,
		npages: s.npages - npages,
		class:  -1,
	}
	s.npages = npages
	p.pushFree(rest)
}

// systemAlloc obtains a fresh region aligned to pageSize. The alignment
// prefix stays inside region.raw so the aligned slice and the prefix are one
// garbage-collected object.
func (p *pageCache) systemAlloc(npages int) *span {
	size := npages * pageSize
	raw := make([]byte, size+pageSize-1)
	rawPtr := uintptr(unsafe.Pointer(unsafe.SliceData(raw)))
	base := (rawPtr + pageSize - 1) &^ uintptr(pageSize-1)
	off := int(base - rawPtr)
	mem := raw[off : off+size : off+size]
	reg := &region{raw: raw, base: base, npages: npages}
	p.live++
	return &span{
		mem:    mem,
		raw:    raw,
		region: reg,
		base:   base,
		npages: npages,
		class:  -1,
	}
}

// publish makes every page of an active span resolvable by address. The
// span's class, block size and free stack must already be initialised: a
// lookup can observe the span as soon as the first page is stored.
func (p *pageCache) publish(s *span) {
	for i := 0; i < s.npages; i++ {
		p.lookup.Store(s.base+uintptr(i*pageSize), s)
	}
	p.mu.Lock()
	p.active[s] = struct{}{}
	p.mu.Unlock()
}

// spanOf returns the active span that contains b, or nil when b is not a
// pooled small block.
func (p *pageCache) spanOf(b []byte) *span {
	if cap(b) == 0 {
		return nil
	}
	addr := uintptr(unsafe.Pointer(unsafe.SliceData(b)))
	page := addr &^ uintptr(pageSize-1)
	value, ok := p.lookup.Load(page)
	if !ok {
		return nil
	}
	s := value.(*span)
	if s.dead.Load() || s.blockSize == 0 {
		return nil
	}
	if addr < s.base || addr >= s.end() {
		return nil
	}
	return s
}

// reclaim takes a fully free span out of the page map and either caches it
// or, once coalescing has rebuilt a whole region the cache does not need,
// drops it. The caller holds the central mutex of the span's former class.
func (p *pageCache) reclaim(s *span) {
	s.retire()
	for i := 0; i < s.npages; i++ {
		p.lookup.CompareAndDelete(s.base+uintptr(i*pageSize), s)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.active, s)
	p.coalesceAndCache(s)
}

// coalesceAndCache merges s with idle neighbours from the same region and
// caches the result. s itself is not on the free list.
func (p *pageCache) coalesceAndCache(s *span) {
	for {
		next := p.byBase[s.end()]
		if next == nil || next.region != s.region {
			break
		}
		p.removeFree(next)
		s.npages += next.npages
		next.mem = nil
		next.raw = nil
		next.region = nil
	}
	for {
		prev := p.byEnd[s.base]
		if prev == nil || prev.region != s.region {
			break
		}
		p.removeFree(prev)
		prev.npages += s.npages
		s.mem = nil
		s.raw = nil
		s = prev
	}
	s.resetMem()

	overBucket := p.freeCount[s.npages] >= maxCachedSpansPerPages
	overBytes := p.cachedPages+s.npages > maxCachedFreePages
	if s.whole() && (overBucket || overBytes) {
		p.release(s)
		return
	}
	p.pushFree(s)
}

// release drops the only references to a whole region so the collector can
// reuse it. The C++ pool mapped pages with mmap and had no munmap path.
func (p *pageCache) release(s *span) {
	pages := s.npages
	bytes := pages * pageSize
	if s.region != nil {
		s.region.raw = nil
	}
	s.mem = nil
	s.raw = nil
	s.region = nil
	p.live--
	p.released++
	slog.Debug("released idle region to the garbage collector", "pages", pages, "bytes", bytes)
}

func (p *pageCache) pushFree(s *span) {
	n := s.npages
	s.prev = nil
	s.next = p.freeHead[n]
	if s.next != nil {
		s.next.prev = s
	}
	s.class = -1
	s.dead.Store(true)
	p.freeHead[n] = s
	p.byBase[s.base] = s
	p.byEnd[s.end()] = s
	p.freeCount[n]++
	p.cachedPages += n
	if n > p.maxFreePages {
		p.maxFreePages = n
	}
}

func (p *pageCache) removeFree(s *span) {
	n := s.npages
	if s.prev != nil {
		s.prev.next = s.next
	} else if p.freeHead[n] == s {
		p.freeHead[n] = s.next
	}
	if s.next != nil {
		s.next.prev = s.prev
	}
	s.next = nil
	s.prev = nil
	delete(p.byBase, s.base)
	delete(p.byEnd, s.end())
	p.freeCount[n]--
	p.cachedPages -= n
	if p.freeCount[n] <= 0 {
		delete(p.freeCount, n)
		delete(p.freeHead, n)
		if n == p.maxFreePages {
			p.recomputeMax()
		}
	}
}

func (p *pageCache) recomputeMax() {
	maxPages := 0
	for n, count := range p.freeCount {
		if count > 0 && n > maxPages {
			maxPages = n
		}
	}
	p.maxFreePages = maxPages
}

// snapshot copies the counters the public Stats value exposes. The caller
// does not hold p.mu.
func (p *pageCache) snapshot() (active, live, released, cachedSpans, cachedPages int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, count := range p.freeCount {
		cachedSpans += count
	}
	return len(p.active), p.live, p.released, cachedSpans, p.cachedPages
}

// audit checks the page map against the active set and the free-span index.
// Callers must not run Alloc or Free on the pool at the same time.
func (p *pageCache) audit() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	seen := make(map[*span]int, len(p.active))
	var auditErr error
	p.lookup.Range(func(_, value any) bool {
		s := value.(*span)
		if s.dead.Load() || s.blockSize == 0 {
			auditErr = fmt.Errorf("page map contains an idle span at %#x", s.base)
			return false
		}
		seen[s]++
		return true
	})
	if auditErr != nil {
		return auditErr
	}
	if len(seen) != len(p.active) {
		return fmt.Errorf("page map has %d spans, active set has %d", len(seen), len(p.active))
	}
	for s := range p.active {
		if seen[s] != s.npages {
			return fmt.Errorf("span %#x published on %d pages, owns %d", s.base, seen[s], s.npages)
		}
		if err := s.auditStack(); err != nil {
			return fmt.Errorf("span %#x: %w", s.base, err)
		}
	}
	for base, s := range p.byBase {
		if base != s.base {
			return fmt.Errorf("free span indexed at %#x but based at %#x", base, s.base)
		}
		if s.class != -1 || s.blockSize != 0 || !s.dead.Load() {
			return fmt.Errorf("free span %#x still looks active", s.base)
		}
		if _, ok := p.active[s]; ok {
			return fmt.Errorf("span %#x is both active and idle", s.base)
		}
		if p.byEnd[s.end()] != s {
			return fmt.Errorf("free span %#x missing from the end index", s.base)
		}
	}
	return nil
}

// auditStack checks the free stack against its bitmap. Blocks currently in a
// processor cache are in neither structure, so the two counts may be less
// than total; they must not exceed it or disagree with each other.
func (s *span) auditStack() error {
	if s.total <= 0 || s.blockSize <= 0 {
		return fmt.Errorf("active span has no blocks")
	}
	if len(s.stack) > s.total {
		return fmt.Errorf("free stack %d exceeds %d blocks", len(s.stack), s.total)
	}
	seen := make([]bool, s.total)
	for _, idx := range s.stack {
		if idx < 0 || idx >= s.total || seen[idx] {
			return fmt.Errorf("free stack repeats or rejects index %d", idx)
		}
		seen[idx] = true
		if !bitSet(s.spanBits, idx) {
			return fmt.Errorf("index %d is on the free stack but not in the bitmap", idx)
		}
	}
	if countBits(s.spanBits, s.total) != len(s.stack) {
		return fmt.Errorf("bitmap has %d free blocks, stack has %d", countBits(s.spanBits, s.total), len(s.stack))
	}
	users := 0
	for i := 0; i < s.total; i++ {
		if s.userBits[i>>5].Load()&(1<<uint(i&31)) != 0 {
			users++
			if seen[i] {
				return fmt.Errorf("block %d is both allocated and on the free stack", i)
			}
		}
	}
	if users+len(s.stack) > s.total {
		return fmt.Errorf("%d allocated + %d free exceeds %d", users, len(s.stack), s.total)
	}
	return nil
}
