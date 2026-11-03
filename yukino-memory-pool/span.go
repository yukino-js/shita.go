package mempool

import (
	"sync/atomic"
	"unsafe"
)

// span is a contiguous run of pages carved into equal blocks of one size
// class. It is either active (owned by that class's central cache, and
// published in the page map) or idle (owned by the page cache, unpublished,
// and eligible for coalescing).
//
// A block is in exactly one of three places: the span's free stack, a
// processor cache, or the caller. userBits records the third state so a
// second Free fails even when the first Free has not yet reached the span.
// spanBits records the first state and is touched only while the size
// class's central mutex is held.
type span struct {
	mem       []byte
	raw       []byte
	region    *region
	base      uintptr
	npages    int
	class     int
	blockSize int
	total     int
	stack     []int
	userBits  []atomic.Uint32
	spanBits  []byte

	dead      atomic.Bool
	onPartial bool
	next      *span
	prev      *span
}

// region is one make of backing storage. Spans split out of it share the
// region and may be coalesced back together. Spans from different regions
// are never coalesced: two separate makes can end up adjacent in the address
// space, and merging them would hand one allocation's tail to the other.
type region struct {
	raw    []byte
	base   uintptr
	npages int
}

// prepare carves the span for class. The span must already describe the
// pages (mem, base, npages) and must not be published.
func (s *span) prepare(class, blockSize int) {
	if len(s.mem) < blockSize {
		panic("mempool: span is smaller than one block")
	}
	if uintptr(unsafe.Pointer(unsafe.SliceData(s.mem))) != s.base {
		panic("mempool: span base does not match its memory")
	}
	s.class = class
	s.blockSize = blockSize
	s.total = len(s.mem) / blockSize
	s.stack = make([]int, s.total)
	for i := range s.stack {
		s.stack[i] = i
	}
	s.userBits = make([]atomic.Uint32, (s.total+31)/32)
	s.spanBits = make([]byte, (s.total+7)/8)
	for i := 0; i < s.total; i++ {
		setBit(s.spanBits, i, true)
	}
	s.dead.Store(false)
	s.onPartial = false
	s.next = nil
	s.prev = nil
}

// block returns the idx-th block with its capacity limited to the block, so
// the caller cannot append into the next block.
func (s *span) block(idx int) []byte {
	off := idx * s.blockSize
	return s.mem[off : off+s.blockSize : off+s.blockSize]
}

// pop removes one block from the free stack. The caller holds the central
// mutex and has already observed a non-empty stack.
func (s *span) pop() []byte {
	last := len(s.stack) - 1
	idx := s.stack[last]
	s.stack = s.stack[:last]
	setBit(s.spanBits, idx, false)
	return s.block(idx)
}

// push returns a block to the free stack. The caller holds the central mutex.
func (s *span) push(b []byte) error {
	idx, err := s.indexOf(b)
	if err != nil {
		return err
	}
	if bitSet(s.spanBits, idx) {
		return ErrDoubleFree
	}
	setBit(s.spanBits, idx, true)
	s.stack = append(s.stack, idx)
	return nil
}

// mark records that the caller now owns the block. It is safe to call without
// the central mutex; the bit update is atomic.
func (s *span) mark(b []byte) error {
	if s.dead.Load() || s.blockSize == 0 {
		return ErrForeignPointer
	}
	idx, err := s.indexOf(b)
	if err != nil {
		return err
	}
	if !s.claimUser(idx) {
		return ErrDoubleFree
	}
	return nil
}

// unmark records that the caller has given the block back and returns the
// full size-class slice, which is what the processor cache stores. The
// user's slice may be shorter.
func (s *span) unmark(b []byte) ([]byte, error) {
	if s.dead.Load() || s.blockSize == 0 {
		return nil, ErrForeignPointer
	}
	idx, err := s.indexOf(b)
	if err != nil {
		return nil, err
	}
	if !s.releaseUser(idx) {
		return nil, ErrDoubleFree
	}
	return s.block(idx), nil
}

// indexOf maps an address to a block index. Addresses inside the span that
// are not a block start are interior pointers; anything else is foreign.
func (s *span) indexOf(b []byte) (int, error) {
	if cap(b) == 0 {
		return 0, ErrForeignPointer
	}
	addr := uintptr(unsafe.Pointer(unsafe.SliceData(b)))
	limit := s.base + uintptr(s.npages*pageSize)
	if addr < s.base || addr >= limit {
		return 0, ErrForeignPointer
	}
	if s.blockSize == 0 {
		return 0, ErrForeignPointer
	}
	off := int(addr - s.base)
	if off%s.blockSize != 0 {
		return 0, ErrInteriorPointer
	}
	idx := off / s.blockSize
	if idx < 0 || idx >= s.total {
		return 0, ErrForeignPointer
	}
	return idx, nil
}

// end is the address one past the last page.
func (s *span) end() uintptr {
	return s.base + uintptr(s.npages*pageSize)
}

// whole reports whether the span covers every page of its region.
func (s *span) whole() bool {
	return s.region != nil && s.base == s.region.base && s.npages == s.region.npages
}

// resetMem reslices the region so mem covers exactly the pages the span now
// owns. Coalescing changes npages without moving the underlying allocation.
func (s *span) resetMem() {
	if s.region == nil || cap(s.region.raw) == 0 {
		panic("mempool: span has no backing region")
	}
	raw := s.region.raw
	rawBase := uintptr(unsafe.Pointer(unsafe.SliceData(raw)))
	start := int(s.base - rawBase)
	size := s.npages * pageSize
	if start < 0 || start+size > len(raw) {
		panic("mempool: coalesced span escapes its region")
	}
	s.mem = raw[start : start+size : start+size]
	s.raw = raw
}

// retire drops the carved-block state so the span can sit on the page-cache
// free list. The pages themselves stay referenced through mem and raw.
func (s *span) retire() {
	s.dead.Store(true)
	s.class = -1
	s.blockSize = 0
	s.total = 0
	s.stack = nil
	s.userBits = nil
	s.spanBits = nil
	s.onPartial = false
	s.next = nil
	s.prev = nil
}

func (s *span) claimUser(idx int) bool {
	return updateUserBit(&s.userBits[idx>>5], uint32(1)<<uint(idx&31), true)
}

func (s *span) releaseUser(idx int) bool {
	return updateUserBit(&s.userBits[idx>>5], uint32(1)<<uint(idx&31), false)
}

// updateUserBit sets the masked bit when claim is true and clears it
// otherwise. It reports whether the previous state allowed the transition.
func updateUserBit(word *atomic.Uint32, mask uint32, claim bool) bool {
	for {
		old := word.Load()
		set := old&mask != 0
		if claim == set {
			return false
		}
		next := old | mask
		if !claim {
			next = old &^ mask
		}
		if word.CompareAndSwap(old, next) {
			return true
		}
	}
}

func bitSet(bits []byte, idx int) bool {
	return bits[idx>>3]&(1<<uint(idx&7)) != 0
}

func setBit(bits []byte, idx int, on bool) {
	bit := byte(1) << uint(idx&7)
	if on {
		bits[idx>>3] |= bit
		return
	}
	bits[idx>>3] &^= bit
}

func countBits(bits []byte, n int) int {
	count := 0
	for i := 0; i < n; i++ {
		if bitSet(bits, i) {
			count++
		}
	}
	return count
}
