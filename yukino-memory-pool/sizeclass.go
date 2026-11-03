package mempool

import "fmt"

// Layout of the allocator. The numbers match the C++ pool where that pool's
// behaviour is sound (8-byte minimum, 256 KiB cutoff, 4 KiB pages, 8-page
// spans, batches that aim at 4 KiB). They diverge where the C++ numbers were
// themselves the defect: one size class every 8 bytes up to 256 KiB, and a
// batch floor of sizeof(1).
const (
	// alignment is the smallest block and the alignment of every block start.
	alignment = 8

	// MaxSmall is the largest request served by the size-class caches.
	// Larger requests go through make; see Pool.Alloc.
	MaxSmall = 256 * 1024

	// pageSize is the unit in which the page cache tracks and coalesces
	// storage. It is the virtual-memory page size assumed by the C++ pool,
	// not a claim that the host page is smaller.
	pageSize = 4096

	// spanPages is the span handed to a size class whose block fits in that
	// many pages. Larger classes get a span of exactly one block, rounded up
	// to whole pages.
	spanPages = 8

	maxBatchBytes  = 4 * 1024
	maxLocalBlocks = 64
	maxLocalBytes  = 1 << 20

	// Caps on idle spans. The C++ page cache never returned memory and never
	// coalesced it correctly, so a spike became the resident set for the
	// life of the process. Whole regions beyond these caps are dropped for
	// the garbage collector. Remnants of a region that is still partly in
	// use are kept regardless, because dropping them would abandon the rest
	// of the region.
	maxCachedSpansPerPages = 4
	maxCachedFreePages     = 1024
)

// classSizes is the block size of each size class, in ascending order.
// classIndex[n] is the class that serves a request of n bytes, for n in
// 1..MaxSmall. Both are built once in init.
var (
	classSizes []int
	classIndex []uint16
)

func init() {
	classSizes = buildClassSizes()
	previous := 0
	for _, size := range classSizes {
		if size <= previous || size%alignment != 0 || size > MaxSmall {
			panic(fmt.Sprintf("mempool: size class %d is not a strictly increasing multiple of %d within MaxSmall", size, alignment))
		}
		// Classes above one default span are page multiples, so a span of
		// that class holds an integral number of blocks and a block never
		// starts off a page boundary.
		if size > spanPages*pageSize && size%pageSize != 0 {
			panic(fmt.Sprintf("mempool: size class %d above one span is not a multiple of the page size", size))
		}
		previous = size
	}
	if previous != MaxSmall {
		panic("mempool: size class table must end at MaxSmall")
	}

	classIndex = make([]uint16, MaxSmall+1)
	class := 0
	for n := 1; n <= MaxSmall; n++ {
		for classSizes[class] < n {
			class++
			if class == len(classSizes) {
				panic("mempool: size class table does not cover MaxSmall")
			}
		}
		classIndex[n] = uint16(class)
	}
}

// buildClassSizes returns the geometric size-class table.
//
//   - 8 B steps through 256 B
//   - 64 B steps through 4 KiB
//   - 512 B steps through 32 KiB
//   - 4 KiB steps through 256 KiB
//
// 204 classes replace the 32,768 classes of the C++ pool. A class every 8
// bytes forced every thread to carry two arrays of that length (half a
// megabyte of metadata) and made the central cache allocate the same number
// of locks. Goroutines are cheap enough that copying that layout would pin
// more metadata than payload.
func buildClassSizes() []int {
	sizes := make([]int, 0, 204)
	for size := alignment; size <= 256; size += alignment {
		sizes = append(sizes, size)
	}
	for size := 320; size <= 4096; size += 64 {
		sizes = append(sizes, size)
	}
	for size := 4608; size <= 32*1024; size += 512 {
		sizes = append(sizes, size)
	}
	for size := 36 * 1024; size <= MaxSmall; size += pageSize {
		sizes = append(sizes, size)
	}
	return sizes
}

// BlockSize reports the size-class block that would back a request of n
// bytes. The second result is false when n is outside 1..MaxSmall and the
// request would not be served from the page cache.
func BlockSize(n int) (int, bool) {
	if n <= 0 || n > MaxSmall {
		return 0, false
	}
	return classSizes[classIndex[n]], true
}

// pagesFor returns the number of pages in a fresh span for a block of the
// given class size.
func pagesFor(block int) int {
	spanBytes := spanPages * pageSize
	if block <= spanBytes {
		return spanPages
	}
	return (block + pageSize - 1) / pageSize
}

// batchFor is the number of blocks a processor cache pulls from the central
// cache at once. The C++ expression used sizeof(1) as its lower bound, which
// is 4 rather than 1, so classes that were supposed to move one block at a
// time moved four and then recorded four even when the span held fewer.
func batchFor(block int) int {
	base := 1
	switch {
	case block <= 32:
		base = 64
	case block <= 64:
		base = 32
	case block <= 128:
		base = 16
	case block <= 256:
		base = 8
	case block <= 512:
		base = 4
	case block <= 1024:
		base = 2
	}
	limit := maxBatchBytes / block
	if limit < 1 {
		limit = 1
	}
	if base < limit {
		return base
	}
	return limit
}

// flushLimit is the number of cached blocks above which a processor cache
// returns the surplus to the central cache. The C++ threshold was a flat 64
// blocks, which pins 16 MiB on a 256 KiB class. The limit here is also capped
// by maxLocalBytes.
func flushLimit(block int) int {
	limit := maxLocalBytes / block
	if limit < 1 {
		limit = 1
	}
	if limit > maxLocalBlocks {
		limit = maxLocalBlocks
	}
	return limit
}
