package yukino_mempool

import (
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"unsafe"
)

func TestMain(m *testing.M) {
	// Misuse tests panic on purpose, and Free logs before panicking. Discard
	// that log so a passing run stays quiet.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

func TestSizeClassesCoverEveryRequest(t *testing.T) {
	if len(classSizes) < 2 {
		t.Fatal("size class table is empty")
	}
	if classSizes[0] != alignment || classSizes[len(classSizes)-1] != MaxSmall {
		t.Fatalf("table runs from %d to %d", classSizes[0], classSizes[len(classSizes)-1])
	}
	for n := 1; n <= MaxSmall; n++ {
		got, ok := BlockSize(n)
		if !ok {
			t.Fatalf("BlockSize(%d) not pooled", n)
		}
		if got < n {
			t.Fatalf("class %d is smaller than request %d", got, n)
		}
		if got != classSizes[classIndex[n]] {
			t.Fatalf("BlockSize(%d) = %d, index points at %d", n, got, classSizes[classIndex[n]])
		}
		// The class must be the smallest one that can hold n.
		for _, size := range classSizes {
			if size >= n {
				if size != got {
					t.Fatalf("request %d rounded to %d, smallest fit is %d", n, got, size)
				}
				break
			}
		}
	}
	if _, ok := BlockSize(0); ok {
		t.Fatal("zero is not a size class")
	}
	if _, ok := BlockSize(MaxSmall + 1); ok {
		t.Fatal("MaxSmall+1 must take the large path")
	}
	if _, ok := BlockSize(-1); ok {
		t.Fatal("negative size reported a class")
	}
}

func TestBatchFloorMatchesTheIntendedMinimum(t *testing.T) {
	// The C++ helper returned max(sizeof(1), ...), and sizeof(1) is 4, so an
	// 8 KiB class was fetched four at a time and the counter was advanced by
	// four even when the span held one block.
	if got := batchFor(8 * 1024); got != 1 {
		t.Fatalf("batchFor(8192) = %d, want 1", got)
	}
	if got := batchFor(8); got != 64 {
		t.Fatalf("batchFor(8) = %d, want 64", got)
	}
	if got := batchFor(32); got != 64 {
		t.Fatalf("batchFor(32) = %d, want 64", got)
	}
	if got := flushLimit(32); got != 64 {
		t.Fatalf("flushLimit(32) = %d, want 64", got)
	}
	if got := flushLimit(MaxSmall); got >= 64 || got < 1 {
		t.Fatalf("flushLimit(%d) = %d, want a small positive cap", MaxSmall, got)
	}
}

func TestAllocFreeRoundTrip(t *testing.T) {
	p := New()
	for _, n := range []int{1, 8, 32, 1024, 4096, MaxSmall} {
		buf := p.Alloc(n)
		if buf == nil || len(buf) != n {
			t.Fatalf("Alloc(%d) len = %d", n, len(buf))
		}
		size, _ := BlockSize(n)
		if cap(buf) != size {
			t.Fatalf("Alloc(%d) cap = %d, want class %d", n, cap(buf), size)
		}
		addr := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
		if addr%alignment != 0 {
			t.Fatalf("Alloc(%d) address %#x is not %d-aligned", n, addr, alignment)
		}
		for i := range buf {
			buf[i] = byte(i)
		}
		for i := range buf {
			if buf[i] != byte(i) {
				t.Fatalf("Alloc(%d) byte %d corrupted", n, i)
			}
		}
		p.Free(buf)
	}

	large := p.Alloc(MaxSmall + 1)
	if len(large) != MaxSmall+1 || cap(large) != MaxSmall+1 {
		t.Fatalf("large alloc len/cap = %d/%d", len(large), cap(large))
	}
	p.Free(large)

	p.Flush()
	if err := p.pages.audit(); err != nil {
		t.Fatal(err)
	}
	stats := p.Stats()
	if stats.Outstanding != 0 || stats.HeldBytes != 0 {
		t.Fatalf("after free: outstanding %d held %d", stats.Outstanding, stats.HeldBytes)
	}
	if stats.SmallAllocs != 6 || stats.LargeAllocs != 1 || stats.Frees != 7 {
		t.Fatalf("counters = %+v", stats)
	}
	if stats.ActiveSpans != 0 {
		t.Fatalf("active spans = %d after every block was freed", stats.ActiveSpans)
	}
}

func TestZeroSizeAndNilFree(t *testing.T) {
	p := New()
	buf := p.Alloc(0)
	if buf == nil || len(buf) != 0 || cap(buf) != 0 {
		t.Fatalf("Alloc(0) = %#v", buf)
	}
	p.Free(buf)
	p.Free(nil)
	var empty []byte
	p.Free(empty)
	if p.Stats().Frees != 0 || p.Stats().SmallAllocs != 0 {
		t.Fatalf("zero-size traffic touched counters: %+v", p.Stats())
	}
}

func TestNegativeAndNilPoolPanic(t *testing.T) {
	p := New()
	mustPanic(t, nil, func() { p.Alloc(-1) })
	var none *Pool
	mustPanic(t, nil, func() { none.Alloc(1) })
	mustPanic(t, nil, func() { none.Free([]byte{1}) })
}

func TestRecycledBlocksAreZeroed(t *testing.T) {
	p := New()
	first := p.Alloc(32)
	for i := range first {
		first[i] = 0xAB
	}
	p.Free(first)
	second := p.Alloc(32)
	for i, b := range second {
		if b != 0 {
			t.Fatalf("recycled byte %d = %#x", i, b)
		}
	}
	p.Free(second)
}

func TestAdjacentBlocksDoNotOverlap(t *testing.T) {
	p := New()
	const n = 128
	blocks := make([][]byte, n)
	for i := range blocks {
		// 3000 bytes crosses a page inside the span, so a carving bug that
		// ignores the tail of a page shows up as a torn pattern.
		blocks[i] = p.Alloc(3000)
		for j := range blocks[i] {
			blocks[i][j] = byte(i)
		}
	}
	for i, block := range blocks {
		for j, b := range block {
			if b != byte(i) {
				t.Fatalf("block %d byte %d = %#x, overwritten by a neighbour", i, j, b)
			}
		}
	}
	for _, block := range blocks {
		p.Free(block)
	}
	p.Flush()
	if err := p.pages.audit(); err != nil {
		t.Fatal(err)
	}
}

func TestFreeRejectsMisuse(t *testing.T) {
	p := New()
	buf := p.Alloc(64)
	mustPanic(t, ErrInteriorPointer, func() { p.Free(buf[1:]) })
	// The interior free must not consume the block.
	p.Free(buf)
	mustPanic(t, ErrDoubleFree, func() { p.Free(buf) })

	other := New()
	foreign := other.Alloc(32)
	mustPanic(t, ErrForeignPointer, func() { p.Free(foreign) })
	other.Free(foreign)

	mustPanic(t, ErrForeignPointer, func() { p.Free(make([]byte, 32)) })

	large := p.Alloc(MaxSmall + 64)
	p.Free(large)
	mustPanic(t, ErrForeignPointer, func() { p.Free(large) })
}

func TestCrossGoroutineFree(t *testing.T) {
	p := New()
	buf := p.Alloc(128)
	for i := range buf {
		buf[i] = byte(i)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i, b := range buf {
			if b != byte(i) {
				t.Errorf("byte %d = %#x", i, b)
				return
			}
		}
		p.Free(buf)
	}()
	<-done
	p.Flush()
	if p.Stats().Outstanding != 0 {
		t.Fatalf("outstanding = %d", p.Stats().Outstanding)
	}
}

func TestReuseAcrossSizeClasses(t *testing.T) {
	p := New()
	big := p.Alloc(MaxSmall)
	if len(big) != MaxSmall {
		t.Fatalf("len = %d", len(big))
	}
	p.Free(big)
	p.Flush()
	before := p.Stats()
	if before.LiveRegions != 1 || before.ActiveSpans != 0 || before.ReleasedRegions != 0 {
		t.Fatalf("after releasing the large block: %+v", before)
	}

	small := make([][]byte, 100)
	for i := range small {
		small[i] = p.Alloc(32)
		small[i][0] = 1
	}
	mid := p.Stats()
	if mid.LiveRegions != 1 {
		t.Fatalf("small allocs grew the pool to %d regions, want the split span", mid.LiveRegions)
	}
	if mid.ActiveSpans != 1 || mid.CachedFreeSpans != 1 {
		t.Fatalf("split did not leave one active span and one idle tail: %+v", mid)
	}
	for _, buf := range small {
		p.Free(buf)
	}
	p.Flush()
	after := p.Stats()
	if after.LiveRegions != 1 || after.ActiveSpans != 0 || after.ReleasedRegions != 0 {
		t.Fatalf("coalesce did not rebuild the original region: %+v", after)
	}
	if after.Outstanding != 0 || after.HeldBytes != 0 {
		t.Fatalf("bytes still held: %+v", after)
	}
	if err := p.pages.audit(); err != nil {
		t.Fatal(err)
	}
}

func TestIdleRegionsAreBounded(t *testing.T) {
	p := New()
	const blocksPerSpan = (spanPages * pageSize) / 32
	const spans = maxCachedSpansPerPages + 1
	bufs := make([][]byte, blocksPerSpan*spans)
	for i := range bufs {
		bufs[i] = p.Alloc(32)
		bufs[i][0] = byte(i)
	}
	created := p.Stats()
	if created.LiveRegions != spans {
		t.Fatalf("created %d regions, want %d", created.LiveRegions, spans)
	}
	for _, buf := range bufs {
		p.Free(buf)
	}
	p.Flush()
	stats := p.Stats()
	if stats.Outstanding != 0 || stats.ActiveSpans != 0 {
		t.Fatalf("blocks still live: %+v", stats)
	}
	if stats.LiveRegions > maxCachedSpansPerPages {
		t.Fatalf("retained %d regions, cap is %d", stats.LiveRegions, maxCachedSpansPerPages)
	}
	if stats.ReleasedRegions < 1 {
		t.Fatalf("nothing was released: %+v", stats)
	}
	if err := p.pages.audit(); err != nil {
		t.Fatal(err)
	}

	// A later wave must be served from the cached regions, not from new ones.
	retained := stats.LiveRegions
	released := stats.ReleasedRegions
	again := make([][]byte, blocksPerSpan)
	for i := range again {
		again[i] = p.Alloc(32)
	}
	if got := p.Stats().LiveRegions; got != retained {
		t.Fatalf("reuse allocated new regions: live %d, was %d", got, retained)
	}
	for _, buf := range again {
		p.Free(buf)
	}
	p.Flush()
	if got := p.Stats().ReleasedRegions; got != released {
		t.Fatalf("reuse released more regions: %d, was %d", got, released)
	}
}

func TestConcurrentAllocFree(t *testing.T) {
	p := New()
	const goroutines = 8
	const ops = 2000
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
			held := make([][]byte, 0, 64)
			for i := 0; i < ops; i++ {
				n := (r.IntN(2048) + 1) * 8
				if r.IntN(32) == 0 {
					n = MaxSmall + r.IntN(4096) + 1
				}
				buf := p.Alloc(n)
				fill(buf, byte(n))
				held = append(held, buf)
				if len(held) > 0 && r.IntN(100) < 75 {
					k := r.IntN(len(held))
					checkPrefix(t, held[k])
					p.Free(held[k])
					held[k] = held[len(held)-1]
					held = held[:len(held)-1]
				}
			}
			for _, buf := range held {
				checkPrefix(t, buf)
				p.Free(buf)
			}
		}(uint64(g + 1))
	}
	wg.Wait()
	p.Flush()
	stats := p.Stats()
	if stats.Outstanding != 0 || stats.HeldBytes != 0 || stats.ActiveSpans != 0 {
		t.Fatalf("pool not quiescent: %+v", stats)
	}
	if err := p.pages.audit(); err != nil {
		t.Fatal(err)
	}
}

func TestStressSequential(t *testing.T) {
	p := New()
	r := rand.New(rand.NewPCG(1, 2))
	held := make([][]byte, 0, 256)
	for i := 0; i < 4000; i++ {
		n := r.IntN(MaxSmall) + 1
		buf := p.Alloc(n)
		fill(buf, byte(n))
		held = append(held, buf)
		if len(held) > 128 {
			k := r.IntN(len(held))
			checkPrefix(t, held[k])
			p.Free(held[k])
			held[k] = held[len(held)-1]
			held = held[:len(held)-1]
		}
	}
	for _, buf := range held {
		checkPrefix(t, buf)
		p.Free(buf)
	}
	p.Flush()
	if err := p.pages.audit(); err != nil {
		t.Fatal(err)
	}
	if p.Stats().Outstanding != 0 {
		t.Fatalf("outstanding = %d", p.Stats().Outstanding)
	}
}

func FuzzAllocFree(f *testing.F) {
	f.Add(0)
	f.Add(1)
	f.Add(8)
	f.Add(257)
	f.Add(4097)
	f.Add(MaxSmall)
	f.Add(MaxSmall + 1)
	p := New()
	f.Fuzz(func(t *testing.T, n int) {
		if n < 0 {
			n = -n
		}
		n %= 1 << 20
		buf := p.Alloc(n)
		if n == 0 {
			p.Free(buf)
			return
		}
		if len(buf) != n {
			t.Fatalf("len %d, want %d", len(buf), n)
		}
		buf[0] = 0x5A
		if len(buf) > 1 {
			buf[len(buf)-1] = 0xA5
		}
		if buf[0] != 0x5A {
			t.Fatal("first byte lost")
		}
		p.Free(buf)
	})
}

func TestFlushIsIdempotent(t *testing.T) {
	p := New()
	buf := p.Alloc(64)
	p.Free(buf)
	p.Flush()
	p.Flush()
	if err := p.pages.audit(); err != nil {
		t.Fatal(err)
	}
	if p.Stats().ActiveSpans != 0 || p.Stats().Outstanding != 0 {
		t.Fatalf("flush left blocks live: %+v", p.Stats())
	}
}

func TestDefaultPoolPackageAPI(t *testing.T) {
	// The package-level Alloc and Free stand in for the C++ static
	// MemoryPool::allocate and deallocate and share one process-wide pool.
	// Deltas are used because the default pool outlives this test.
	if Default() != Default() {
		t.Fatal("Default returned two different pools")
	}
	before := Default().Stats()

	buf := Alloc(48)
	if len(buf) != 48 {
		t.Fatalf("Alloc(48) len = %d", len(buf))
	}
	if size, _ := BlockSize(48); cap(buf) != size {
		t.Fatalf("Alloc(48) cap = %d, want class %d", cap(buf), size)
	}
	for i := range buf {
		buf[i] = byte(i)
	}
	mid := Default().Stats()
	if mid.SmallAllocs != before.SmallAllocs+1 || mid.Outstanding != before.Outstanding+1 {
		t.Fatalf("default pool did not record the allocation: %+v", mid)
	}

	Free(buf)
	after := Default().Stats()
	if after.Frees != before.Frees+1 || after.Outstanding != before.Outstanding {
		t.Fatalf("default pool did not record the free: %+v", after)
	}

	// A large request goes through the same package-level entry points.
	big := Alloc(MaxSmall + 128)
	if len(big) != MaxSmall+128 {
		t.Fatalf("large Alloc len = %d", len(big))
	}
	Free(big)
	if got := Default().Stats(); got.Outstanding != before.Outstanding {
		t.Fatalf("large round trip left %d outstanding", got.Outstanding-before.Outstanding)
	}
}

func fill(buf []byte, value byte) {
	limit := len(buf)
	if limit > 64 {
		limit = 64
	}
	for i := 0; i < limit; i++ {
		buf[i] = value
	}
}

func checkPrefix(t *testing.T, buf []byte) {
	t.Helper()
	if len(buf) == 0 {
		t.Fatal("empty block")
	}
	want := buf[0]
	limit := len(buf)
	if limit > 64 {
		limit = 64
	}
	for i := 0; i < limit; i++ {
		if buf[i] != want {
			t.Fatalf("byte %d = %#x, want %#x", i, buf[i], want)
		}
	}
}

func mustPanic(t *testing.T, target error, fn func()) {
	t.Helper()
	defer func() {
		t.Helper()
		value := recover()
		if value == nil {
			t.Fatal("expected a panic")
		}
		if target == nil {
			return
		}
		err, ok := value.(error)
		if !ok || !errors.Is(err, target) {
			t.Fatalf("panic %v, want %v", value, target)
		}
	}()
	fn()
}
