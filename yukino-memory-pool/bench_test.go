package yukino_mempool

import (
	"sync/atomic"
	"testing"
	"unsafe"
)

// sinkAddr is stored with an atomic so parallel benchmarks can publish a
// pointer without racing, and so the compiler cannot delete the allocation
// being measured. The uintptr does not itself keep the object alive for the
// collector; the store is enough to keep the allocation in the benchmark.
var sinkAddr atomic.Uintptr

func keep(buf []byte) {
	if len(buf) == 0 {
		return
	}
	sinkAddr.Store(uintptr(unsafe.Pointer(&buf[0])))
}

func BenchmarkPoolSmall(b *testing.B) {
	p := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := p.Alloc(32)
		buf[0] = 1
		p.Free(buf)
		keep(buf)
	}
}

func BenchmarkMakeSmall(b *testing.B) {
	// A 32-byte slice that does not escape is allocated on the stack, so this
	// reports no heap allocation. The demo in cmd/yukino_mempool-demo retains every
	// slice and is the comparison against make.
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 32)
		buf[0] = 1
		keep(buf)
	}
}

func BenchmarkPoolSmallParallel(b *testing.B) {
	p := New()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := p.Alloc(32)
			buf[0] = 1
			p.Free(buf)
			keep(buf)
		}
	})
}

func BenchmarkMakeSmallParallel(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := make([]byte, 32)
			buf[0] = 1
			keep(buf)
		}
	})
}

func BenchmarkPoolMixed(b *testing.B) {
	p := New()
	sizes := []int{16, 32, 64, 128, 256, 512, 1024, 2048}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := p.Alloc(sizes[i%len(sizes)])
		buf[0] = 1
		p.Free(buf)
		keep(buf)
	}
}
