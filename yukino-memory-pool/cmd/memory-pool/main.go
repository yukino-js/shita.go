// Command memory-pool times the pool against make:
// a small-object loop, a concurrent mix, and a mixed-size loop.
//
// make does not free inside the loop. Its bytes stay reachable until the
// slice is dropped, and the collector reclaims them later. The pool's Free
// returns a block before the call returns, so the two "loop" columns are not
// the same kind of work. The make column is printed twice, once for the loop
// alone and once with a full GC after it, so the deferred cost is visible.
package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"time"

	yukino_mempool "github.com/yukino-js/shita.go/yukino-memory-pool"
)

func main() {
	small := flag.Int("small", 100_000, "iterations of the 32-byte loop")
	workers := flag.Int("workers", 4, "goroutines in the concurrent loop")
	ops := flag.Int("ops", 25_000, "allocations per goroutine in the concurrent loop")
	mixed := flag.Int("mixed", 50_000, "iterations of the mixed-size loop")
	flag.Parse()

	fmt.Println("yukino-memory-pool")
	fmt.Println("warmup")
	warmup()

	section("Small allocations")
	fmt.Printf("%d x 32 bytes, every fourth freed immediately\n", *small)
	report("pool", timeSmallPool(*small))
	report("make", timeSmallMake(*small, false))
	report("make+GC", timeSmallMake(*small, true))

	section("Concurrent allocations")
	fmt.Printf("%d goroutines x %d allocations, sizes 8..256\n", *workers, *ops)
	report("pool", timeConcurrent(true, *workers, *ops))
	report("make", timeConcurrent(false, *workers, *ops))

	section("Mixed sizes")
	fmt.Printf("%d allocations from 16..2048 bytes\n", *mixed)
	report("pool", timeMixed(true, *mixed))
	report("make", timeMixed(false, *mixed))
}

func section(title string) {
	fmt.Printf("\n%s\n", title)
}

func report(label string, elapsed time.Duration) {
	fmt.Printf("  %-8s %8.3f ms\n", label, float64(elapsed.Microseconds())/1000)
}

func warmup() {
	pool := yukino_mempool.New()
	held := make([][]byte, 0, 5000)
	for i := 0; i < 1000; i++ {
		for _, size := range []int{32, 64, 128, 256, 512} {
			buf := pool.Alloc(size)
			buf[0] = 1
			held = append(held, buf)
		}
	}
	for _, buf := range held {
		pool.Free(buf)
	}
	pool.Flush()
}

func timeSmallPool(n int) time.Duration {
	pool := yukino_mempool.New()
	held := make([][]byte, 0, n)
	start := time.Now()
	for i := 0; i < n; i++ {
		buf := pool.Alloc(32)
		buf[0] = 1
		held = append(held, buf)
		if i%4 == 0 {
			pool.Free(held[len(held)-1])
			held = held[:len(held)-1]
		}
	}
	for _, buf := range held {
		pool.Free(buf)
	}
	return time.Since(start)
}

func timeSmallMake(n int, collect bool) time.Duration {
	held := make([][]byte, 0, n)
	start := time.Now()
	for i := 0; i < n; i++ {
		buf := make([]byte, 32)
		buf[0] = 1
		held = append(held, buf)
		if i%4 == 0 {
			held = held[:len(held)-1]
		}
	}
	held = nil
	if collect {
		runtime.GC()
	}
	return time.Since(start)
}

func timeConcurrent(usePool bool, workers, ops int) time.Duration {
	pool := yukino_mempool.New()
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, seed))
			held := make([][]byte, 0, ops)
			for i := 0; i < ops; i++ {
				size := r.IntN(249) + 8
				var buf []byte
				if usePool {
					buf = pool.Alloc(size)
				} else {
					buf = make([]byte, size)
				}
				buf[0] = 1
				held = append(held, buf)
				if r.IntN(100) < 75 {
					k := r.IntN(len(held))
					if usePool {
						pool.Free(held[k])
					}
					held[k] = held[len(held)-1]
					held = held[:len(held)-1]
				}
			}
			if usePool {
				for _, buf := range held {
					pool.Free(buf)
				}
			}
		}(uint64(w + 1))
	}
	wg.Wait()
	return time.Since(start)
}

func timeMixed(usePool bool, n int) time.Duration {
	pool := yukino_mempool.New()
	sizes := []int{16, 32, 64, 128, 256, 512, 1024, 2048}
	r := rand.New(rand.NewPCG(7, 11))
	held := make([][]byte, 0, n)
	start := time.Now()
	for i := 0; i < n; i++ {
		size := sizes[r.IntN(len(sizes))]
		var buf []byte
		if usePool {
			buf = pool.Alloc(size)
		} else {
			buf = make([]byte, size)
		}
		buf[0] = 1
		held = append(held, buf)
		if i%100 == 0 && len(held) > 0 {
			release := len(held)
			if release > 20 {
				release = 20
			}
			for j := 0; j < release; j++ {
				last := held[len(held)-1]
				if usePool {
					pool.Free(last)
				}
				held = held[:len(held)-1]
			}
		}
	}
	if usePool {
		for _, buf := range held {
			pool.Free(buf)
		}
	}
	return time.Since(start)
}
