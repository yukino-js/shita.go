package yukino_mempool_test

import (
	"fmt"

	yukino_mempool "github.com/yukino-js/shita.go/yukino-memory-pool"
)

func ExamplePool() {
	pool := yukino_mempool.New()
	buf := pool.Alloc(5)
	copy(buf, "hello")
	fmt.Printf("%s %d %d\n", buf, len(buf), cap(buf))
	pool.Free(buf)
	// Output:
	// hello 5 8
}

// ExampleAlloc shows the package-level entry points, which use the shared
// default pool and stand in for the C++ static MemoryPool::allocate and
// MemoryPool::deallocate.
func ExampleAlloc() {
	buf := yukino_mempool.Alloc(3)
	copy(buf, "abc")
	fmt.Printf("%s %d %d\n", buf, len(buf), cap(buf))
	yukino_mempool.Free(buf)
	// Output:
	// abc 3 8
}
