package mempool_test

import (
	"fmt"

	mempool "github.com/yukino-js/brave/yukino-memory-pool"
)

func ExamplePool() {
	pool := mempool.New()
	buf := pool.Alloc(5)
	copy(buf, "hello")
	fmt.Printf("%s %d %d\n", buf, len(buf), cap(buf))
	pool.Free(buf)
	// Output:
	// hello 5 8
}
