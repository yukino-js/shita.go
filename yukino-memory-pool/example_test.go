package yukino_mempool_test

import (
	"fmt"

	yukino_mempool "github.com/yukino-js/brave/yukino-memory-pool"
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
