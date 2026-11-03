package mempool

import (
	"fmt"
	"sync"
)

// centralCache is the shared free path for one size class. Sharded caches
// refill from it in batches and return surplus blocks to it. The mutex is
// the only lock that guards the partial-span list and every span stack of
// this class.
type centralCache struct {
	mu      sync.Mutex
	pool    *Pool
	class   int
	partial *span
}

// take appends exactly want blocks to dst, asking the page cache for a new
// span when the partial list runs dry. dst's existing length is discarded by
// the caller (it passes dst[:0]); the capacity is reused.
func (c *centralCache) take(dst [][]byte, want int) [][]byte {
	if want <= 0 {
		return dst
	}
	if len(dst) != 0 {
		panic("mempool: central cache refill requires an empty destination")
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	// Each iteration either produces a block or discards an empty span and
	// allocates another. A span that stays empty across iterations is a bug;
	// the bound turns it into a panic instead of a livelock.
	steps := 0
	limit := want*2 + 8
	for len(dst) < want {
		steps++
		if steps > limit {
			panic("mempool: central cache could not fill the requested batch")
		}
		s := c.partial
		if s == nil {
			s = c.grow()
		}
		if len(s.stack) == 0 {
			c.unlink(s)
			continue
		}
		dst = append(dst, s.pop())
		if len(s.stack) == 0 {
			c.unlink(s)
		}
	}
	return dst
}

// grow publishes a fresh span and puts it at the head of the partial list.
// The caller holds c.mu. Page-cache locks are taken inside allocate and
// publish, never the other way around.
func (c *centralCache) grow() *span {
	block := classSizes[c.class]
	s := c.pool.pages.allocate(pagesFor(block))
	s.prepare(c.class, block)
	c.pool.pages.publish(s)
	c.pushFront(s)
	return s
}

// giveBack returns blocks that a sharded cache no longer wants. Every block
// is pushed onto its own span; a span whose free stack is complete goes back
// to the page cache, where its pages can be reused for a different size
// class. The C++ central cache never did this, so a span carved into 32-byte
// blocks stayed a 32-byte span until the process exited.
func (c *centralCache) giveBack(blocks [][]byte) {
	if len(blocks) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	reclaim := make([]*span, 0, 1)
	queued := make(map[*span]struct{})
	for _, block := range blocks {
		s := c.pool.pages.spanOf(block)
		if s == nil || s.class != c.class {
			addr := uintptr(0)
			if cap(block) > 0 {
				addr = uintptr(sliceData(block))
			}
			panic(fmt.Sprintf("mempool: returned block %#x is not owned by size class %d", addr, c.class))
		}
		if err := s.push(block); err != nil {
			c.pool.fault(err, sliceData(block))
		}
		if len(s.stack) == s.total {
			c.unlink(s)
			if _, ok := queued[s]; !ok {
				queued[s] = struct{}{}
				reclaim = append(reclaim, s)
			}
			continue
		}
		if !s.onPartial {
			c.pushFront(s)
		}
	}
	for _, s := range reclaim {
		c.pool.pages.reclaim(s)
	}
}

func (c *centralCache) unlink(s *span) {
	if !s.onPartial {
		return
	}
	if s.prev != nil {
		s.prev.next = s.next
	} else {
		c.partial = s.next
	}
	if s.next != nil {
		s.next.prev = s.prev
	}
	s.next = nil
	s.prev = nil
	s.onPartial = false
}

func (c *centralCache) pushFront(s *span) {
	if s.onPartial {
		return
	}
	s.prev = nil
	s.next = c.partial
	if c.partial != nil {
		c.partial.prev = s
	}
	c.partial = s
	s.onPartial = true
}
