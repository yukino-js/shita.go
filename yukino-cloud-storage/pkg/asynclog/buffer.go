package asynclog

// Growth controls how a Buffer expands when it runs out of writable space.
//
// While the buffer is smaller than Threshold it grows multiplicatively by
// doubling; once it reaches Threshold it grows additively by Linear bytes. This
// mirrors the policy of the original C++ implementation while guaranteeing that
// every growth step makes forward progress.
type Growth struct {
	Base      int64 // initial capacity in bytes
	Threshold int64 // switch from doubling to linear growth at this size
	Linear    int64 // additive increment applied once Threshold is reached
}

// DefaultGrowth returns a sensible growth policy.
func DefaultGrowth() Growth {
	return Growth{Base: 1 << 20, Threshold: 16 << 20, Linear: 1 << 20}
}

// normalize fills zero fields with defaults and enforces sane minimums.
func (g Growth) normalize() Growth {
	if g.Base <= 0 {
		g.Base = 1 << 20
	}
	if g.Threshold <= 0 {
		g.Threshold = 16 << 20
	}
	if g.Linear <= 0 {
		g.Linear = 1 << 20
	}
	if g.Threshold < g.Base {
		g.Threshold = g.Base
	}
	return g
}

// next returns the capacity that should follow a buffer of size cur.
func (g Growth) next(cur int64) int64 {
	if cur < g.Threshold {
		return cur * 2
	}
	return cur + g.Linear
}

// Buffer is an append-only byte buffer with independent read and write cursors.
//
// Two buffers are swapped between a producer and a consumer by the async
// Worker. A buffer only ever grows and is never shrunk, so its memory is reused
// across swaps instead of being reallocated on every flush.
//
// Buffer is not safe for concurrent use; the Worker serializes access through
// its own mutex.
type Buffer struct {
	data     []byte
	writePos int
	readPos  int
	growth   Growth
}

// NewBuffer returns an empty buffer governed by the supplied growth policy.
func NewBuffer(g Growth) *Buffer {
	g = g.normalize()
	return &Buffer{data: make([]byte, 0, g.Base), growth: g}
}

// Len returns the number of readable bytes.
func (b *Buffer) Len() int { return b.writePos - b.readPos }

// IsEmpty reports whether there is nothing to read.
func (b *Buffer) IsEmpty() bool { return b.writePos == b.readPos }

// Bytes returns the readable slice. The slice is only valid until the next
// mutation of the buffer.
func (b *Buffer) Bytes() []byte { return b.data[b.readPos:b.writePos] }

// Cap returns the total capacity of the buffer in bytes.
func (b *Buffer) Cap() int { return cap(b.data) }

// writeable returns the number of bytes that can be appended without growing.
func (b *Buffer) writeable() int { return cap(b.data) - b.writePos }

// Push appends p to the buffer, growing it as required.
func (b *Buffer) Push(p []byte) {
	b.ensure(len(p))
	b.writePos += copy(b.data[b.writePos:cap(b.data)], p)
}

// ensure guarantees room for at least extra more bytes.
//
// Unlike the original implementation, which applied a single fixed-size bump
// that could still be too small, this loop keeps growing until the request
// fits, so Push can never overflow the backing array.
func (b *Buffer) ensure(extra int) {
	if b.writeable() >= extra {
		return
	}
	// Reclaim space already consumed by the reader before allocating.
	if b.readPos > 0 {
		moved := copy(b.data, b.data[b.readPos:b.writePos])
		b.writePos = moved
		b.readPos = 0
		if b.writeable() >= extra {
			return
		}
	}
	need := int64(b.writePos + extra)
	newCap := int64(cap(b.data))
	for newCap < need {
		next := b.growth.next(newCap)
		if next <= newCap {
			// Guarantee forward progress even for a pathological policy.
			next = need
		}
		newCap = next
	}
	grown := make([]byte, newCap)
	copy(grown, b.data[:b.writePos])
	b.data = grown
}

// Swap exchanges the backing storage and cursors of b and other. After a swap
// each buffer owns the memory the other one used, which is what lets the worker
// recycle buffers without allocating.
func (b *Buffer) Swap(other *Buffer) {
	b.data, other.data = other.data, b.data
	b.writePos, other.writePos = other.writePos, b.writePos
	b.readPos, other.readPos = other.readPos, b.readPos
	b.growth, other.growth = other.growth, b.growth
}

// Reset discards all data while keeping the allocated memory.
func (b *Buffer) Reset() {
	b.writePos = 0
	b.readPos = 0
}
