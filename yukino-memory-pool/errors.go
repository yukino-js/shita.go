package yukino_mempool

import "errors"

// Misuse of Free panics with one of these errors, possibly wrapped with the
// address that was rejected. Alloc and Free of a nil pool panic with a plain
// string, because there is no pointer to attribute.
var (
	// ErrDoubleFree reports a block that was already returned, or that was
	// never handed out. The pool cannot tell those two cases apart once the
	// block has been cached.
	ErrDoubleFree = errors.New("double free")

	// ErrForeignPointer reports an address the pool does not own. Typical
	// causes are a slice allocated with make, a slice produced by appending
	// past the capacity Alloc returned, or a slice taken from another Pool.
	ErrForeignPointer = errors.New("pointer was not allocated by this pool")

	// ErrInteriorPointer reports an address that falls inside a live block
	// but is not its first byte. Free the slice Alloc returned, not a
	// subslice that advances the start.
	ErrInteriorPointer = errors.New("pointer does not address the start of a block")
)
