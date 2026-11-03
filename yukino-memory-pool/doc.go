// Package mempool is a concurrent size-class allocator for small byte buffers.
//
// # Overview
//
// A [Pool] serves requests up to [MaxSmall] (256 KiB) from three cooperating
// caches, which is the structure of the C++ memory pool this package is ported
// from:
//
//   - a sharded cache, one mutex per processor, that satisfies the fast path
//     in batches;
//   - a central cache, with one mutex per size class, that refills those
//     shards and takes idle blocks back;
//   - a page cache that owns the underlying storage, splits and coalesces
//     contiguous spans, and drops spans the pool no longer needs so the
//     garbage collector can reuse them.
//
// Requests larger than [MaxSmall] are allocated with make and retained by the
// pool until [Pool.Free]. Alloc(0) returns a non-nil empty slice and does not
// consume a block.
//
// # Size classes
//
// Blocks are rounded up to a size class. The classes step by 8 bytes through
// 256 B, then by 64 B, 512 B and finally one page, instead of keeping a
// distinct class for every 8-byte size up to 256 KiB. The returned slice has
// length equal to the request and capacity equal to the class, so the caller
// may use the rounded tail without extending into the neighbouring block.
//
// # Concurrency
//
// A Pool is safe for concurrent Alloc and Free. A single slice must be freed
// at most once, and only by passing a slice that still starts at the address
// Alloc returned. Free panics if the pointer is foreign, addresses the middle
// of a block, or is freed twice. The log/slog default logger records the
// rejection before the panic.
//
// # Garbage collection
//
// Backing storage is ordinary Go memory, so abandoning a Pool collects it.
// The bytes themselves are not scanned for pointers: a block must not be the
// only place a Go pointer is stored, or the collector can free the referent
// while the block still holds the address. Small blocks also stay reachable
// from the pool until they are freed and their span is released, which is the
// point of the cache.
//
// # Relationship to the C++ original
//
// This package is a Go port of memory-pool v3 by 程序员Carl
// (https://github.com/youngyangyang04/memory-pool). The upstream tree does not
// ship a licence file. Behaviour that was undefined or simply wrong in that
// tree is fixed here; see the "Migration notes" section of the README.
package mempool
