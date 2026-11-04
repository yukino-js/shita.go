# yukino-memory-pool

A concurrent size-class allocator for small byte slices. Requests up to 256 KiB are rounded up to a size class and satisfied from three caches. Larger requests are ordinary
`make` slices retained by the pool until `Free`.

```go
pool := yukino_mempool.New()
buf := pool.Alloc(24) // len 24, cap 32
copy(buf, []byte("payload"))
pool.Free(buf)
```

Import path: `github.com/yukino-js/shita.go/yukino-memory-pool`.

The package-level `Alloc` and `Free` use one shared default pool and stand in
for the C++ static `MemoryPool::allocate` and `MemoryPool::deallocate`:

```go
buf := yukino_mempool.Alloc(24)
yukino_mempool.Free(buf)
```

Prefer `New` when a caller wants a pool it can flush, inspect with `Stats`, or
drop on its own; everything that shares the default pool shares its caches and
its counters.

## Layout

```
caller
  │  Alloc / Free
  V
sharded cache            one mutex per processor, batches of blocks
  │  refill / flush
  V
central cache            one mutex per size class, partial spans
  │  new span / fully free span
  V
page cache               split, coalesce, drop idle regions to the GC
```

A shard is the stand-in for the original `thread_local` cache. Go does not
provide goroutine-local storage. `sync.Pool` is the usual substitute, but its
private slot is not stolen by other processors and it may drop items at
collection time; a batch left behind there forces the next allocation to cut
a new span. The pool instead keeps a fixed slice of shards, sized to
`GOMAXPROCS` when the pool is created, and every cached block stays reachable
until it is flushed or handed out.

A caller picks its shard from the address of a local variable, which lies on
its own stack. The choice is stable per goroutine and touches nothing shared,
which replaces the shared round-robin counter that otherwise serialised every
`Alloc` and `Free` on one cache line and made the pool slower as callers were
added. Two goroutines can still land on the same shard, so the lock is taken
with `TryLock` and a collision redirects to a neighbouring shard rather than
blocking; blocking is what made the scheduler park and wake goroutines on the
fast path. The allocation counters are plain integers under that same lock, so
a cached `Alloc` or `Free` performs no atomic write at all.

Blocks are carved out of a single `make` of the span, not out of `mmap`.
Dropping the span's slice is what returns the storage to the runtime. The
free list is a stack of indexes into that slice, so the allocator does not
overlay pointers on user memory.

## Size classes

204 classes, shared by the process:

| Request          | Step  |
| ---------------- | ----- |
| 8 B – 256 B      | 8 B   |
| 320 B – 4 KiB    | 64 B  |
| 4.5 KiB – 32 KiB | 512 B |
| 36 KiB – 256 KiB | 4 KiB |

`Alloc` returns `len == n` and `cap == BlockSize(n)`. The capacity stops at
the end of the block, so `append` cannot run into the next block. Bytes past
`len` are zero and belong to the caller until `Free`.

`Alloc(0)` returns a non-nil empty slice and does not take a block. `Free` of
that slice, of `nil`, or of any zero-capacity slice is a no-op.

## Concurrency and lifetime

`Alloc` and `Free` may run from any goroutine. One slice is freed at most
once, and the slice passed to `Free` must still start at the address `Alloc`
returned. `Free(buf[1:])` panics with `ErrInteriorPointer`. A second `Free`,
or a slice the pool does not own, panics with `ErrDoubleFree` or
`ErrForeignPointer`. The rejection is written to the default `slog` logger
before the panic.

The bytes are not scanned by the garbage collector. Do not store the only
copy of a Go pointer inside a block.

Small blocks stay reachable from the pool until they are freed and their span
is released. Large blocks (`n > MaxSmall`) stay reachable until `Free`, which
is the same contract as `malloc`/`free`. Forgetting `Free` on a large block
retains it for the life of the pool.

Idle whole regions are kept up to four spans of a given page count and 1024
pages in total (4 MiB). Beyond that the pool drops the slice so the collector
can reuse it. A remnant of a region that is still partly live is kept,
because releasing it would abandon the rest of the region. `Flush` pushes
every shard back to the central cache immediately; it is safe to call while
other goroutines are allocating. An allocation that has popped a block but
not yet returned it to the caller is not part of that flush.

## Logging

Misuse of `Free` is logged at error level. A region handed back to the
collector is logged at debug level. Enable that with:

```go
slog.SetLogLoggerLevel(slog.LevelDebug)
```

## Tests

```
go test ./...
go test -race ./...
go test -fuzz=FuzzAllocFree -fuzztime=10s
go test -bench=. -benchmem
go run ./cmd/memory-pool
```

The demo follows the C++ performance driver (small loop, concurrent mix,
mixed sizes) and prints `make` both with and without a forced collection.
The runtime allocator is heavily tuned for the sizes this pool serves; the
demo is a comparison, not a claim that the pool is faster than `make`.
