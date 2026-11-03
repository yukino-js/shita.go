# yukino-memory-pool

A concurrent size-class allocator for small byte slices, ported from the v3
C++ memory pool in `memory-pool/v3`. Requests up to 256 KiB are rounded up to
a size class and satisfied from three caches. Larger requests are ordinary
`make` slices retained by the pool until `Free`.

```go
pool := yukino_mempool.New()
buf := pool.Alloc(24) // len 24, cap 32
copy(buf, []byte("payload"))
pool.Free(buf)
```

Import path: `github.com/yukino-js/brave/yukino-memory-pool`.

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

## Migration notes

The v3 C++ pool is a three-level size-class allocator (thread cache, central
cache, page cache) with an 8-byte minimum and a 256 KiB cutoff. That
structure is unchanged. The following defects in that tree are fixed here
rather than reproduced.

1. **Free-list length underflow.** `ThreadCache::allocate` decremented
   `freeListSize_` before testing the list. On an empty list the `size_t`
   counter wrapped to its maximum, so the cache believed it held a huge
   number of blocks. The return path then tried to keep a quarter of that
   fictional length, walked off the real list, and returned nothing. Thread
   caches never recycled memory to the central cache. The Go cache uses the
   slice length as the counter, and both move together.

2. **Batch count disagreed with the blocks actually returned.** `getBatchNum`
   used `sizeof(1)` as its lower bound. That expression is 4, not 1, so
   classes that should move one block at a time asked for four. On the
   page-cache path `fetchRange` also left the caller's `batchNum` unchanged
   when the span was shorter than the request, and the thread cache then
   linked a single block as if the rest of the batch existed. `batchFor`
   floors at 1, and the central cache returns exactly the requested batch,
   growing another span when the current one runs out.

3. **The return path passed a byte count where a block count was required.**
   `returnToCentralCache` called `returnRange(start, returnNum * alignedSize, index)`
   while `returnRange` treated the second argument as a maximum number of
   nodes. It only stopped because the list happened to be null-terminated.
   The Go central cache receives the blocks themselves.

4. **`deallocate(nullptr, small)` wrote through the null pointer.** `free`
   defines `free(nullptr)` as a no-op. `Free(nil)` and `Free` of a
   zero-capacity slice are no-ops.

5. **Thread-local blocks leaked when the thread exited.** There was no
   destructor. `Flush` returns every shard, and dropping the pool drops the
   shards with it because they are part of the pool value. Cached blocks are
   not stored in a place the garbage collector can discard on its own.

6. **Page-cache deallocation was dead, and unsafe if called.** Nothing called
   `deallocateSpan`. The routine merged only with the following span, used
   `map::operator[]` (which inserts an empty entry), could leave a null list
   head in the map, and did not register a split remainder in `spanMap_`.
   The next allocation of that size would then dereference the null head.
   Idle spans are indexed by base and by end. A null head is never inserted.
   Remainders are tracked, and coalescing walks both neighbours, repeatedly,
   but only inside one `make`. Two separate allocations are never merged,
   even if the runtime happens to place them next to each other.

7. **Mapped pages were never returned.** `mmap` had no `munmap` path, so the
   high-water mark was the resident set for the life of the process. A fully
   free span is coalesced and, once the idle-region caps above are exceeded,
   dropped for the collector.

8. **Fresh anonymous memory was zeroed twice, and recycled blocks were not
   zeroed at all.** `MAP_ANONYMOUS` pages are already zero-filled, so the
   `memset` was pure cost, while a later caller could read the previous
   caller's bytes. `make` provides the fresh zeroes. `Alloc` clears a block
   again when it is handed out, including one that has been recycled.

9. **32,768 size classes.** `FREE_LIST_SIZE` was `MAX_BYTES / ALIGNMENT`, and
   the comment justifying that by the width of `void*` is wrong: the
   alignment and the pointer width are independent. Every thread carried two
   arrays of 32,768 entries (about 512 KiB of metadata) and the central cache
   carried the same number of atomic lists and spinlocks. The geometric
   table above is 204 classes.

10. **`std::atomic_flag` was cleared without being initialised.** In C++17
    the default constructor of `atomic_flag` does not clear it;
    `ATOMIC_FLAG_INIT` was required. The build asked for C++17. Per-class
    `sync.Mutex` values need no such ceremony.

11. **A fully free span could not be reused for another size class.** Blocks
    stayed on the central free list forever, so pages carved into 32-byte
    blocks could never satisfy a 64-byte request. A span whose every block
    has come back is unpublished and returned to the page cache, which may
    hand it to a different class or coalesce it with its neighbours.

12. **`BlockHeader` was unused.** Ownership is the span plus a per-block bit,
    not a header the allocator never wrote. `Free` therefore does not take a
    size: the address identifies the block. Passing the wrong size to the
    C++ `deallocate` put the block on the wrong free list.

`std::atomic_flag` spinlocks were also replaced with `sync.Mutex`. The
central free lists were already mutated only while the per-class lock was
held, so the extra atomics did not make those lists safe on their own.

## Tests

```
go test ./...
go test -race ./...
go test -fuzz=FuzzAllocFree -fuzztime=10s
go test -bench=. -benchmem
go run ./cmd/yukino_mempool-demo
```

The demo follows the C++ performance driver (small loop, concurrent mix,
mixed sizes) and prints `make` both with and without a forced collection.
The runtime allocator is heavily tuned for the sizes this pool serves; the
demo is a comparison, not a claim that the pool is faster than `make`.
