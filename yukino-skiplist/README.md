# yukino-skiplist

Concurrent ordered map implemented as a skip list. It keeps the original
operations — insert, delete, lookup, ordered display, dump, load, and size —
adds the in-place update the C++ type was missing, and corrects the defects
listed below.

The package requires Go 1.26 or later. It uses only the standard library.

## Install

```bash
go get github.com/yukino-js/shita.go/yukino-skiplist
```

## Usage

```go
list := skiplist.New[int, string]()
list.Insert(1, "learn")          // added == true
list.Insert(1, "ignored")        // added == false; the stored value is unchanged
previous, existed := list.Set(1, "study")
value, ok := list.Get(1)

for key, value := range list.All() {
    fmt.Println(key, value)
}
```

`Insert` matches the C++ insert: an existing key is left untouched. `Set`
replaces it and reports the previous value. Keys are ordered with
`cmp.Ordered`, or with a comparator passed to `NewWithComparator`.

`All`, `Range`, `RangeFrom`, `Keys`, and `Values` hold the shared lock for the
whole iteration. The loop body must not call back into the same list. Take a
`Snapshot` first when the body needs to mutate the list.

A nil or zero `SkipList` panics. Values are returned as stored; a slice or map
value is not deep-copied.

## Persistence

`Dump` and `DumpFile` write one record per line:

```text
<key>:<value>\n
```

`\`, the separator, `\n`, and `\r` inside a field are escaped. The reader
splits on the first separator that is not itself escaped, so a key such as
`a:b` round-trips. `Dump` copies the entries under the shared lock and releases
that lock before encoding or writing. `DumpFile` writes a temporary file in the
destination directory, flushes it, and renames it over the target.

`Load` and `LoadFile` build a new list. A malformed line, a bad escape, a codec
error, or a duplicate key aborts the load and discards the partial result.
Blank lines are ignored. Codecs cover `string`, `bool`, integers, `float64`,
`[]byte`, and any type that implements `encoding.TextMarshaler`.

A dump produced by the C++ program still loads when its fields contain no
backslash and its keys contain no separator. The value is everything after the
first separator, which is how that loader split the line.

## Concurrency

Each list has its own `sync.RWMutex`. Lookups and iteration take the shared
lock, so readers do not block each other. Insert, set, delete, and clear take
the exclusive lock. The default random source is `math/rand/v2`, which is safe
for concurrent use. A source installed with `WithRandSource` is read only while
the exclusive lock is held; do not share a source that is not itself
goroutine-safe between two lists.

## Layout

| Path                                  | Role                                                       |
| ------------------------------------- | ---------------------------------------------------------- |
| `skiplist.go`                         | List, insert, lookup, delete                               |
| `iterate.go`                          | Ordered iteration, floor, ceiling, snapshot                |
| `persist.go`, `record.go`, `codec.go` | Text dump and load                                         |
| `cmd/skiplist-demo`                   | Driver corresponding to `main.cpp`, plus a concurrent step |

## Tests

```bash
go test -race -count=1 ./...
go test -bench=. -benchmem ./...
go run ./cmd/skiplist-demo -workers 4 -ops 1000
```

## License

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version. See `LICENSE`.

Copyright (C) 2026 the yukino-skiplist authors
