<div align="center">

# shita.go

**Five classic C++ systems, rebuilt in idiomatic Go — same architecture, bugs fixed.**

[![Go](https://img.shields.io/badge/Go-1.26%2B-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![Workspace](https://img.shields.io/badge/Go%20workspace-5%20modules-4A6FA5)](https://go.dev/ref/mod#workspaces)
[![Tests](https://img.shields.io/badge/tests-unit%20%2B%20e2e%20%2B%20race-2EA26A)](#testing)

</div>

---

**shita.go** is a Go workspace collecting five independent re-implementations of
well-known C++ systems projects: an asynchronous logging library, a
tcmalloc-style memory pool, a skip list, a Raft-based key-value store, and a
cloud-storage service.

Each port keeps the control flow of its original so the two codebases can be
read side by side, replaces C++ machinery with Go idioms — threads and fibers
become goroutines, boost serialization becomes `encoding/gob`, hand-rolled TCP
becomes [yukino_rpc](https://github.com/hangtiancheng/yukino.go/tree/main/libs/yukino_rpc) —
and **deliberately fixes the defects of the original** rather than reproducing
them. Every module README carries a full "defects corrected" section.

## Modules

| Module                                             | What it is                                                                                                       | Deps                    |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | ----------------------- |
| [**yukino-log**](./yukino-log)                     | Concurrent logging: synchronous formatting front end, double-buffered async backend, size- and day-rolling files | stdlib                  |
| [**yukino-memory-pool**](./yukino-memory-pool)     | Size-class allocator: sharded -\> central -\> page cache, 204 classes up to 256 KiB                              | stdlib                  |
| [**yukino-skiplist**](./yukino-skiplist)           | Generic concurrent ordered map with atomic dump/load persistence                                                 | stdlib                  |
| [**yukino-kv**](./yukino-kv)                       | Linearizable key-value store on Raft: elections, replication, snapshots, crash recovery                          | yukino_rpc              |
| [**yukino-cloud-storage**](./yukino-cloud-storage) | Async log system + two-tier cloud storage service with web UI, upload agent and remote log backup                | yukino_http, yukino_rpc |

---

## yukino-log

```go
import yukino_log "github.com/yukino-js/shita.go/yukino-log"

backend, _ := yukino_log.NewAsync(yukino_log.AsyncOptions{
    Basename: "logs/server", // → logs/server.YYYYMMDD-HHMMSS.log
    RollSize: 64 << 20,
})
backend.Start()
defer backend.Stop() // drains every accepted record, syncs, exits cleanly

logger := yukino_log.New(yukino_log.Options{MinLevel: yukino_log.LevelInfo})
logger.Attach(backend)
logger.Infof("listening on %s", addr)
```

Producers share one staging buffer and one spare under a mutex; a single
backend goroutine writes and recycles them outside the lock. Records are never
dropped or split, files roll on size and on the calendar day, and `Fatal`
panics instead of aborting so deferred cleanup still runs.

Try it: `cd yukino-log && go run ./cmd/yukino-log`

## yukino-memory-pool

```go
import yukino_mempool "github.com/yukino-js/shita.go/yukino-memory-pool"

pool := yukino_mempool.New()
buf := pool.Alloc(24) // len 24, cap 32 (rounded to a size class)
copy(buf, []byte("payload"))
pool.Free(buf)
```

Requests up to 256 KiB flow through three cooperating caches — a per-processor
sharded cache, a per-size-class central cache, and a page cache that splits and
coalesces spans and returns idle ones to the garbage collector. Shard selection
uses the caller's stack address and `TryLock` redirection, so the fast path
performs no atomic writes and never parks a goroutine.

Try it: `cd yukino-memory-pool && go run ./cmd/memory-pool` (times the pool
against `make`)

## yukino-skiplist

```go
import "github.com/yukino-js/shita.go/yukino-skiplist"

list := skiplist.New[int, string]()
list.Insert(1, "learn")        // added == true
list.Set(1, "study")           // in-place update the C++ original lacked

for key, value := range list.All() {
    fmt.Println(key, value)    // ordered, O(k)
}
```

Generic over any ordered key (or a custom comparator), O(log n) expected
operations, and an `RWMutex` per list so readers never block each other — a
correction of the original, whose lookups ran with no synchronisation at all.
`DumpFile` persists atomically via temp-file + fsync + rename.

Try it: `cd yukino-skiplist && go run ./cmd/skiplist-demo`

## yukino-kv

A distributed, linearizable key-value store on Raft:

- Leader election with randomized timeouts and fast conflict-term backtracking.
- Figure-8-safe commit by `matchIndex` scans; snapshot compaction with
  `InstallSnapshot` catch-up for lagging followers.
- Every read goes through the log; per-client request ids make retries
  idempotent (exactly-once execution over at-least-once delivery).
- Atomic persistence (temp file + `fsync` + rename) and full crash recovery.

```sh
cd yukino-kv
make cluster                 # bootstrap a local 3-node cluster (Ctrl-C stops it)

# in a second terminal:
bin/clerk -f test.conf -put x=hello
bin/clerk -f test.conf -append x=,world
bin/clerk -f test.conf -get x        # "hello,world"
```

## yukino-cloud-storage

An asynchronous, non-blocking logging system grafted onto a small cloud-storage
service:

- **Async log** (`pkg/asynclog`) — level filtering, size-rotated file sinks,
  and `ERROR`/`FATAL` records shipped to a remote backup receiver so they
  survive a crash. Dependency free; embeddable anywhere.
- **Storage service** — upload, list and download files across two tiers:
  _low_ (verbatim) and _deep_ (gzip/zlib/flate bundles), with a built-in web UI.
- **Upload agent** — watches two local directories and uploads new or changed
  files, remembering what it has already sent.

```sh
cd yukino-cloud-storage
make run-backup              # optional: remote ERROR/FATAL receiver
make run-server              # storage server + web UI at http://localhost:8081
make run-agent               # optional: directory-watching upload agent
```

---

## Repository layout

```text
shita.go/
├── go.work                      # workspace tying the five modules together
├── yukino-log/                  # double-buffered concurrent logging library
├── yukino-memory-pool/          # sharded / central / page-cache allocator
├── yukino-skiplist/             # concurrent ordered map + persistence
├── yukino-kv/                   # Raft key-value store
│   ├── raft/                    #   consensus core, persister, RPC types
│   ├── kvserver/                #   linearizable state machine
│   └── clerk/                   #   client library with leader failover
└── yukino-cloud-storage/        # async log + cloud storage service
    ├── cmd/                     #   storaged / storage-agent / logbackupd
    ├── pkg/asynclog/            #   reusable async logging system
    ├── pkg/bundle/              #   self-describing compression container
    ├── pkg/logbackup/           #   RPC backup client + receiver
    └── internal/                #   storage domain, upload agent, web UI
```

## Getting started

Requires **Go 1.26+**. The repository is a
[Go workspace](https://go.dev/ref/mod#workspaces), so build and test commands
name the module directories explicitly:

```sh
git clone https://github.com/yukino-js/shita.go.git
cd shita.go

# build everything
go build ./yukino-log/... ./yukino-memory-pool/... ./yukino-skiplist/... \
         ./yukino-kv/... ./yukino-cloud-storage/...

# or work inside a single module
cd yukino-skiplist && go test ./...
```

`yukino-kv` and `yukino-cloud-storage` additionally ship Makefiles
(`make build`, `make test`, `make race`, …).

## Testing

```sh
go test ./yukino-log/... ./yukino-memory-pool/... ./yukino-skiplist/... \
        ./yukino-kv/... ./yukino-cloud-storage/...
```

The suites include unit tests, benchmarks, `httptest` and in-process RPC tests,
and end-to-end cluster tests that boot real Raft clusters on loopback —
covering leader failover, snapshot compaction and crash-restart recovery.
Everything passes under `-race`.

## C++ → Go, the recurring translations

| C++ original                        | Go replacement                                                                       |
| ----------------------------------- | ------------------------------------------------------------------------------------ |
| `std::thread`, fiber libraries      | goroutines, channels, `select`                                                       |
| `std::mutex` / `condition_variable` | `sync.Mutex`, `sync.RWMutex`, `sync.Cond`                                            |
| boost serialization, protobuf       | `encoding/gob`, `encoding/json`                                                      |
| hand-rolled TCP framing             | [yukino_rpc](https://github.com/hangtiancheng/yukino.go/tree/main/libs/yukino_rpc)   |
| libevent `evhttp`                   | [yukino_http](https://github.com/hangtiancheng/yukino.go/tree/main/libs/yukino_http) |
| `thread_local` caches               | sharded caches picked by stack address                                               |
| `DPrintf` debug macros              | structured `log/slog`                                                                |

## License

`yukino-log` is a derivative of the kamaLog teaching project, which is
distributed under the GNU General Public License v3; that license is unchanged
for the derivative work. The remaining modules currently ship without an
explicit license file — open an issue if you would like to use them.
