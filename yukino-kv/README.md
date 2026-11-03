# yukino-kv

A distributed, linearizable key-value store built on the Raft consensus
algorithm, written in idiomatic Go.

yukino-kv is a port of the Cpp project to Go. The control flow of the original is preserved so the two
implementations can be read side by side, while C++ threads and the bundled
fiber library become goroutines, the custom mprpc framework becomes
[yukino_rpc](https://github.com/hangtiancheng/yukino.go/tree/main/libs/yukino_rpc),
boost serialization becomes `encoding/gob`, and the skip-list storage engine
becomes a mutex-guarded Go map. Several real defects of the C++ original —
including a persistence layer that destroyed the very state it was supposed
to protect — are fixed here; see
[Defects fixed from the C++ original](#defects-fixed-from-the-c-original).

## Features

- **Raft consensus** — leader election with randomized timeouts, log
  replication with the fast conflict-term backtracking optimization, and
  commit-by-majority through `matchIndex` scans (Raft Figure 8 safe:
  only current-term entries are committed by count).
- **Log compaction** — the KV layer snapshots the database and deduplication
  table once the persisted Raft state grows past a configurable threshold;
  lagging followers catch up through `InstallSnapshot`.
- **Linearizable client semantics** — every operation (reads included) goes
  through the Raft log; per-client request ids make retries idempotent, so
  at-least-once delivery from the clerk still yields exactly-once execution.
- **Crash recovery** — Raft state and snapshots are written atomically
  (temporary file + `fsync` + rename) and reloaded on startup, so a node
  rejoins the cluster with its term, vote, log and database intact.
- **Graceful lifecycle** — `SIGINT`/`SIGTERM` shuts a node (or a whole
  bootstrapped cluster) down cleanly: goroutines exit, in-flight RPC
  handlers drain, connections close.
- **Observability** — structured logging through `log/slog` with per-node
  and per-component attributes; `-v` enables debug level.

## Architecture

```
                    Get / PutAppend (yukino_rpc, JSON codec)
   ┌────────┐      ┌──────────────────────────────────────────────┐
   │ clerk  ├─────►│                 kvserver node                │
   │ client │◄─────┤                                              │
   └────────┘      │  ┌───────────┐  applyChan   ┌────────────┐  │
                   │  │ KVServer  │◄─────────────┤    Raft    │  │
                   │  │  state    │   Start(op)  │  consensus │  │
                   │  │  machine  ├─────────────►│    core    │  │
                   │  └─────┬─────┘              └───────────┘  │
                   │        │ Snapshot(db)              │ persist │
                   │        ▼                           ▼         │
                   │   gob snapshot                Persister      │
                   │   (db + dedup table)     raftstatePersistN   │
                   │                          snapshotPersistN    │
                   └──────────────────────────────────────────────┘
                        ▲   RequestVote / AppendEntries /
                        │   InstallSnapshot (yukino_rpc)
                        ▼
                    raft peers
```

| C++ module            | Go counterpart        | Notes                                        |
| --------------------- | --------------------- | -------------------------------------------- |
| `src/raftCore/raft`   | `raft/`               | goroutines replace fibers and `std::thread`  |
| `src/raftCore/kvServer` | `kvserver/`         | skip list replaced by `map[string]string`    |
| `src/raftCore/Persister` | `raft/persister.go` | atomic writes; loads state instead of truncating |
| `src/raftClerk`       | `clerk/`              | paced retries, `crypto/rand` client ids      |
| `src/raftRpcPro` (protobuf) | Go structs      | JSON-encoded by yukino_rpc; gob inside log entries |
| `src/rpc` (mprpc)     | yukino_rpc            | lazy connection pools, no startup barriers   |
| `src/fiber`           | Go runtime            | goroutines, channels, `select`               |
| `src/skipList`        | `map` + `sync.Mutex`  | the original README treats the engine as replaceable |
| `src/common` (config/util) | `config/`, `log/slog` | structured logging replaces `DPrintf`   |

## Repository layout

```
.
├── cmd/
│   ├── kvserver/       # node binary; also bootstraps whole local clusters
│   └── clerk/          # demo client binary
├── raft/               # Raft consensus core, persistence, RPC types
├── kvserver/           # KV state machine, RPC handlers, node wiring
├── clerk/              # client library with leader failover
├── config/             # node-address config file (node0ip/node0port/...)
└── test/               # end-to-end cluster tests
```

## Quick start

Requires Go 1.26 or newer.

```sh
make build                     # produces bin/kvserver and bin/clerk

# Terminal 1: bootstrap a local 3-node cluster. This picks three free
# ports, writes them to test.conf and starts each node as a child process.
bin/kvserver -n 3 -f test.conf -datadir ./data

# Terminal 2: drive the cluster.
bin/clerk -f test.conf -put x=hello
bin/clerk -f test.conf -append x=,world
bin/clerk -f test.conf -get x          # "hello,world"
bin/clerk -f test.conf -count 500      # demo workload: 500 Put/Get rounds
```

`Ctrl-C` in the first terminal shuts the whole cluster down gracefully.

A single node can also be run directly against a hand-written config:

```sh
bin/kvserver -f test.conf -id 0 -maxraftstate 500 -datadir ./data -v
```

### Configuration file

```
node0ip=127.0.0.1
node0port=29016
node1ip=127.0.0.1
node1port=29017
node2ip=127.0.0.1
node2port=29018
```

Nodes are read in order from `node0`; every node in the cluster must use
the same file (the same ordering defines node ids).

### Flags

`kvserver`:

| Flag           | Default     | Meaning                                            |
| -------------- | ----------- | -------------------------------------------------- |
| `-f`           | `test.conf` | cluster config file                                |
| `-id`          | `-1`        | node id (single-node mode)                         |
| `-n`           | `0`         | bootstrap a local cluster of n child nodes         |
| `-maxraftstate`| `500`       | snapshot threshold in bytes; `-1` disables         |
| `-datadir`     | `.`         | directory for persisted state and snapshots        |
| `-v`           | `false`     | debug logging                                      |

`clerk`:

| Flag      | Default     | Meaning                                  |
| --------- | ----------- | ---------------------------------------- |
| `-f`      | `test.conf` | cluster config file                      |
| `-count`  | `500`       | Put/Get rounds in demo mode              |
| `-key`    | `x`         | key used by the demo workload            |
| `-get`    | –           | one-shot read of a key                   |
| `-put`    | –           | one-shot `key=value` write               |
| `-append` | –           | one-shot `key=value` append              |

## Testing

```sh
make test     # unit + end-to-end tests
make race     # the same under the race detector
```

The end-to-end suite (`test/e2e_test.go`) boots real clusters on loopback
and covers basic operations, Append semantics, missing keys, leader
failover, snapshot compaction, follower restart catch-up and full
crash-restart recovery of committed data.

## Design notes

### Request path

1. The clerk sends `Get`/`PutAppend` to the server it believes is the
   leader, tagging the request with its client id and a monotonically
   increasing request id.
2. The KV handler encodes the operation with gob, submits it through
   `Raft.Start`, and registers a per-index wait channel.
3. Raft replicates the entry, commits it by majority, and the applier
   goroutine delivers it back on the apply channel.
4. The KV apply loop executes the command against the map unless the
   deduplication table shows it already ran, snapshots when the Raft state
   has grown too large, and signals the waiting handler.
5. The handler answers the client. On timeout it falls back to the
   deduplication table (writes) or a guarded local read (still-leader
   reads), and the clerk retries elsewhere with the *same* request id, so
   retries never execute twice.

### Concurrency model

One mutex guards all Raft state and one guards all KV state; the lock
ordering is strictly KV → Raft, so the two never deadlock. Network calls
never hold a mutex. The three Raft tickers (election, heartbeat, applier)
are goroutines that recompute their sleep intervals from wall-clock
deadlines, mirroring the original ticker design. Shutdown flows through a
`shutdown` channel that every blocking select observes.

### RPC transport

Inter-node and client traffic both run over
[yukino_rpc](https://github.com/hangtiancheng/yukino.go/tree/main/libs/yukino_rpc)
with its default JSON codec. Connections are lazy, which removes the
fixed `sleep(6)` bring-up barrier the C++ original needed: nodes may
start in any order and Raft's own retries bridge the gap. Two services
share one listener per node — `Raft` (RequestVote, AppendEntries,
InstallSnapshot) and `KVServer` (Get, PutAppend) — and the Raft service is
exposed through a thin `raft.Service` wrapper so administrative methods
(`Start`, `Snapshot`, `Kill`) are not reachable from the network.

## Defects fixed from the C++ original

Persistence and crash recovery:

- **Startup truncation destroyed persisted state.** The C++ `Persister`
  constructor truncated `raftstatePersistN.txt` and `snapshotPersistN.txt`
  on every start, so a restarted node always came back empty — defeating
  the purpose of persistence. Files are now loaded on startup and never
  truncated implicitly.
- **Whitespace-delimited reads corrupted archives.** `ReadRaftState` and
  `ReadSnapshot` used `ifs >> str`, which stops at the first whitespace
  token and returned a broken prefix of every boost text archive. State is
  now gob-encoded and read whole.
- **Non-atomic writes.** A crash mid-write could leave a half-written state
  file. Writes now go to a temporary file, are synced, and are renamed into
  place; `Save` writes the snapshot before the Raft state so any crash
  window resolves toward duplicate (deduplicated) applies rather than lost
  data.
- **`commitIndex` was not restored after restart**, leaving a restarted
  node briefly believing committed data was uncommitted. Both `commitIndex`
  and `lastApplied` now resume from the snapshot point.

Raft protocol handling:

- **Missing `return` crashed the process.** In `AppendEntries`, when
  `PrevLogIndex` was behind the follower's snapshot point, the C++ code
  filled in the rejection reply but fell through into `matchLog()`, whose
  assertion aborted the whole process on this rare but legal input. The
  branch now returns.
- **Term bumps were not always persisted.** `sendAppendEntries` adopted a
  higher term from a reply without persisting, so a crash could resurrect
  the stale term. All "three changes" (status/term/vote) now persist.
- **Commit accounting was fragile.** Commits rode a per-heartbeat-round
  shared ack counter whose threshold could be crossed by an empty heartbeat
  reply, delaying commits by a full round; the correct `matchIndex` scan
  existed but was never called. The scan (with the current-term check from
  Raft Figure 8) is now the only commit path.
- **Stale vote replies could promote a non-candidate.** `sendRequestVote`
  counted grants without re-checking that the node was still the candidate
  of that term. An explicit guard was added.
- **`matchIndex` could move backwards** when a snapshot ack arrived after
  newer replication progress; updates now take the maximum.
- **`nextIndex` rewound blindly** on any rejection, including delayed
  replies from earlier rounds (a hazard the original only noted in a
  comment). Hints are applied only when positive and strictly decreasing.
- **Stale-snapshot rejection was incomplete.** `InstallSnapshot` compared
  only against the previous snapshot point; it now also rejects snapshots
  at or behind `commitIndex`, as in the standard 6.824 formulation.
- **Unsynchronized status reads.** The C++ tickers read `m_status` without
  the mutex (a data race); every state access here happens under the lock.

KV layer and clients:

- **`Append` behaved like `Put`.** The skip-list path overwrote values;
  concatenation existed only in commented-out code. Append concatenates
  again, matching the operation's name and 6.824 semantics.
- **The snapshot proportion parameter was ignored**, with the trigger
  hard-coded to `maxRaftState/10`. The documented 9/10 proportion is
  honored.
- **Wait-channel cleanup could evict another request.** After a leader
  change the same log index can belong to a new in-flight request; the
  unconditional map erase is replaced by an identity-checked delete.
- **Clerk ids collided across runs and processes** (four unseeded `rand()`
  calls). Ids now come from `crypto/rand`.
- **The clerk hot-spun** through the server ring with no delay and could
  loop forever on unexpected error strings. Retries are paced, honor
  context cancellation, and unknown errors surface to the caller.
- **Startup barrier sleeps** (`sleep(6)` plus a node-count-dependent sleep)
  were removed in favor of lazy dials and natural retries.
- **Undefined behavior in logging** (a `DPrintf` format string with missing
  arguments, `std::string*` passed to `%s`) is gone with `slog` structured
  logging.
