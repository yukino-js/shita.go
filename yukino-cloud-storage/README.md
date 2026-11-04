# yukino-cloud-storage

A Go re-implementation of the C++ project: an
asynchronous logging system grafted onto a small cloud-storage service.

The project keeps the two halves of the original:

1. **Async log system** (`pkg/asynclog`) — non-blocking, multi-goroutine-safe
   logging with level filtering, size-rotated file sinks, and remote backup of
   `ERROR`/`FATAL` records.
2. **Cloud storage service** (`internal/storage`, `internal/client`) — a web
   service for uploading, listing and downloading files across two tiers:
   _low_ (verbatim) and _deep_ (compressed).

Everything is written against the Go standard library wherever possible. The
only external dependencies are the in-house
[`yukino_http`](https://github.com/hangtiancheng/yukino.go/tree/main/libs/yukino_http)
web framework and
[`yukino_rpc`](https://github.com/hangtiancheng/yukino.go/tree/main/libs/yukino_rpc)
RPC framework, used for the HTTP surface and the remote log-backup transport
respectively.

---

## Layout

```text
.
├── cmd/
│   ├── yukino-storaged/         # cloud storage HTTP server
│   ├── yukino-storage-agent/    # directory watcher / upload agent
│   └── yukino-logbackupd/       # remote log backup receiver
├── pkg/
│   ├── asynclog/                # asynchronous logging system (reusable)
│   ├── bundle/                  # self-describing compression container
│   └── logbackup/               # RPC log backup client + receiver
├── internal/
│   ├── storage/                 # server-side storage domain + web UI
│   ├── client/                  # upload agent domain
│   └── logging/                 # per-binary logger bootstrap
└── configs/                     # example JSON configuration files
```

---

## Building

```bash
go build ./...
go test ./...
make build        # produces ./bin/yukino-*
```

---

## Running

Each binary accepts `-h` for the full flag list. Sensible defaults are built in,
so the quick start below works from the repository root.

### 1. (Optional) start the log backup receiver

```bash
./bin/yukino-logbackupd -addr 0.0.0.0:8080 -out ./logfile/backup.log
```

Then point the other processes at it by setting `backup_addr`/`backup_port` in
`configs/log.json`. `ERROR` and `FATAL` records are shipped there so they
survive a crash of the originating process.

### 2. start the storage server

```bash
./bin/yukino-storaged -config configs/storage.json -log-config configs/log.json
```

Browse to `http://localhost:8081/` for the web UI: choose a file, pick a storage
tier, and upload. The page lists stored files with size, tier and time, and each
entry has a download link.

### 3. (Optional) start the upload agent

```bash
./bin/yukino-storage-agent -config configs/agent.json -log-config configs/log.json
```

The agent watches two local directories and uploads new or changed files to the
server, recording what it has sent so unchanged files are not re-uploaded.

All daemons handle `SIGINT`/`SIGTERM` and shut down gracefully, flushing the
async log buffers before exit.

---

## Configuration

### `configs/log.json` (async log system)

| Key                    | Meaning                                                   |
| ---------------------- | --------------------------------------------------------- |
| `buffer_size`          | initial async buffer capacity in bytes                    |
| `threshold`            | below this the buffer doubles; at/above it grows linearly |
| `linear_growth`        | additive growth step once `threshold` is reached          |
| `flush_log`            | `0` none, `1` flush to OS, `2` flush + `fsync`            |
| `backup_addr`/`port`   | remote backup receiver for ERROR/FATAL (empty disables)   |
| `level`                | minimum level to emit (`DEBUG`..`FATAL`)                  |
| `include_goroutine_id` | include the goroutine id in each record                   |

### `configs/storage.json` (server)

| Key                | Meaning                                          |
| ------------------ | ------------------------------------------------ |
| `server_ip`/`port` | bind address                                     |
| `download_prefix`  | URL prefix for downloads                         |
| `deep_storage_dir` | directory for compressed bundles                 |
| `low_storage_dir`  | directory for verbatim files                     |
| `storage_info`     | JSON metadata table path                         |
| `bundle_format`    | `gzip` (default), `zlib`, `flate` or `raw`       |
| `temp_dir`         | scratch dir for deep downloads (default OS temp) |
| `max_upload_bytes` | upload body limit                                |

### `configs/agent.json` (upload agent)

| Key                | Meaning                                       |
| ------------------ | --------------------------------------------- |
| `server_url`       | storage service base URL                      |
| `low_storage_dir`  | directory watched for verbatim uploads        |
| `deep_storage_dir` | directory watched for compressed uploads      |
| `state_file`       | where upload state is persisted               |
| `scan_interval`    | rescan period, e.g. `"1s"`                    |
| `quiet_period`     | ignore files modified more recently than this |
| `timeout`          | per-upload request timeout                    |

---

## Dependencies

- **`yukino_http`** — routing, middleware, recovery and response rendering for
  the storage service.
- **`yukino_rpc`** — framed, timeout-bounded transport for the remote log backup
  (replacing the original's hand-rolled, truncating TCP protocol).
- **`yukino_cache`** — intentionally _not_ used: the service has no genuine
  caching need. Deep downloads decompress into a unique temporary file that is
  removed after the response, rather than caching large payloads in memory.

---

## Testing

```bash
go test ./...          # unit + httptest + in-process RPC tests
go test -race ./...    # with the race detector
```

The suite covers buffer growth and drain-on-stop, level filtering, file rotation,
the compression round-trip for every format, metadata persistence, the full
upload/download/range/list HTTP flows, path-traversal sanitisation, the upload
agent, and the end-to-end `ERROR`/`FATAL` → RPC backup path.
