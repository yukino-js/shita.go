# yukino-log

Concurrent logging library with a synchronous front end and a double-buffered
file backend. It keeps that design — a formatted line, a fixed
staging buffer, one backend goroutine, and size- and day-based file rolling —
and corrects the defects listed below.

The package uses only the standard library. It requires Go 1.26.5 or later.

Import path: `github.com/yukino-js/yukino-log`.

## Install

```bash
go get github.com/yukino-js/yukino-log
```

## Usage

A logger with no options writes every level to standard output. `Info`
concatenates arguments the way `fmt.Sprint` does. `Infof` formats a string.
The call site is the file and line of the caller.

```go
logger := yukino_log.New(yukino_log.Options{MinLevel: yukino_log.LevelInfo})
logger.Infof("listening on %s", addr)
logger.Info("ready")
```

`Fatal` and `Fatalf` write the record, run the flush hook, and panic with a
`FatalError`. Deferred calls still run. The process is not aborted.

To write files, start an `Async` backend and attach it. `Start` creates the
parent directory and the first file. `Stop` writes every record that was
accepted, syncs the file, and waits for the backend goroutine.

```go
backend, err := yukino_log.NewAsync(yukino_log.AsyncOptions{
    Basename: "logs/server", // files are logs/server.YYYYMMDD-HHMMSS.log
    RollSize: 64 << 20,
})
if err != nil {
    return err
}
if err := backend.Start(); err != nil {
    return err
}
defer backend.Stop()

logger := yukino_log.New(yukino_log.Options{})
logger.Attach(backend)
logger.Infof("listening on %s", addr)
```

`Basename` is a file prefix, not a directory. A second file opened in the same
second is named `logs/server.YYYYMMDD-HHMMSS.log.000001`. The numeric suffix
sorts after the original name. An existing file from an earlier process is
appended to. If that file is already past `RollSize`, the new process opens
the next suffix instead of growing it further.

`Open` is the same rolling file without the async queue. It is an `io.Writer`.

```go
file, err := yukino_log.Open("logs/server", 64<<20, yukino_log.FileOptions{})
if err != nil {
    return err
}
defer file.Close()
logger := yukino_log.New(yukino_log.Options{Output: file.Write, Flush: file.Flush})
```

The byte slice passed to an output function is reused after the function
returns and must not be retained. `io.Writer` already follows that rule.

## Record format

```text
2006/01/02 15:04:05.000000 LEVEL message - file.go:42
```

The level occupies six bytes (`TRACE `, `DEBUG `, `INFO  `, `WARN  `,
`ERROR `, `FATAL `). The fractional second is always six digits. The clock is
formatted in `Options.Location`, which defaults to the local zone.

Records below `MinLevel` are discarded. The default is trace, so nothing is
discarded. A fatal record is written even when the minimum is fatal.

## Backend

```text
Logger.Info / Infof
        │  one formatted line
        ▼
Async.Append
        │  current buffer, one spare, queue of full buffers
        │  producers share a mutex; a full buffer wakes the backend
        ▼
one goroutine
        │  writes outside the mutex
        ▼
LogFile
        bufio writer, 64 KiB
        roll when the file grows past RollSize
        roll when the local calendar day changes
```

`Append` copies the record before it returns. A record that fills the staging
buffer is handed to the backend immediately. A record larger than
`BufferSize` (default 4,000,000 bytes) is queued whole; it is not dropped and
it is not split. Anything smaller waits in the current buffer until the next
full buffer, `Flush`, `Stop`, or `FlushInterval` (default 3 seconds).

The backend is the only goroutine that calls `LogFile` for an `Async`. The
file itself is also safe for concurrent `Write`, `Flush`, and `Close`.

`Flush` pushes the `bufio` buffer to the operating system. It does not sync.
`Close`, `Stop`, and a roll sync the file they are finishing. A crash can
still drop data that has not reached `Flush`.

Size rolls happen before an append that would pass `RollSize`. The cut is the
last newline that still fits, so a record stays on one line. A single record
longer than `RollSize` is written whole to an empty file and can exceed the
limit; the C++ logger instead wrote an entire staging buffer and only then
checked the size, so the limit was soft by up to 4,000,000 bytes. The
successor is created when the next byte arrives, which avoids an empty file
after the last roll. The daily check runs every `CheckEveryN` appends (default 1024) and uses local midnight, not a 86400-second boundary on the Unix clock.
The append that observes the new day stays in the previous file.

`Append` before `Start` returns `ErrNotStarted`. `Append` after `Stop` returns
`ErrClosed`. `Stop` before `Start` returns `ErrNotStarted`. A second `Stop`
returns the error from the first, which is nil when the file closed cleanly.
The first write error is what `Stop` and `Flush` report; the same diagnostic
is written to standard error and does not go back through the logger.

## Layout

```text
doc.go        package documentation
logger.go     levels, formatting, package-level logger
async.go      double-buffered backend
logfile.go    rolling file
cmd/yukino_log-demo small driver
```

## Tests

```bash
go test ./...
go test -race ./...
```

The demo writes one line to the terminal and the rest under `logs/`:

```bash
go run ./cmd/yukino-log -n 1000 -roll 1048576
```
