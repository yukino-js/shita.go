// Package yukino_log is a concurrent logging library with a synchronous
// formatting front end and a double-buffered file backend.
//
// # Overview
//
// A [Logger] turns each call into one formatted record:
//
//	2006/01/02 15:04:05.000000 LEVEL message - file.go:42
//
// The level occupies a fixed six-byte column, the fractional second is
// always six digits, and the call site is the file and line of the caller.
// Records below [Options.MinLevel] are discarded; a fatal record is written
// regardless of the minimum. A logger with zero [Options] writes every level
// to standard output.
//
// [Logger.Fatal] and [Logger.Fatalf] write the record, run the flush hook,
// and panic with a [FatalError] so deferred cleanup still runs. The panic
// replaces the abort of the C++ original.
//
// # Backends
//
// [Async] accepts records from any number of goroutines and writes them from
// a single backend goroutine, keeping the double-buffer design of the C++
// original: producers share one staging buffer and one spare under a mutex, a
// full buffer is queued and the backend is woken, and the backend writes and
// recycles buffers outside the mutex. A record larger than the staging buffer
// is queued whole; nothing is dropped and nothing is split. [Async.Stop]
// drains every accepted record, syncs the file, and waits for the backend
// goroutine to exit.
//
// [LogFile] is the rolling file that [Async] writes through. [Open] returns
// one directly as an [io.Writer]. Files are named
// basename.YYYYMMDD-HHMMSS.log, and a second file opened in the same second
// gets a zero-padded numeric suffix that sorts after the original name. The
// file rolls before an append would grow it past the roll size, cutting at
// the last newline that still fits so a record stays on one line, and it
// rolls when the local calendar day changes.
//
// # Errors
//
// The backends report the first write failure to standard error and remember
// it for [Async.Stop], [Async.Flush], and [LogFile.Close]. A failing output
// never panics and never feeds back into the logger, so a broken sink cannot
// recurse.
package yukino_log
