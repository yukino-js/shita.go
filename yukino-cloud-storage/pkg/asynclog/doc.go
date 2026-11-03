// Package asynclog provides an asynchronous, non-blocking logging system.
//
// It is a Go migration of the C++ asynchronous log system. Log records
// are formatted on the caller's goroutine, handed to a dedicated background
// worker through a double buffer, and flushed to one or more sinks (standard
// output, a single file, or size-rotated files) without blocking the business
// logic that produced them.
//
// ERROR and FATAL records can additionally be shipped to a remote backup sink
// so they survive a process crash. The backup path is asynchronous and never
// blocks the caller.
//
// The package is intentionally dependency free and builds only on the Go
// standard library, so it can be embedded in any project.
package asynclog
