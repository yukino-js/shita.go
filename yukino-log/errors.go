package yukino_log

import "errors"

// Sentinel errors returned by the file and async backends.
var (
	// ErrClosed is returned when a logger is used after [Async.Stop] or
	// [LogFile.Close].
	ErrClosed = errors.New("yukino_log: logger is closed")

	// ErrNotStarted is returned when [Async.Append], [Async.Flush], or
	// [Async.Stop] runs before a successful [Async.Start].
	ErrNotStarted = errors.New("yukino_log: async logger is not started")

	// ErrBasename is returned when a log basename is empty, ends with a
	// path separator, or has no usable final element.
	ErrBasename = errors.New("yukino_log: log basename is empty or has no final element")

	// ErrRollSize is returned when a roll size is not positive.
	ErrRollSize = errors.New("yukino_log: roll size must be positive")

	// ErrFlushInterval is returned when a flush interval is negative.
	ErrFlushInterval = errors.New("yukino_log: flush interval must not be negative")

	// ErrCheckEvery is returned when the daily-check interval is negative.
	ErrCheckEvery = errors.New("yukino_log: daily check interval must not be negative")

	// ErrBufferSize is returned when an async buffer size is negative.
	ErrBufferSize = errors.New("yukino_log: buffer size must not be negative")
)

// FatalError is the value [Logger.Fatal] and [Logger.Fatalf] panic with
// after the record has been written and the flush hook has run.
type FatalError struct {
	// Message is the formatted user text, without the time, level, or call site.
	Message string
}

// Error returns the formatted user text.
func (e FatalError) Error() string { return e.Message }
