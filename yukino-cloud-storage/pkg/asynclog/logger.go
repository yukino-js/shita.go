package asynclog

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// Logger formats records and hands them to an async Worker for flushing. It is
// safe for concurrent use by multiple goroutines.
type Logger struct {
	name       string
	minLevel   Level
	includeGID bool
	flushers   []Flusher
	worker     *Worker
	backup     BackupSink
}

// Name returns the logger name.
func (l *Logger) Name() string { return l.name }

// Enabled reports whether records at the given level pass the level filter.
func (l *Logger) Enabled(level Level) bool { return level >= l.minLevel }

// log formats and enqueues a record when it passes the level filter.
//
// extraSkip is the number of additional stack frames between the public entry
// point and the user's call site; it lets package-level helpers report the
// correct file and line.
func (l *Logger) log(level Level, extraSkip int, format string, args []any) {
	if !l.Enabled(level) {
		return
	}
	payload := format
	if len(args) > 0 {
		payload = fmt.Sprintf(format, args...)
	}
	var file string
	var line int
	if _, f, ln, ok := runtime.Caller(2 + extraSkip); ok {
		file = filepath.Base(f)
		line = ln
	}
	var gid int64 = -1
	if l.includeGID {
		gid = goroutineID()
	}
	msg := &Message{
		Level:       level,
		LoggerName:  l.name,
		File:        file,
		Line:        line,
		Payload:     payload,
		Time:        time.Now(),
		GoroutineID: gid,
	}
	data := msg.Format()

	// ERROR and FATAL records are additionally shipped to the backup sink. The
	// sink is asynchronous, so this never blocks the caller.
	if l.backup != nil && level >= LevelError {
		l.backup.Backup(data)
	}

	l.worker.Push([]byte(data))
}

// realFlush is the worker callback: it writes a batch of records to every sink.
func (l *Logger) realFlush(b *Buffer) {
	data := b.Bytes()
	for _, f := range l.flushers {
		if err := f.Flush(data); err != nil {
			// Reporting through the logger itself would recurse, so surface the
			// problem on standard error instead of losing it silently.
			fmt.Fprintf(os.Stderr, "asynclog: logger %q flush failed: %v\n", l.name, err)
		}
	}
}

// Debugf logs a formatted record at DEBUG level.
func (l *Logger) Debugf(format string, args ...any) { l.log(LevelDebug, 0, format, args) }

// Infof logs a formatted record at INFO level.
func (l *Logger) Infof(format string, args ...any) { l.log(LevelInfo, 0, format, args) }

// Warnf logs a formatted record at WARN level.
func (l *Logger) Warnf(format string, args ...any) { l.log(LevelWarn, 0, format, args) }

// Errorf logs a formatted record at ERROR level.
func (l *Logger) Errorf(format string, args ...any) { l.log(LevelError, 0, format, args) }

// Fatalf logs a formatted record at FATAL level.
func (l *Logger) Fatalf(format string, args ...any) { l.log(LevelFatal, 0, format, args) }

// Debug logs a record built by concatenating its arguments at DEBUG level.
func (l *Logger) Debug(args ...any) { l.log(LevelDebug, 0, fmt.Sprint(args...), nil) }

// Info logs a record built by concatenating its arguments at INFO level.
func (l *Logger) Info(args ...any) { l.log(LevelInfo, 0, fmt.Sprint(args...), nil) }

// Warn logs a record built by concatenating its arguments at WARN level.
func (l *Logger) Warn(args ...any) { l.log(LevelWarn, 0, fmt.Sprint(args...), nil) }

// Error logs a record built by concatenating its arguments at ERROR level.
func (l *Logger) Error(args ...any) { l.log(LevelError, 0, fmt.Sprint(args...), nil) }

// Fatal logs a record built by concatenating its arguments at FATAL level.
func (l *Logger) Fatal(args ...any) { l.log(LevelFatal, 0, fmt.Sprint(args...), nil) }

// Close stops the worker, drains buffered records, and closes every sink. It is
// idempotent.
func (l *Logger) Close() error {
	l.worker.Stop()
	var firstErr error
	for _, f := range l.flushers {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if l.backup != nil {
		if err := l.backup.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
