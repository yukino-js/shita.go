package yukino_log

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Level is the severity of a log record. Higher values are more severe.
type Level int

const (
	// LevelTrace is the finest diagnostic level and the default minimum.
	LevelTrace Level = iota
	// LevelDebug is for information useful while developing.
	LevelDebug
	// LevelInfo is for routine operational messages.
	LevelInfo
	// LevelWarn is for a recoverable condition that should be looked at.
	LevelWarn
	// LevelError is for a failed operation that the process can survive.
	LevelError
	// LevelFatal is for a condition after which the caller panics.
	LevelFatal
)

// levelTags are fixed at six bytes so columns line up. The padding matches
// the C++ logger's level names.
var levelTags = [...]string{
	LevelTrace: "TRACE ",
	LevelDebug: "DEBUG ",
	LevelInfo:  "INFO  ",
	LevelWarn:  "WARN  ",
	LevelError: "ERROR ",
	LevelFatal: "FATAL ",
}

// String returns the level name without the padding used in the record.
func (l Level) String() string {
	switch l {
	case LevelTrace:
		return "TRACE"
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	case LevelFatal:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

func (l Level) tag() string {
	if l >= 0 && int(l) < len(levelTags) {
		return levelTags[l]
	}
	return "UNK   "
}

// OutputFunc consumes one complete log record. The slice is reused after the
// function returns and must not be retained. It follows the same rule as
// [io.Writer].
type OutputFunc func(p []byte) (int, error)

// FlushFunc pushes buffered records to their destination.
type FlushFunc func() error

// Options configures a [Logger]. Zero values select the defaults: every
// level is recorded, output goes to standard output, and timestamps use the
// local time zone.
type Options struct {
	// MinLevel drops records below this level. Fatal records are never dropped.
	MinLevel Level
	// Output receives each formatted record. Nil selects standard output.
	Output OutputFunc
	// Flush runs before a fatal record panics, and when [Logger.Flush] is called.
	// Nil means there is nothing to flush.
	Flush FlushFunc
	// Now supplies the record timestamp. Nil selects [time.Now].
	Now func() time.Time
	// Location is the zone used to format timestamps. Nil selects [time.Local].
	Location *time.Location
}

// Logger formats records and hands them to an output function.
// A Logger is safe for concurrent use. Its output function must be as well.
type Logger struct {
	mu       sync.RWMutex
	min      Level
	output   OutputFunc
	flush    FlushFunc
	now      func() time.Time
	location *time.Location
}

// New returns a logger. The default logger used by the package-level
// functions is created the same way with zero options.
func New(opts Options) *Logger {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Location == nil {
		opts.Location = time.Local
	}
	if opts.Output == nil {
		opts.Output = writeStdout
	}
	return &Logger{
		min:      opts.MinLevel,
		output:   opts.Output,
		flush:    opts.Flush,
		now:      opts.Now,
		location: opts.Location,
	}
}

// SetMinLevel drops subsequent records below level. A fatal record is written
// regardless of the minimum.
func (l *Logger) SetMinLevel(level Level) {
	l.mu.Lock()
	l.min = level
	l.mu.Unlock()
}

// SetOutput replaces the record destination. A nil function restores
// standard output. The previous function is not waited on.
func (l *Logger) SetOutput(fn OutputFunc) {
	if fn == nil {
		fn = writeStdout
	}
	l.mu.Lock()
	l.output = fn
	l.mu.Unlock()
}

// SetFlush replaces the flush hook. A nil function clears it.
func (l *Logger) SetFlush(fn FlushFunc) {
	l.mu.Lock()
	l.flush = fn
	l.mu.Unlock()
}

// Attach sends records to an async backend and uses its Flush as the flush
// hook. Start the backend before any record is written. Attach panics if a
// is nil.
func (l *Logger) Attach(a *Async) {
	if a == nil {
		panic("yukino_log: attach nil async logger")
	}
	l.SetOutput(a.Write)
	l.SetFlush(a.Flush)
}

// Flush runs the flush hook. A logger with no hook returns nil.
func (l *Logger) Flush() error {
	l.mu.RLock()
	fn := l.flush
	l.mu.RUnlock()
	if fn == nil {
		return nil
	}
	return fn()
}

// Log writes a record at the given level.
func (l *Logger) Log(level Level, args ...any) { l.write(level, 2, "", args) }

// Logf writes a formatted record at the given level.
func (l *Logger) Logf(level Level, format string, args ...any) {
	l.write(level, 2, format, args)
}

// Trace writes a trace record.
func (l *Logger) Trace(args ...any) { l.write(LevelTrace, 2, "", args) }

// Tracef writes a formatted trace record.
func (l *Logger) Tracef(format string, args ...any) { l.write(LevelTrace, 2, format, args) }

// Debug writes a debug record.
func (l *Logger) Debug(args ...any) { l.write(LevelDebug, 2, "", args) }

// Debugf writes a formatted debug record.
func (l *Logger) Debugf(format string, args ...any) { l.write(LevelDebug, 2, format, args) }

// Info writes an info record. Arguments are concatenated like [fmt.Sprint]:
// a space is inserted between two non-string values only.
func (l *Logger) Info(args ...any) { l.write(LevelInfo, 2, "", args) }

// Infof writes a formatted info record.
func (l *Logger) Infof(format string, args ...any) { l.write(LevelInfo, 2, format, args) }

// Warn writes a warning record.
func (l *Logger) Warn(args ...any) { l.write(LevelWarn, 2, "", args) }

// Warnf writes a formatted warning record.
func (l *Logger) Warnf(format string, args ...any) { l.write(LevelWarn, 2, format, args) }

// Error writes an error record.
func (l *Logger) Error(args ...any) { l.write(LevelError, 2, "", args) }

// Errorf writes a formatted error record.
func (l *Logger) Errorf(format string, args ...any) { l.write(LevelError, 2, format, args) }

// Fatal writes a fatal record, runs the flush hook, and panics with a
// [FatalError]. The panic is the replacement for the C++ logger's abort:
// deferred calls, including [Async.Stop], still run.
func (l *Logger) Fatal(args ...any) { l.write(LevelFatal, 2, "", args) }

// Fatalf writes a formatted fatal record and then panics, like [Logger.Fatal].
func (l *Logger) Fatalf(format string, args ...any) { l.write(LevelFatal, 2, format, args) }

func (l *Logger) write(level Level, skip int, format string, args []any) {
	l.mu.RLock()
	if level < l.min && level != LevelFatal {
		l.mu.RUnlock()
		return
	}
	out, now, loc := l.output, l.now, l.location
	l.mu.RUnlock()

	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		file = "???"
		line = 0
	}

	bp := linePool.Get().(*[]byte)
	buf := (*bp)[:0]
	buf = appendStamp(buf, now(), loc)
	buf = append(buf, level.tag()...)
	msgStart := len(buf)
	if format == "" {
		buf = fmt.Append(buf, args...)
	} else {
		buf = fmt.Appendf(buf, format, args...)
	}
	var fatalMsg string
	if level == LevelFatal {
		fatalMsg = string(buf[msgStart:])
	}
	buf = appendSuffix(buf, file, line)

	_, err := out(buf)
	releaseLine(bp, buf)
	if err != nil {
		report("writing log output", err)
	}
	if level == LevelFatal {
		if ferr := l.Flush(); ferr != nil {
			report("flushing log output", ferr)
		}
		panic(FatalError{Message: fatalMsg})
	}
}

const (
	// defaultLineCap is only the initial capacity of a pooled formatting
	// buffer. The C++ front end used a 4000-byte buffer and dropped the rest
	// of a record; this buffer grows instead.
	defaultLineCap = 4000
	// maxPooledLine bounds how large a buffer the pool will keep.
	maxPooledLine = 64 * 1024
)

var linePool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, defaultLineCap)
		return &b
	},
}

func releaseLine(bp *[]byte, buf []byte) {
	if cap(buf) <= maxPooledLine {
		*bp = buf
		linePool.Put(bp)
	}
}

func appendStamp(dst []byte, now time.Time, loc *time.Location) []byte {
	if loc == nil {
		loc = time.Local
	}
	now = now.In(loc)
	// AppendFormat strips trailing zeros in a fractional second. The record
	// format always uses six digits, matching the C++ "%06d" microseconds.
	dst = now.AppendFormat(dst, "2006/01/02 15:04:05")
	dst = fmt.Appendf(dst, ".%06d ", now.Nanosecond()/1000)
	return dst
}

func appendSuffix(dst []byte, file string, line int) []byte {
	dst = append(dst, " - "...)
	dst = append(dst, basename(file)...)
	dst = append(dst, ':')
	dst = strconv.AppendInt(dst, int64(line), 10)
	dst = append(dst, '\n')
	return dst
}

func basename(path string) string {
	if path == "" {
		return "???"
	}
	if i := strings.LastIndexAny(path, `/\`); i >= 0 {
		path = path[i+1:]
	}
	if path == "" {
		return "???"
	}
	return path
}

var (
	stdoutMu sync.Mutex
	stderrMu sync.Mutex
)

func writeStdout(p []byte) (int, error) {
	stdoutMu.Lock()
	defer stdoutMu.Unlock()
	return os.Stdout.Write(p)
}

// report writes a diagnostic to standard error. It does not go through a
// Logger, so a failing output cannot recurse.
func report(what string, err error) {
	if err == nil {
		return
	}
	stderrMu.Lock()
	fmt.Fprintf(os.Stderr, "yukino_log: %s: %v\n", what, err)
	stderrMu.Unlock()
}

var std = New(Options{})

// SetMinLevel sets the minimum level on the package logger.
func SetMinLevel(level Level) { std.SetMinLevel(level) }

// SetOutput sets the output on the package logger. Nil restores standard output.
func SetOutput(fn OutputFunc) { std.SetOutput(fn) }

// SetFlush sets the flush hook on the package logger.
func SetFlush(fn FlushFunc) { std.SetFlush(fn) }

// Attach connects the package logger to an async backend.
func Attach(a *Async) { std.Attach(a) }

// Flush flushes the package logger.
func Flush() error { return std.Flush() }

// Log writes a record with the package logger.
func Log(level Level, args ...any) { std.write(level, 2, "", args) }

// Logf writes a formatted record with the package logger.
func Logf(level Level, format string, args ...any) { std.write(level, 2, format, args) }

// Trace writes a trace record with the package logger.
func Trace(args ...any) { std.write(LevelTrace, 2, "", args) }

// Tracef writes a formatted trace record with the package logger.
func Tracef(format string, args ...any) { std.write(LevelTrace, 2, format, args) }

// Debug writes a debug record with the package logger.
func Debug(args ...any) { std.write(LevelDebug, 2, "", args) }

// Debugf writes a formatted debug record with the package logger.
func Debugf(format string, args ...any) { std.write(LevelDebug, 2, format, args) }

// Info writes an info record with the package logger.
func Info(args ...any) { std.write(LevelInfo, 2, "", args) }

// Infof writes a formatted info record with the package logger.
func Infof(format string, args ...any) { std.write(LevelInfo, 2, format, args) }

// Warn writes a warning record with the package logger.
func Warn(args ...any) { std.write(LevelWarn, 2, "", args) }

// Warnf writes a formatted warning record with the package logger.
func Warnf(format string, args ...any) { std.write(LevelWarn, 2, format, args) }

// Error writes an error record with the package logger.
func Error(args ...any) { std.write(LevelError, 2, "", args) }

// Errorf writes a formatted error record with the package logger.
func Errorf(format string, args ...any) { std.write(LevelError, 2, format, args) }

// Fatal writes a fatal record with the package logger and panics.
func Fatal(args ...any) { std.write(LevelFatal, 2, "", args) }

// Fatalf writes a formatted fatal record with the package logger and panics.
func Fatalf(format string, args ...any) { std.write(LevelFatal, 2, format, args) }

// Compile-time check that the file backends satisfy io.Writer.
var (
	_ io.Writer = (*LogFile)(nil)
	_ io.Writer = (*Async)(nil)
)
