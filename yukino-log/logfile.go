package yukino_log

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// DefaultFlushInterval is how long a partly filled async buffer waits
	// before the backend writes it, and how long a [LogFile] waits between
	// flushes of its own buffer. Both match the C++ default of three seconds.
	DefaultFlushInterval = 3 * time.Second

	// DefaultCheckEveryN is the number of appends between local-midnight
	// checks. Size-based rolls are decided on every append.
	DefaultCheckEveryN = 1024

	// fileBufferSize matches the 64 KiB stdio buffer in the C++ FileUtil.
	fileBufferSize = 64 * 1024

	// maxRollSequence stops a same-second roll from looping forever if every
	// candidate name is already taken.
	maxRollSequence = 100_000
)

// FileOptions configures a [LogFile]. Zero values select the defaults.
type FileOptions struct {
	// FlushInterval is the maximum time between flushes. Zero selects
	// [DefaultFlushInterval]. A negative value is rejected.
	FlushInterval time.Duration
	// CheckEveryN is how many appends elapse between local-midnight checks.
	// Zero selects [DefaultCheckEveryN]. Size rolls are not affected.
	CheckEveryN int
	// Now supplies the clock used for names, the daily boundary, and the
	// flush interval. Nil selects [time.Now]. Now must not call back into
	// the LogFile.
	Now func() time.Time
	// Location is the zone used for file names and the daily boundary.
	// Nil selects [time.Local].
	Location *time.Location
}

// LogFile appends records to a rolling file. It is safe for concurrent use.
// An append that would grow the file past the roll size is cut at the last
// newline that still fits, and the rest begins the successor. A single record
// longer than the roll size is written whole. The successor is created on the
// next append, so a roll does not leave an empty file behind.
type LogFile struct {
	mu            sync.Mutex
	basename      string
	rollSize      int64
	flushInterval time.Duration
	checkEveryN   int
	now           func() time.Time
	loc           *time.Location

	file      *os.File
	bufw      *bufio.Writer
	name      string
	stamp     string
	seq       int
	written   int64
	count     int
	period    time.Time
	lastFlush time.Time
	opened    bool
	closed    bool
}

// Open creates the parent directory and the first log file. The file name is
// basename.YYYYMMDD-HHMMSS.log in Location. A file left by an earlier process
// in that same second is appended to; once it is already past the roll size,
// a numbered successor is opened instead.
func Open(basename string, rollSize int64, opts FileOptions) (*LogFile, error) {
	if err := checkBasename(basename); err != nil {
		return nil, err
	}
	if rollSize <= 0 {
		return nil, ErrRollSize
	}
	flush, err := positiveDuration(opts.FlushInterval, DefaultFlushInterval)
	if err != nil {
		return nil, err
	}
	every, err := positiveCount(opts.CheckEveryN, DefaultCheckEveryN)
	if err != nil {
		return nil, err
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	loc := opts.Location
	if loc == nil {
		loc = time.Local
	}
	if err := prepareDir(basename); err != nil {
		return nil, err
	}
	f := &LogFile{
		basename:      basename,
		rollSize:      rollSize,
		flushInterval: flush,
		checkEveryN:   every,
		now:           now,
		loc:           loc,
	}
	if err := f.openLocked(f.now(), true); err != nil {
		return nil, err
	}
	f.opened = true
	return f, nil
}

// Append writes p, rolling on a record boundary when the file would pass the
// roll size and when the local day changes. A nil or empty p is a no-op. On a
// non-nil error some of p may already have been accepted; the caller should
// not retry the same bytes blindly.
func (f *LogFile) Append(p []byte) error {
	if len(p) == 0 {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrClosed
	}
	for len(p) > 0 {
		now := f.now()
		if err := f.ensureLocked(now); err != nil {
			return err
		}
		n := f.chunkLen(p)
		if n == 0 {
			// The next record does not fit. Close this file and open the
			// successor on the next iteration.
			if err := f.closeLocked(); err != nil {
				return err
			}
			continue
		}
		if err := f.writeLocked(p[:n]); err != nil {
			return err
		}
		p = p[n:]
		if f.written > f.rollSize {
			if err := f.closeLocked(); err != nil {
				return err
			}
		}
	}
	return f.maintainLocked(f.now())
}

// chunkLen is how much of p should be written to the current file. It returns
// 0 when the file already has data and no complete record fits in the room
// that is left, which tells the caller to roll first.
func (f *LogFile) chunkLen(p []byte) int {
	room := f.rollSize - f.written
	if room < 0 {
		room = 0
	}
	if int64(len(p)) <= room {
		return len(p)
	}
	limit := int(room)
	if limit > len(p) {
		limit = len(p)
	}
	if limit > 0 {
		if i := bytes.LastIndexByte(p[:limit], '\n'); i >= 0 {
			return i + 1
		}
	}
	if f.written == 0 {
		// An empty file takes one whole record, even when that record is
		// longer than the roll size. Splitting it would tear a line.
		if i := bytes.IndexByte(p, '\n'); i >= 0 {
			return i + 1
		}
		return len(p)
	}
	return 0
}

func (f *LogFile) ensureLocked(now time.Time) error {
	if f.file != nil {
		return nil
	}
	if err := f.openLocked(now, !f.opened); err != nil {
		return err
	}
	f.opened = true
	return nil
}

func (f *LogFile) maintainLocked(now time.Time) error {
	if f.file == nil {
		return nil
	}
	f.count++
	if f.count >= f.checkEveryN {
		f.count = 0
		if !sameDay(now, f.period, f.loc) {
			return f.closeLocked()
		}
	}
	if now.Sub(f.lastFlush) >= f.flushInterval {
		if err := f.flushLocked(); err != nil {
			return err
		}
		f.lastFlush = now
	}
	return nil
}

// Write implements [io.Writer]. See [LogFile.Append] for error semantics.
func (f *LogFile) Write(p []byte) (int, error) {
	if err := f.Append(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Flush writes the buffered bytes to the operating system. It does not
// sync the file; [LogFile.Close] and a roll do.
func (f *LogFile) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrClosed
	}
	if err := f.flushLocked(); err != nil {
		return err
	}
	f.lastFlush = f.now()
	return nil
}

// Close flushes, syncs, and closes the current file. A second call returns nil.
func (f *LogFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	return f.closeLocked()
}

// Name returns the path of the file currently open, or the last file if the
// logger is closed.
func (f *LogFile) Name() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.name
}

// Written returns how many bytes the current file had when it was opened plus
// the bytes accepted since. It is not a count of bytes already synced.
func (f *LogFile) Written() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.written
}

func (f *LogFile) writeLocked(p []byte) error {
	n, err := f.bufw.Write(p)
	f.written += int64(n)
	if err != nil {
		return fmt.Errorf("yukino_log: write %s: %w", f.name, err)
	}
	if n != len(p) {
		return fmt.Errorf("yukino_log: write %s: short write: %d of %d bytes", f.name, n, len(p))
	}
	return nil
}

func (f *LogFile) flushLocked() error {
	if f.bufw == nil {
		return nil
	}
	if err := f.bufw.Flush(); err != nil {
		return fmt.Errorf("yukino_log: flush %s: %w", f.name, err)
	}
	return nil
}

func (f *LogFile) openLocked(now time.Time, first bool) error {
	file, name, err := f.create(now, first)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("yukino_log: stat %s: %w", name, err)
	}
	// A restarted process must not keep appending to a file that is already
	// over the limit. Counting only the bytes written by this process was
	// the C++ behaviour, and it let the file grow by another roll every run.
	if first && info.Size() > f.rollSize {
		file.Close()
		f.name = name
		return f.openLocked(now, false)
	}
	if f.file != nil {
		if err := f.closeLocked(); err != nil {
			file.Close()
			return err
		}
	}
	f.file = file
	f.bufw = bufio.NewWriterSize(file, fileBufferSize)
	f.name = name
	f.written = info.Size()
	f.lastFlush = now
	f.period = startOfDay(now, f.loc)
	f.count = 0
	return nil
}

func (f *LogFile) create(now time.Time, first bool) (*os.File, string, error) {
	stamp := now.In(f.loc).Format("20060102-150405")
	if stamp != f.stamp {
		f.stamp = stamp
		f.seq = 0
	}
	if first {
		name := stampName(f.basename, stamp, 0)
		file, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, "", fmt.Errorf("yukino_log: open %s: %w", name, err)
		}
		return file, name, nil
	}
	for f.seq < maxRollSequence {
		name := stampName(f.basename, stamp, f.seq)
		if name == f.name {
			f.seq++
			continue
		}
		file, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o644)
		if errors.Is(err, fs.ErrExist) {
			f.seq++
			continue
		}
		if err != nil {
			return nil, "", fmt.Errorf("yukino_log: open %s: %w", name, err)
		}
		return file, name, nil
	}
	return nil, "", fmt.Errorf("yukino_log: exhausted roll suffixes for %s", f.basename)
}

func (f *LogFile) closeLocked() error {
	if f.file == nil {
		return nil
	}
	name := f.name
	flushErr := f.bufw.Flush()
	syncErr := f.file.Sync()
	closeErr := f.file.Close()
	f.bufw = nil
	f.file = nil
	if err := errors.Join(flushErr, syncErr, closeErr); err != nil {
		return fmt.Errorf("yukino_log: close %s: %w", name, err)
	}
	return nil
}

// stampName builds basename.YYYYMMDD-HHMMSS.log, or the same name plus a
// zero-padded suffix when more than one file is opened in that second.
// The suffix sorts after the original name in a directory listing.
func stampName(basename, stamp string, seq int) string {
	name := basename + "." + stamp + ".log"
	if seq == 0 {
		return name
	}
	return fmt.Sprintf("%s.%06d", name, seq)
}

func startOfDay(t time.Time, loc *time.Location) time.Time {
	t = t.In(loc)
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, loc)
}

func sameDay(t, period time.Time, loc *time.Location) bool {
	year, month, day := t.In(loc).Date()
	py, pm, pd := period.In(loc).Date()
	return year == py && month == pm && day == pd
}

func checkBasename(basename string) error {
	base := filepath.Base(basename)
	if basename == "" || base == "." || base == ".." || endsWithSeparator(basename) {
		return ErrBasename
	}
	return nil
}

// endsWithSeparator reports whether path ends with a path separator, which
// means it names a directory rather than a file prefix.
func endsWithSeparator(path string) bool {
	if path == "" {
		return false
	}
	last := path[len(path)-1]
	return last == '/' || last == byte(filepath.Separator)
}

func prepareDir(basename string) error {
	dir := filepath.Dir(basename)
	if dir == "" || dir == "." {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("yukino_log: create log directory: %w", err)
	}
	return nil
}

func positiveDuration(d, fallback time.Duration) (time.Duration, error) {
	if d < 0 {
		return 0, ErrFlushInterval
	}
	if d == 0 {
		return fallback, nil
	}
	return d, nil
}

func positiveCount(n, fallback int) (int, error) {
	if n < 0 {
		return 0, ErrCheckEvery
	}
	if n == 0 {
		return fallback, nil
	}
	return n, nil
}
