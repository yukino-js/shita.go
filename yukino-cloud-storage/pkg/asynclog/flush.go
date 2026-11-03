package asynclog

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FlushMode controls how aggressively written bytes are pushed toward stable
// storage.
type FlushMode int

const (
	// FlushNone leaves bytes in the user-space buffer; they reach the OS page
	// cache when the buffer fills or the sink is closed.
	FlushNone FlushMode = iota
	// FlushBuffered flushes the user-space buffer to the OS after each write.
	FlushBuffered
	// FlushSync flushes the user-space buffer and then calls fsync so bytes are
	// durable on disk before the call returns.
	FlushSync
)

// ParseFlushMode maps the legacy numeric configuration value onto a FlushMode.
// Unknown values fall back to FlushNone.
func ParseFlushMode(v int) FlushMode {
	switch v {
	case 1:
		return FlushBuffered
	case 2:
		return FlushSync
	default:
		return FlushNone
	}
}

// Flusher is a destination for formatted log records.
//
// Implementations are only ever invoked from the single worker goroutine that
// owns a logger, so they do not need to be safe for concurrent use. The
// concrete types here are nevertheless guarded by a mutex so they remain safe if
// reused elsewhere.
type Flusher interface {
	// Flush writes one batch of records.
	Flush(record []byte) error
	// Close releases the underlying resource, flushing pending bytes.
	Close() error
}

// WriterFlusher writes records to an underlying io.Writer through a buffered
// writer, honouring a FlushMode.
type WriterFlusher struct {
	mu     sync.Mutex
	bw     *bufio.Writer
	mode   FlushMode
	syncer interface{ Sync() error }
}

// NewWriterFlusher returns a flusher that writes to w. If w supports Sync it is
// used for FlushSync mode.
func NewWriterFlusher(w io.Writer, mode FlushMode) *WriterFlusher {
	f := &WriterFlusher{bw: bufio.NewWriter(w), mode: mode}
	if s, ok := w.(interface{ Sync() error }); ok {
		f.syncer = s
	}
	return f
}

// Flush implements Flusher.
func (f *WriterFlusher) Flush(record []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.bw.Write(record); err != nil {
		return err
	}
	return f.flushLocked()
}

func (f *WriterFlusher) flushLocked() error {
	switch f.mode {
	case FlushBuffered:
		return f.bw.Flush()
	case FlushSync:
		if err := f.bw.Flush(); err != nil {
			return err
		}
		if f.syncer != nil {
			return f.syncer.Sync()
		}
		return nil
	default:
		return nil
	}
}

// Close implements Flusher.
func (f *WriterFlusher) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bw.Flush()
}

// NewStdoutFlusher returns a flusher that writes to standard output.
func NewStdoutFlusher(mode FlushMode) *WriterFlusher {
	return NewWriterFlusher(os.Stdout, mode)
}

// FileFlusher appends records to a single file, creating parent directories as
// needed.
//
// If the file cannot be opened the error is returned from Flush instead of
// crashing the process, and the flusher keeps retrying on later records. This
// repairs the original implementation, which dereferenced a null file handle
// after a failed open.
type FileFlusher struct {
	mu   sync.Mutex
	path string
	mode FlushMode
	file *os.File
	bw   *bufio.Writer
}

// NewFileFlusher returns a flusher that appends to path. The file is opened
// lazily on the first Flush.
func NewFileFlusher(path string, mode FlushMode) *FileFlusher {
	return &FileFlusher{path: path, mode: mode}
}

func (f *FileFlusher) ensureOpen() error {
	if f.file != nil {
		return nil
	}
	if dir := filepath.Dir(f.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	f.file = file
	f.bw = bufio.NewWriter(file)
	return nil
}

// Flush implements Flusher.
func (f *FileFlusher) Flush(record []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.ensureOpen(); err != nil {
		return err
	}
	if _, err := f.bw.Write(record); err != nil {
		f.closeLocked()
		return err
	}
	switch f.mode {
	case FlushBuffered:
		return f.bw.Flush()
	case FlushSync:
		if err := f.bw.Flush(); err != nil {
			return err
		}
		return f.file.Sync()
	default:
		return nil
	}
}

func (f *FileFlusher) closeLocked() {
	if f.bw != nil {
		_ = f.bw.Flush()
	}
	if f.file != nil {
		_ = f.file.Close()
		f.file = nil
		f.bw = nil
	}
}

// Close implements Flusher.
func (f *FileFlusher) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeLocked()
	return nil
}

// RollFileFlusher writes records to a sequence of files, starting a new file
// once the current one reaches a configured maximum size.
type RollFileFlusher struct {
	mu       sync.Mutex
	base     string
	maxSize  int64
	mode     FlushMode
	file     *os.File
	bw       *bufio.Writer
	curSize  int64
	sequence int64
}

// NewRollFileFlusher returns a flusher that rotates files derived from base
// once they exceed maxSize bytes.
func NewRollFileFlusher(base string, maxSize int64, mode FlushMode) *RollFileFlusher {
	if maxSize <= 0 {
		maxSize = 1 << 20
	}
	return &RollFileFlusher{base: base, maxSize: maxSize, mode: mode}
}

// Flush implements Flusher.
func (r *RollFileFlusher) Flush(record []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ensureOpen(int64(len(record))); err != nil {
		return err
	}
	if _, err := r.bw.Write(record); err != nil {
		r.closeLocked()
		return err
	}
	r.curSize += int64(len(record))
	switch r.mode {
	case FlushBuffered:
		return r.bw.Flush()
	case FlushSync:
		if err := r.bw.Flush(); err != nil {
			return err
		}
		return r.file.Sync()
	default:
		return nil
	}
}

// ensureOpen opens a fresh file when none is open or the current file can no
// longer accept the record without exceeding the size limit.
func (r *RollFileFlusher) ensureOpen(need int64) error {
	if r.file != nil && r.curSize+need <= r.maxSize {
		return nil
	}
	r.closeLocked()
	if dir := filepath.Dir(r.base); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	name := r.nextName()
	file, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	r.file = file
	r.bw = bufio.NewWriter(file)
	// Honour any pre-existing content so the rotation size stays accurate.
	if st, err := file.Stat(); err == nil {
		r.curSize = st.Size()
	} else {
		r.curSize = 0
	}
	return nil
}

// nextName builds a collision-free, human readable file name. The timestamp is
// zero padded and the hour, minute and second are used verbatim, correcting an
// off-by-one in the original implementation that added one to each field.
func (r *RollFileFlusher) nextName() string {
	r.sequence++
	now := time.Now()
	return fmt.Sprintf("%s.%s.%04d.log", r.base, now.Format("20060102-150405.000"), r.sequence)
}

func (r *RollFileFlusher) closeLocked() {
	if r.bw != nil {
		_ = r.bw.Flush()
	}
	if r.file != nil {
		_ = r.file.Close()
		r.file = nil
		r.bw = nil
	}
}

// Close implements Flusher.
func (r *RollFileFlusher) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closeLocked()
	return nil
}
