package skiplist

import (
	"bufio"
	"cmp"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	// MaxRecordSize is the longest single record Load accepts. bufio.Scanner
	// caps its buffer at 64 KiB by default, which is far too small for records
	// holding serialised documents, so the limit is raised explicitly and a
	// record that still exceeds it is reported as an error rather than being
	// silently truncated.
	MaxRecordSize = 16 << 20

	// dumpFileMode is the permission set applied to a file created by
	// DumpFile. os.CreateTemp would otherwise leave the dump readable only by
	// its owner.
	dumpFileMode = 0o644

	// dumpDirMode is the permission set applied to directories created by
	// DumpFile.
	dumpDirMode = 0o755
)

// Dump writes every element to w in list order, one record per line.
//
// The entries are copied under the shared lock, which is released before any
// encoding or writing. The output is therefore a consistent snapshot, and a
// slow writer does not block other goroutines. The writer may call back into
// the same list. An error while encoding or writing a later record can leave
// a prefix of that snapshot already written; DumpFile avoids exposing such a
// prefix by renaming a temporary file into place only after success.
//
// Callers that write to a slow destination should pass a buffered writer.
// DumpFile already does.
func (s *SkipList[K, V]) Dump(w io.Writer, format Format[K, V]) error {
	s.requireUsable()
	normalized, err := format.normalize()
	if err != nil {
		return err
	}
	return writeRecords(w, s.Snapshot(), normalized)
}

// writeRecords encodes entries without touching the skip list. format must
// already have been validated by Format.normalize.
func writeRecords[K, V any](w io.Writer, entries []Entry[K, V], format Format[K, V]) error {
	separator := string([]byte{format.Separator})
	var record strings.Builder
	for _, entry := range entries {
		keyText, err := format.Key.Encode(entry.Key)
		if err != nil {
			return fmt.Errorf("skiplist: encoding key: %w", err)
		}
		valueText, err := format.Value.Encode(entry.Value)
		if err != nil {
			return fmt.Errorf("skiplist: encoding value of key %q: %w", keyText, err)
		}

		record.Reset()
		record.WriteString(escapeField(keyText, format.Separator))
		record.WriteString(separator)
		record.WriteString(escapeField(valueText, format.Separator))
		record.WriteByte(recordTerminator)

		if _, err := io.WriteString(w, record.String()); err != nil {
			return fmt.Errorf("skiplist: writing record: %w", err)
		}
	}
	return nil
}

// DumpFile writes the list to path, replacing any existing file atomically.
//
// The data is written to a temporary file in the destination directory,
// flushed, synced to stable storage and only then renamed over path, so a
// crash or a full disk can never leave a half written dump in place. Missing
// parent directories are created. The temporary file is removed when any step
// fails.
func (s *SkipList[K, V]) DumpFile(path string, format Format[K, V]) (err error) {
	s.requireUsable()
	normalized, err := format.normalize()
	if err != nil {
		return err
	}

	dir := filepath.Dir(path)
	if err = os.MkdirAll(dir, dumpDirMode); err != nil {
		return fmt.Errorf("skiplist: creating directory %s: %w", dir, err)
	}

	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("skiplist: creating temporary file in %s: %w", dir, err)
	}
	tempPath := temp.Name()
	defer func() {
		if err != nil {
			// Best effort: the file may already be closed or renamed away.
			_ = temp.Close()
			_ = os.Remove(tempPath)
		}
	}()

	buffered := bufio.NewWriter(temp)
	if err = s.Dump(buffered, normalized); err != nil {
		return err
	}
	if err = buffered.Flush(); err != nil {
		return fmt.Errorf("skiplist: flushing %s: %w", tempPath, err)
	}
	if err = temp.Chmod(dumpFileMode); err != nil {
		return fmt.Errorf("skiplist: setting the mode of %s: %w", tempPath, err)
	}
	if err = temp.Sync(); err != nil {
		return fmt.Errorf("skiplist: syncing %s: %w", tempPath, err)
	}
	if err = temp.Close(); err != nil {
		return fmt.Errorf("skiplist: closing %s: %w", tempPath, err)
	}
	if err = os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("skiplist: replacing %s: %w", path, err)
	}
	syncDir(dir)
	return nil
}

// syncDir flushes a directory entry so that a rename survives a crash. It is
// best effort: platforms that do not support syncing a directory simply ignore
// the failure.
func syncDir(dir string) {
	handle, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = handle.Sync()
	_ = handle.Close()
}

// Load reads records from r and returns a new skip list ordered by the
// standard comparison operators.
//
// Blank lines are skipped. Any other malformed record aborts the load and is
// reported as a *ParseError carrying the one-based line number, so a corrupt
// dump is never partially imported without the caller noticing.
func Load[K cmp.Ordered, V any](r io.Reader, format Format[K, V], opts ...Option) (*SkipList[K, V], error) {
	return LoadWithComparator(func(a, b K) int { return cmp.Compare(a, b) }, r, format, opts...)
}

// LoadWithComparator behaves like Load but orders the keys with a custom
// comparator, which must not be nil.
func LoadWithComparator[K, V any](compare func(a, b K) int, r io.Reader, format Format[K, V], opts ...Option) (*SkipList[K, V], error) {
	if compare == nil {
		return nil, errors.New("skiplist: comparator must not be nil")
	}
	normalized, err := format.normalize()
	if err != nil {
		return nil, err
	}
	cfg, err := newConfig(opts...)
	if err != nil {
		return nil, err
	}
	list := newSkipList[K, V](compare, cfg)
	if err := list.load(r, normalized); err != nil {
		return nil, err
	}
	return list, nil
}

// LoadFile reads records from the file at path and returns a new skip list
// ordered by the standard comparison operators.
func LoadFile[K cmp.Ordered, V any](path string, format Format[K, V], opts ...Option) (*SkipList[K, V], error) {
	return LoadFileWithComparator(func(a, b K) int { return cmp.Compare(a, b) }, path, format, opts...)
}

// LoadFileWithComparator behaves like LoadFile but orders the keys with a
// custom comparator, which must not be nil.
func LoadFileWithComparator[K, V any](compare func(a, b K) int, path string, format Format[K, V], opts ...Option) (*SkipList[K, V], error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("skiplist: opening %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	list, err := LoadWithComparator(compare, file, format, opts...)
	if err != nil {
		return nil, fmt.Errorf("skiplist: loading %s: %w", path, err)
	}
	return list, nil
}

// load parses every record of r into an empty list.
func (s *SkipList[K, V]) load(r io.Reader, format Format[K, V]) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, bufio.MaxScanTokenSize), MaxRecordSize)

	separator := string(rune(format.Separator))
	for line := 1; scanner.Scan(); line++ {
		record := scanner.Text()
		if record == "" {
			// Tolerate blank padding lines, which text editors and log
			// rotation commonly introduce.
			continue
		}
		if err := s.loadRecord(record, format, separator); err != nil {
			return &ParseError{Line: line, Record: record, Err: err}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("skiplist: reading records: %w", err)
	}
	return nil
}

// loadRecord parses and stores a single line.
func (s *SkipList[K, V]) loadRecord(record string, format Format[K, V], separator string) error {
	keyText, valueText, found := splitRecord(record, format.Separator)
	if !found {
		return fmt.Errorf("%w: no %q separator", ErrMalformedRecord, separator)
	}

	rawKey, err := unescapeField(keyText, format.Separator)
	if err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}
	rawValue, err := unescapeField(valueText, format.Separator)
	if err != nil {
		return fmt.Errorf("invalid value: %w", err)
	}

	key, err := format.Key.Decode(rawKey)
	if err != nil {
		return fmt.Errorf("decoding key %s: %w", summarise(rawKey), err)
	}
	value, err := format.Value.Decode(rawValue)
	if err != nil {
		return fmt.Errorf("decoding value %s: %w", summarise(rawValue), err)
	}

	// Insert takes the write lock, so the random source is only read while
	// that lock is held, matching the contract of WithRandSource. The list
	// under construction is not yet visible to other goroutines, but the
	// source itself may be shared with another list.
	if !s.Insert(key, value) {
		return fmt.Errorf("%w: %s", ErrDuplicateKey, summarise(rawKey))
	}
	return nil
}
