package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// The helpers in this file replace the C++ FileUtil class. Compression, which
// the original mixed into the same class, now lives in the bundle package so
// these functions stay focused on the filesystem.

// Exists reports whether path exists.
func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// FileSize returns the size of path in bytes.
func FileSize(path string) (int64, error) {
	st, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// ModTime returns the last modification time of path.
func ModTime(path string) (time.Time, error) {
	st, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}
	return st.ModTime(), nil
}

// EnsureDir creates dir and any missing parents.
func EnsureDir(dir string) error {
	if dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

// SafeFileName sanitises a client-supplied file name so it cannot escape the
// storage directory.
//
// The original implementation concatenated the decoded name straight into the
// destination path, which allowed path traversal such as "../../etc/passwd".
// This strips every directory component, rejects empty and dot-only names, and
// replaces characters that are unsafe on common filesystems.
func SafeFileName(name string) (string, error) {
	name = strings.TrimSpace(name)
	// Reduce to the final element, defeating both "/" and "\" traversal.
	name = filepath.Base(filepath.FromSlash(name))
	name = strings.Trim(name, `/\`)
	name = strings.Map(func(r rune) rune {
		if r < 0x20 {
			return -1
		}
		switch r {
		case '/', '\\', ':', '*', '?', '"', '<', '>', '|':
			return '_'
		}
		return r
	}, name)
	if name == "" || name == "." || name == ".." {
		return "", fmt.Errorf("storage: invalid file name %q", name)
	}
	return name, nil
}

// SafeJoin joins dir and name, guaranteeing the result stays inside dir.
func SafeJoin(dir, name string) (string, error) {
	clean, err := SafeFileName(name)
	if err != nil {
		return "", err
	}
	full := filepath.Join(dir, clean)
	// Defence in depth: confirm the resolved path does not escape dir.
	rel, err := filepath.Rel(dir, full)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("storage: path %q escapes the storage directory", name)
	}
	return full, nil
}

// WriteFileAtomic writes data to path through a temporary file in the same
// directory followed by a rename, so a crash mid-write cannot corrupt an
// existing file. The original wrote in place.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := EnsureDir(dir); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".yukino-tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	renamed := false
	defer func() {
		if !renamed {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	renamed = true
	return nil
}

// ScanFiles lists the regular files directly inside dir, creating it if needed.
func ScanFiles(dir string) ([]string, error) {
	if err := EnsureDir(dir); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		out = append(out, filepath.Join(dir, e.Name()))
	}
	return out, nil
}
