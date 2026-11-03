package asynclog

import (
	"bytes"
	"runtime"
	"strconv"
)

// goroutineID returns the numeric id of the calling goroutine.
//
// The Go runtime does not expose the goroutine id through a public API, so it
// is recovered from the first line of a small runtime stack trace. The value is
// used purely for diagnostics and never for synchronization.
func goroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// The first line has the form "goroutine 123 [running]:".
	line := bytes.TrimPrefix(buf[:n], []byte("goroutine "))
	if i := bytes.IndexByte(line, ' '); i > 0 {
		if id, err := strconv.ParseInt(string(line[:i]), 10, 64); err == nil {
			return id
		}
	}
	return -1
}
