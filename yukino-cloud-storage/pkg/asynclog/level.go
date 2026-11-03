package asynclog

import (
	"fmt"
	"strings"
)

// Level represents the severity of a log record.
type Level int32

// Log severity levels, in increasing order of importance.
const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

// levelNames maps each level to its canonical upper-case name.
var levelNames = [...]string{
	LevelDebug: "DEBUG",
	LevelInfo:  "INFO",
	LevelWarn:  "WARN",
	LevelError: "ERROR",
	LevelFatal: "FATAL",
}

// String returns the canonical upper-case name of the level.
func (l Level) String() string {
	if l >= LevelDebug && int(l) < len(levelNames) {
		return levelNames[l]
	}
	return "UNKNOWN"
}

// MarshalText implements encoding.TextMarshaler so a level can be embedded in
// JSON or other text encodings.
func (l Level) MarshalText() ([]byte, error) {
	return []byte(l.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (l *Level) UnmarshalText(text []byte) error {
	parsed, err := ParseLevel(string(text))
	if err != nil {
		return err
	}
	*l = parsed
	return nil
}

// ParseLevel converts a textual level name (case-insensitive) into a Level.
func ParseLevel(name string) (Level, error) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "DEBUG":
		return LevelDebug, nil
	case "INFO":
		return LevelInfo, nil
	case "WARN", "WARNING":
		return LevelWarn, nil
	case "ERROR":
		return LevelError, nil
	case "FATAL":
		return LevelFatal, nil
	default:
		return LevelDebug, fmt.Errorf("asynclog: unknown level %q", name)
	}
}
