package asynclog

import (
	"fmt"
	"sync"
)

// Manager owns a set of named loggers plus a default logger. It is safe for
// concurrent use.
type Manager struct {
	mu       sync.RWMutex
	loggers  map[string]*Logger
	defaultL *Logger
}

// NewManager returns a manager whose default logger writes to standard output.
func NewManager() *Manager {
	def, _ := NewBuilder().Name("default").Build()
	return &Manager{
		loggers:  map[string]*Logger{"default": def},
		defaultL: def,
	}
}

// Add registers a logger. It returns an error if the name is already taken,
// avoiding the check-then-insert race of the original implementation.
func (m *Manager) Add(l *Logger) error {
	if l == nil {
		return fmt.Errorf("asynclog: cannot add nil logger")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.loggers[l.Name()]; exists {
		return fmt.Errorf("asynclog: logger %q already registered", l.Name())
	}
	m.loggers[l.Name()] = l
	return nil
}

// Get returns the logger registered under name, or nil if there is none.
func (m *Manager) Get(name string) *Logger {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.loggers[name]
}

// Default returns the default logger.
func (m *Manager) Default() *Logger {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.defaultL
}

// Close stops every registered logger and flushes their sinks.
func (m *Manager) Close() error {
	m.mu.Lock()
	loggers := make([]*Logger, 0, len(m.loggers))
	for _, l := range m.loggers {
		loggers = append(loggers, l)
	}
	m.mu.Unlock()
	var firstErr error
	for _, l := range loggers {
		if err := l.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// defaultManager is the process-wide manager used by the package-level helpers.
var defaultManager = NewManager()

// DefaultManager returns the process-wide logger manager.
func DefaultManager() *Manager { return defaultManager }

// GetLogger returns a logger from the default manager, or nil if absent.
func GetLogger(name string) *Logger { return defaultManager.Get(name) }

// Default returns the default logger of the default manager.
func Default() *Logger { return defaultManager.Default() }

// Register adds a logger to the default manager.
func Register(l *Logger) error { return defaultManager.Add(l) }

// The package-level helpers below log through the default logger. Each helper
// replaces the equivalent method frame rather than adding to it, so the stack
// depth between the helper and Logger.log is the same as between Debugf and
// Logger.log; extraSkip therefore stays zero and the user's call site is
// reported correctly.

// Debugf logs a formatted record at DEBUG level through the default logger.
func Debugf(format string, args ...any) { Default().log(LevelDebug, 0, format, args) }

// Infof logs a formatted record at INFO level through the default logger.
func Infof(format string, args ...any) { Default().log(LevelInfo, 0, format, args) }

// Warnf logs a formatted record at WARN level through the default logger.
func Warnf(format string, args ...any) { Default().log(LevelWarn, 0, format, args) }

// Errorf logs a formatted record at ERROR level through the default logger.
func Errorf(format string, args ...any) { Default().log(LevelError, 0, format, args) }

// Fatalf logs a formatted record at FATAL level through the default logger.
func Fatalf(format string, args ...any) { Default().log(LevelFatal, 0, format, args) }
