package asynclog

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config captures the tunables of the async logging system. It corresponds to
// the JSON configuration file consumed by the original C++ implementation.
type Config struct {
	// BufferSize is the initial capacity, in bytes, of each async buffer.
	BufferSize int64 `json:"buffer_size"`
	// Threshold is the buffer size below which growth doubles and at or above
	// which growth becomes linear.
	Threshold int64 `json:"threshold"`
	// LinearGrowth is the additive increment used once Threshold is reached.
	LinearGrowth int64 `json:"linear_growth"`
	// FlushLog selects the flush mode: 0 none, 1 buffered, 2 sync.
	FlushLog int `json:"flush_log"`
	// BackupAddr and BackupPort identify the remote log backup receiver used
	// for ERROR and FATAL records. An empty BackupAddr disables backup.
	BackupAddr string `json:"backup_addr"`
	BackupPort int    `json:"backup_port"`
	// ThreadCount is retained for compatibility with the original config file.
	// The Go implementation uses one worker goroutine per logger, so the value
	// is informational only.
	ThreadCount int `json:"thread_count"`

	// Level is the minimum level that will be emitted. Empty means DEBUG.
	Level string `json:"level"`
	// IncludeGoroutineID toggles the goroutine id in each record. Nil means the
	// default, which is true.
	IncludeGoroutineID *bool `json:"include_goroutine_id"`
}

// DefaultConfig returns the configuration used when none is supplied.
func DefaultConfig() *Config {
	return &Config{
		BufferSize:   1 << 20,
		Threshold:    16 << 20,
		LinearGrowth: 1 << 20,
		FlushLog:     1,
		ThreadCount:  1,
		Level:        "DEBUG",
	}
}

// WithDefaults returns a copy of the config with zero-valued fields replaced by
// sensible defaults, so partially specified files still behave predictably.
func (c *Config) WithDefaults() *Config {
	out := *c
	d := DefaultConfig()
	if out.BufferSize <= 0 {
		out.BufferSize = d.BufferSize
	}
	if out.Threshold <= 0 {
		out.Threshold = d.Threshold
	}
	if out.LinearGrowth <= 0 {
		out.LinearGrowth = d.LinearGrowth
	}
	if out.ThreadCount <= 0 {
		out.ThreadCount = d.ThreadCount
	}
	if out.Level == "" {
		out.Level = d.Level
	}
	return &out
}

// Growth translates the buffer tunables into a Growth policy.
func (c *Config) Growth() Growth {
	return Growth{Base: c.BufferSize, Threshold: c.Threshold, Linear: c.LinearGrowth}
}

// FlushModeValue translates the legacy flush_log integer.
func (c *Config) FlushModeValue() FlushMode { return ParseFlushMode(c.FlushLog) }

// MinLevel parses the configured minimum level, defaulting to DEBUG.
func (c *Config) MinLevel() Level {
	if c.Level == "" {
		return LevelDebug
	}
	l, err := ParseLevel(c.Level)
	if err != nil {
		return LevelDebug
	}
	return l
}

// LoadConfig reads and decodes a JSON configuration file. Unlike the original
// implementation the path is supplied explicitly rather than hard coded.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("asynclog: read config: %w", err)
	}
	cfg := DefaultConfig()
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("asynclog: parse config %s: %w", path, err)
	}
	return cfg.WithDefaults(), nil
}
