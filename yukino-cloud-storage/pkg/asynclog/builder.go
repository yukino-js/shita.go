package asynclog

import "fmt"

// Builder constructs Logger values in a readable, step-by-step fashion. It is
// the Go equivalent of the C++ LoggerBuilder.
type Builder struct {
	name       string
	minLevel   Level
	asyncType  AsyncType
	growth     Growth
	hasGrowth  bool
	flushMode  FlushMode
	flushers   []Flusher
	backup     BackupSink
	includeGID *bool
}

// NewBuilder returns an empty builder with safe defaults.
func NewBuilder() *Builder {
	return &Builder{asyncType: AsyncSafe, flushMode: FlushBuffered}
}

// Name sets the logger name. It is required.
func (b *Builder) Name(name string) *Builder { b.name = name; return b }

// Level sets the minimum level that will be emitted.
func (b *Builder) Level(l Level) *Builder { b.minLevel = l; return b }

// AsyncType selects bounded (blocking) or unbounded (growing) buffering.
func (b *Builder) AsyncType(t AsyncType) *Builder { b.asyncType = t; return b }

// Growth sets the buffer growth policy.
func (b *Builder) Growth(g Growth) *Builder { b.growth = g; b.hasGrowth = true; return b }

// FlushMode sets how aggressively bytes are pushed toward stable storage.
func (b *Builder) FlushMode(m FlushMode) *Builder { b.flushMode = m; return b }

// Flusher appends an output sink.
func (b *Builder) Flusher(f Flusher) *Builder { b.flushers = append(b.flushers, f); return b }

// Backup attaches a sink that receives ERROR and FATAL records.
func (b *Builder) Backup(s BackupSink) *Builder { b.backup = s; return b }

// IncludeGoroutineID toggles the goroutine id in each record.
func (b *Builder) IncludeGoroutineID(v bool) *Builder { b.includeGID = &v; return b }

// FromConfig seeds the builder from a Config, applying buffer growth, flush
// mode, level and goroutine-id settings. Individual settings may still be
// overridden afterwards.
func (b *Builder) FromConfig(c *Config) *Builder {
	c = c.WithDefaults()
	b.growth = c.Growth()
	b.hasGrowth = true
	b.flushMode = c.FlushModeValue()
	b.minLevel = c.MinLevel()
	if c.IncludeGoroutineID != nil {
		b.includeGID = c.IncludeGoroutineID
	}
	return b
}

// Build validates the accumulated options and returns a ready Logger whose
// worker goroutine is already running.
func (b *Builder) Build() (*Logger, error) {
	if b.name == "" {
		return nil, fmt.Errorf("asynclog: logger name is required")
	}
	flushers := b.flushers
	if len(flushers) == 0 {
		flushers = []Flusher{NewStdoutFlusher(b.flushMode)}
	}
	growth := b.growth
	if !b.hasGrowth {
		growth = DefaultGrowth()
	}
	includeGID := true
	if b.includeGID != nil {
		includeGID = *b.includeGID
	}
	l := &Logger{
		name:       b.name,
		minLevel:   b.minLevel,
		includeGID: includeGID,
		flushers:   flushers,
		backup:     b.backup,
	}
	l.worker = NewWorker(l.realFlush, b.asyncType, growth)
	return l, nil
}
