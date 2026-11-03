package skiplist

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
)

const (
	// DefaultMaxLevel is the level cap applied when a constructor is called
	// without WithMaxLevel. With DefaultProbability it comfortably supports
	// lists of more than 65 thousand elements before the expected height
	// saturates, which is ample for most workloads.
	DefaultMaxLevel = 16

	// DefaultProbability is the chance that a node promoted to level i is also
	// promoted to level i+1. A value of one half yields the classic skip list
	// with an expected two forward pointers per element.
	DefaultProbability = 0.5

	// MaxLevelLimit is the largest height WithMaxLevel accepts. The header keeps
	// one successor pointer per lane. Several thousand lanes already exceed
	// anything a geometric height distribution can populate, and rejecting a
	// larger value avoids turning a mistaken argument into a huge allocation.
	MaxLevelLimit = 4096

	// stackUpdateLimit is the largest maximum level for which insert and delete
	// keep their scratch array of predecessor pointers on the stack. Taller
	// lists, up to MaxLevelLimit, fall back to a heap allocation.
	stackUpdateLimit = 64
)

// config collects the tunables shared by every constructor.
type config struct {
	maxLevel    int
	probability float64
	nextFloat   func() float64
}

func defaultConfig() config {
	return config{
		maxLevel:    DefaultMaxLevel,
		probability: DefaultProbability,
		// The package level source of math/rand/v2 is safe for concurrent use
		// and is seeded from the runtime, so the default configuration needs no
		// explicit seeding.
		nextFloat: rand.Float64,
	}
}

// Option configures a skip list created by New, NewWithComparator, Load,
// LoadFile or any of their comparator variants.
//
// Options are applied in order; the last value supplied for a given setting
// wins.
type Option func(*config)

// WithMaxLevel sets the tallest height a node may reach. It must lie in the
// closed interval [1, MaxLevelLimit]. The value counts lanes, including the
// bottom lane. The C++ constructor instead took the highest level index and
// therefore allocated one extra lane for the same numeric argument.
//
// Higher values reduce the expected number of hops per lookup at the cost of
// additional memory, and are only useful for very large lists or for a
// promotion probability close to one.
func WithMaxLevel(maxLevel int) Option {
	return func(cfg *config) { cfg.maxLevel = maxLevel }
}

// WithProbability sets the chance that a node present on level i is also
// promoted to level i+1. It must be a real number in the open interval (0, 1);
// NaN and infinities are rejected. The classic choices are one half, which
// minimises the expected number of pointers traversed per lookup, and one
// quarter, which minimises the expected number of pointer words stored per
// element.
func WithProbability(probability float64) Option {
	return func(cfg *config) { cfg.probability = probability }
}

// WithRandSource replaces the default random source used to draw node heights.
// Supplying a source seeded with a fixed value makes the shape of the list
// reproducible, which is useful in tests and benchmarks.
//
// The source is only ever read while the skip list holds its exclusive lock,
// so it does not need to be safe for concurrent use by itself. It must not be
// shared between two skip lists that may be mutated at the same time unless it
// is itself goroutine safe.
func WithRandSource(source *rand.Rand) Option {
	return func(cfg *config) {
		if source == nil {
			cfg.nextFloat = nil
			return
		}
		cfg.nextFloat = source.Float64
	}
}

// newConfig applies opts to the defaults and validates the result.
func newConfig(opts ...Option) (config, error) {
	cfg := defaultConfig()
	for index, opt := range opts {
		if opt == nil {
			return config{}, fmt.Errorf("skiplist: option %d is nil", index)
		}
		opt(&cfg)
	}
	switch {
	case cfg.maxLevel < 1 || cfg.maxLevel > MaxLevelLimit:
		return config{}, fmt.Errorf("skiplist: max level must be in [1, %d], got %d", MaxLevelLimit, cfg.maxLevel)
	case math.IsNaN(cfg.probability) || cfg.probability <= 0 || cfg.probability >= 1:
		return config{}, fmt.Errorf("skiplist: probability must lie in the open interval (0, 1), got %v", cfg.probability)
	case cfg.nextFloat == nil:
		return config{}, errors.New("skiplist: random source must not be nil")
	}
	return cfg, nil
}

// mustConfig is newConfig for the constructors that cannot report an error.
// Invalid options are programmer errors, so they are surfaced as a panic in
// the same spirit as regexp.MustCompile.
func mustConfig(opts ...Option) config {
	cfg, err := newConfig(opts...)
	if err != nil {
		panic(err)
	}
	return cfg
}
