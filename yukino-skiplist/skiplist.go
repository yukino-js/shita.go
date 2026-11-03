package skiplist

import (
	"cmp"
	"sync"
)

// SkipList is an ordered map from keys of type K to values of type V backed by
// a probabilistic skip list. It supports insertion, lookup, deletion and
// ordered iteration in O(log n) expected time and is safe for concurrent use
// by multiple goroutines.
//
// The zero value is not usable. Every exported method panics if it is called
// on a nil pointer or on a SkipList that was not constructed with New or
// NewWithComparator; the panic names those constructors.
//
// Because SkipList embeds a sync.RWMutex it must not be copied after first
// use; pass it around by pointer. go vet reports such a copy.
type SkipList[K, V any] struct {
	compare     func(a, b K) int
	maxLevel    int
	probability float64
	nextFloat   func() float64
	// ready is set once, before the constructor returns, and is never cleared.
	// Exported methods read it without holding mu. That is race-free because
	// the write happens before the pointer is published and no later write
	// exists. A zero SkipList has ready == false.
	ready bool

	// mu guards every field below it as well as the node graph reachable from
	// header. Mutators take the exclusive lock and accessors take the shared
	// one.
	mu     sync.RWMutex
	header *node[K, V]
	// level is the height of the tallest occupied level; it is zero while the
	// list is empty.
	level int
	// length is the number of stored elements.
	length int
}

// New returns an empty skip list whose keys are ordered with the standard
// comparison operators, as exposed by the [cmp.Ordered] constraint.
//
// New panics if any option is invalid; see the Option constructors for the
// accepted ranges.
func New[K cmp.Ordered, V any](opts ...Option) *SkipList[K, V] {
	return NewWithComparator[K, V](func(a, b K) int { return cmp.Compare(a, b) }, opts...)
}

// NewWithComparator returns an empty skip list ordered by compare, which must
// return a negative number when a sorts before b, zero when they are equal and
// a positive number when a sorts after b. Passing a nil comparator panics.
//
// Use this constructor for key types that are not [cmp.Ordered], or to obtain
// a non-natural ordering such as descending or case-insensitive.
func NewWithComparator[K, V any](compare func(a, b K) int, opts ...Option) *SkipList[K, V] {
	if compare == nil {
		panic("skiplist: comparator must not be nil")
	}
	return newSkipList[K, V](compare, mustConfig(opts...))
}

// newSkipList builds a list from an already validated configuration.
func newSkipList[K, V any](compare func(a, b K) int, cfg config) *SkipList[K, V] {
	return &SkipList[K, V]{
		compare:     compare,
		maxLevel:    cfg.maxLevel,
		probability: cfg.probability,
		nextFloat:   cfg.nextFloat,
		ready:       true,
		header:      headerNode[K, V](cfg.maxLevel),
	}
}

const (
	nilListPanic  = "skiplist: method called on a nil SkipList; construct one with New or NewWithComparator"
	zeroListPanic = "skiplist: method called on a zero SkipList; construct one with New or NewWithComparator"
)

// requireUsable rejects a nil or zero SkipList before the caller touches the
// node graph. It does not lock.
func (s *SkipList[K, V]) requireUsable() {
	if s == nil {
		panic(nilListPanic)
	}
	if !s.ready {
		panic(zeroListPanic)
	}
}

// Len returns the number of elements stored.
func (s *SkipList[K, V]) Len() int {
	s.requireUsable()
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.length
}

// Level returns the height of the tallest occupied level, or zero when the
// list is empty. It is primarily useful for diagnostics.
func (s *SkipList[K, V]) Level() int {
	s.requireUsable()
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.level
}

// MaxLevel returns the configured level cap. The value is the number of
// lanes, including the bottom lane.
func (s *SkipList[K, V]) MaxLevel() int {
	s.requireUsable()
	return s.maxLevel
}

// Probability returns the configured promotion probability.
func (s *SkipList[K, V]) Probability() float64 {
	s.requireUsable()
	return s.probability
}

// descend walks down from the tallest occupied level and returns the last node
// whose key sorts strictly before key; the sentinel header is returned when no
// such node exists. When update is non-nil it additionally receives, for every
// level below the current height, the node whose successor pointer must be
// rewritten to splice a node with the given key into, or out of, that level.
//
// The caller must hold at least the shared lock.
func (s *SkipList[K, V]) descend(key K, update []*node[K, V]) *node[K, V] {
	current := s.header
	for level := s.level - 1; level >= 0; level-- {
		for {
			next := current.forward[level]
			if next == nil || s.compare(next.key, key) >= 0 {
				break
			}
			current = next
		}
		if update != nil {
			update[level] = current
		}
	}
	return current
}

// find returns the node holding key, or nil when key is absent.
//
// The caller must hold at least the shared lock.
func (s *SkipList[K, V]) find(key K) *node[K, V] {
	next := s.descend(key, nil).forward[0]
	if next != nil && s.compare(next.key, key) == 0 {
		return next
	}
	return nil
}

// randomLevel draws the height of a freshly inserted node. The result is at
// least one and never exceeds the configured maximum level. A height of one
// places the node on the bottom lane only.
//
// The C++ original started its draw at one and then linked every index from
// zero through that draw, so the bottom two lanes always held the same nodes.
// It also kept flipping coins until it lost, and only afterwards clamped the
// result, so the loop had no local upper bound.
//
// The caller must hold the exclusive lock, which also serialises access to the
// random source.
func (s *SkipList[K, V]) randomLevel() int {
	level := 1
	for level < s.maxLevel && s.nextFloat() < s.probability {
		level++
	}
	return level
}

// put inserts key with value, or updates the existing element when overwrite
// is set. It reports the previous value and whether the key was already
// present.
//
// The caller must hold the exclusive lock.
func (s *SkipList[K, V]) put(key K, value V, overwrite bool) (previous V, existed bool) {
	// The predecessor scratch array lives on the stack for the common case and
	// only escapes to the heap for unusually tall lists.
	var update []*node[K, V]
	if s.maxLevel <= stackUpdateLimit {
		var buffer [stackUpdateLimit]*node[K, V]
		update = buffer[:s.maxLevel]
	} else {
		update = make([]*node[K, V], s.maxLevel)
	}

	predecessor := s.descend(key, update)
	if next := predecessor.forward[0]; next != nil && s.compare(next.key, key) == 0 {
		previous = next.value
		existed = true
		if overwrite {
			next.value = value
		}
		return previous, existed
	}

	height := s.randomLevel()
	if height > s.level {
		// Levels above the current height are anchored at the header.
		for level := s.level; level < height; level++ {
			update[level] = s.header
		}
		s.level = height
	}

	inserted := newNode(key, value, height)
	for level := 0; level < height; level++ {
		inserted.forward[level] = update[level].forward[level]
		update[level].forward[level] = inserted
	}
	s.length++
	return previous, existed
}

// Insert stores value under key but leaves an existing element untouched. It
// reports whether the element was added. This matches the C++ insert, which
// refused to replace a value; use Set when the new value should win.
func (s *SkipList[K, V]) Insert(key K, value V) bool {
	s.requireUsable()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.put(key, value, false)
	return !existed
}

// Set stores value under key, replacing any existing element. It reports the
// previous value and whether the key was already present, mirroring the
// LoadOrStore family of the standard library's sync.Map.
func (s *SkipList[K, V]) Set(key K, value V) (previous V, existed bool) {
	s.requireUsable()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.put(key, value, true)
}

// Get returns the value stored under key and whether it was found.
func (s *SkipList[K, V]) Get(key K) (V, bool) {
	s.requireUsable()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if found := s.find(key); found != nil {
		return found.value, true
	}
	var zero V
	return zero, false
}

// Has reports whether key is present.
func (s *SkipList[K, V]) Has(key K) bool {
	s.requireUsable()
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.find(key) != nil
}

// Delete removes key and reports the value that was stored under it and
// whether anything was removed. A missing key yields the zero value and false.
func (s *SkipList[K, V]) Delete(key K) (value V, deleted bool) {
	s.requireUsable()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.length == 0 {
		return value, false
	}
	var update []*node[K, V]
	if s.maxLevel <= stackUpdateLimit {
		var buffer [stackUpdateLimit]*node[K, V]
		update = buffer[:s.maxLevel]
	} else {
		update = make([]*node[K, V], s.maxLevel)
	}

	victim := s.descend(key, update).forward[0]
	if victim == nil || s.compare(victim.key, key) != 0 {
		return value, false
	}
	for level := 0; level < s.level; level++ {
		if update[level].forward[level] != victim {
			// The victim does not participate in this level.
			break
		}
		update[level].forward[level] = victim.forward[level]
	}
	// Detach the victim's successors so the garbage collector can reclaim the
	// rest of the chain without holding on to the removed node.
	victim.forward = nil
	for s.level > 0 && s.header.forward[s.level-1] == nil {
		s.level--
	}
	value = victim.value
	deleted = true
	s.length--
	return value, deleted
}

// Clear removes every element, leaving an empty list. Unlike the recursive
// teardown of the C++ original it runs in constant time and cannot exhaust the
// stack, because the garbage collector reclaims the detached nodes.
func (s *SkipList[K, V]) Clear() {
	s.requireUsable()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.header = headerNode[K, V](s.maxLevel)
	s.level = 0
	s.length = 0
}
