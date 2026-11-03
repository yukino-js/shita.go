package skiplist

import (
	"fmt"
	"iter"
	"strconv"
	"strings"
)

// Entry is a single key/value pair materialised by Snapshot.
type Entry[K, V any] struct {
	Key   K
	Value V
}

// All returns an iterator over every element in ascending key order.
//
// The list is held under its shared lock for the whole range statement, so the
// range body must not call back into the same list or it will deadlock. Use
// Snapshot when the body needs to mutate the list.
func (s *SkipList[K, V]) All() iter.Seq2[K, V] {
	s.requireUsable()
	return func(yield func(K, V) bool) {
		s.mu.RLock()
		defer s.mu.RUnlock()
		for current := s.header.forward[0]; current != nil; current = current.forward[0] {
			if !yield(current.key, current.value) {
				return
			}
		}
	}
}

// Range returns an iterator over the half-open key interval [from, to) in
// ascending order. It is a no-op when from does not sort before to.
//
// The locking contract is the same as All.
func (s *SkipList[K, V]) Range(from, to K) iter.Seq2[K, V] {
	s.requireUsable()
	return func(yield func(K, V) bool) {
		s.mu.RLock()
		defer s.mu.RUnlock()
		current := s.descend(from, nil).forward[0]
		for current != nil && s.compare(current.key, to) < 0 {
			if !yield(current.key, current.value) {
				return
			}
			current = current.forward[0]
		}
	}
}

// RangeFrom returns an iterator over every element whose key is greater than or
// equal to from, in ascending order.
//
// The locking contract is the same as All.
func (s *SkipList[K, V]) RangeFrom(from K) iter.Seq2[K, V] {
	s.requireUsable()
	return func(yield func(K, V) bool) {
		s.mu.RLock()
		defer s.mu.RUnlock()
		for current := s.descend(from, nil).forward[0]; current != nil; current = current.forward[0] {
			if !yield(current.key, current.value) {
				return
			}
		}
	}
}

// Keys returns an iterator over the keys in ascending order. The locking
// contract is the same as All.
func (s *SkipList[K, V]) Keys() iter.Seq[K] {
	s.requireUsable()
	return func(yield func(K) bool) {
		for key := range s.All() {
			if !yield(key) {
				return
			}
		}
	}
}

// Values returns an iterator over the values in ascending key order. The
// locking contract is the same as All.
func (s *SkipList[K, V]) Values() iter.Seq[V] {
	s.requireUsable()
	return func(yield func(V) bool) {
		for _, value := range s.All() {
			if !yield(value) {
				return
			}
		}
	}
}

// Min returns the smallest element and whether the list is non-empty.
func (s *SkipList[K, V]) Min() (key K, value V, ok bool) {
	s.requireUsable()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if first := s.header.forward[0]; first != nil {
		return first.key, first.value, true
	}
	return key, value, false
}

// Max returns the largest element and whether the list is non-empty.
func (s *SkipList[K, V]) Max() (key K, value V, ok bool) {
	s.requireUsable()
	s.mu.RLock()
	defer s.mu.RUnlock()
	current := s.header
	for level := s.level - 1; level >= 0; level-- {
		for current.forward[level] != nil {
			current = current.forward[level]
		}
	}
	if current == s.header {
		return key, value, false
	}
	return current.key, current.value, true
}

// Floor returns the element with the greatest key that sorts less than or
// equal to key, and whether such an element exists.
//
// An exact match wins over the strict predecessor. descend stops on the node
// before the first key that sorts at or after the probe, so returning that
// predecessor unconditionally drops an equal key whenever a smaller one exists.
func (s *SkipList[K, V]) Floor(key K) (foundKey K, foundValue V, ok bool) {
	s.requireUsable()
	s.mu.RLock()
	defer s.mu.RUnlock()
	predecessor := s.descend(key, nil)
	if next := predecessor.forward[0]; next != nil && s.compare(next.key, key) == 0 {
		return next.key, next.value, true
	}
	if predecessor != s.header {
		return predecessor.key, predecessor.value, true
	}
	return foundKey, foundValue, false
}

// Ceiling returns the element with the smallest key that is greater than or
// equal to key, and whether such an element exists.
func (s *SkipList[K, V]) Ceiling(key K) (foundKey K, foundValue V, ok bool) {
	s.requireUsable()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if candidate := s.descend(key, nil).forward[0]; candidate != nil {
		return candidate.key, candidate.value, true
	}
	return foundKey, foundValue, false
}

// Snapshot returns a detached copy of every element in ascending key order.
// The result is safe to inspect or iterate while continuing to mutate the
// list, at the cost of one allocation per element.
func (s *SkipList[K, V]) Snapshot() []Entry[K, V] {
	s.requireUsable()
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := make([]Entry[K, V], 0, s.length)
	for current := s.header.forward[0]; current != nil; current = current.forward[0] {
		entries = append(entries, Entry[K, V]{Key: current.key, Value: current.value})
	}
	return entries
}

// String renders the list level by level, tallest level first, and implements
// [fmt.Stringer]. It is intended for diagnostics; the exact layout is not a
// stable interface.
func (s *SkipList[K, V]) String() string {
	s.requireUsable()
	s.mu.RLock()
	defer s.mu.RUnlock()

	var builder strings.Builder
	fmt.Fprintf(&builder, "SkipList(len=%d, level=%d, maxLevel=%d, probability=%.2f)\n",
		s.length, s.level, s.maxLevel, s.probability)

	labelWidth := len(strconv.Itoa(max(s.level-1, 0)))
	for level := s.level - 1; level >= 0; level-- {
		fmt.Fprintf(&builder, "L%0*d |", labelWidth, level)
		for current := s.header.forward[level]; current != nil; current = current.forward[level] {
			fmt.Fprintf(&builder, " %v:%v ->", current.key, current.value)
		}
		builder.WriteString(" nil\n")
	}
	return builder.String()
}
