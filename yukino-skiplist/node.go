package skiplist

// node is a single element of the skip list.
//
// forward holds one successor pointer per level the node participates in, so
// len(forward) is the height of the node and forward[0] is always the
// bottom-level successor. A nil successor marks the end of that level.
//
// A node is created once and mutated only while the owning SkipList holds its
// exclusive lock; readers observe it under the shared lock.
type node[K, V any] struct {
	key     K
	value   V
	forward []*node[K, V]
}

// newNode allocates a node of the given height. Height must be at least one.
func newNode[K, V any](key K, value V, height int) *node[K, V] {
	return &node[K, V]{
		key:     key,
		value:   value,
		forward: make([]*node[K, V], height),
	}
}

// headerNode allocates the sentinel that anchors every level of the list. Its
// key and value are the zero values of K and V and are never exposed to
// callers.
func headerNode[K, V any](maxLevel int) *node[K, V] {
	var (
		zeroKey   K
		zeroValue V
	)
	return newNode(zeroKey, zeroValue, maxLevel)
}
