// Package skiplist implements a concurrent, ordered key/value map backed by a
// probabilistic skip list.
//
// # Overview
//
// A skip list is a layered linked list. Every element appears on level 0 and,
// with geometric probability, on progressively higher levels. Lookups descend
// from the tallest level, skipping long runs of keys, and therefore run in
// O(log n) expected time without the rebalancing bookkeeping a tree requires.
//
// The implementation is generic over both the key and the value type. Keys may
// be ordered by the standard [cmp.Ordered] constraint or by an arbitrary
// comparator supplied to [NewWithComparator], which makes reverse ordering,
// case-insensitive strings and composite keys straightforward.
//
// # Operations
//
// The table below summarises the expected cost of each operation, where n is
// the number of stored elements and L is the configured maximum level:
//
//	Insert / Set / Delete / Get / Has   O(log n) expected, O(n) worst case
//	Len / Level / Min                   O(1)
//	Max / Floor / Ceiling               O(log n) expected
//	All / Range / RangeFrom             O(k) for k yielded elements
//	Snapshot / String / Dump            O(n)
//
// # Concurrency
//
// A SkipList is safe for concurrent use by multiple goroutines. Mutators take
// an exclusive lock while accessors take a shared one, so readers never block
// each other. This is a deliberate correction of the original C++ design, in
// which the search path ran without any synchronisation at all.
//
// The iterators returned by [SkipList.All], [SkipList.Range] and
// [SkipList.RangeFrom] hold the shared lock for the whole range statement. The
// range body must therefore not call back into the same list, or it will
// deadlock. When the body needs to mutate the list, iterate over
// [SkipList.Snapshot] instead.
//
// # Persistence
//
// [SkipList.Dump] and [SkipList.DumpFile] serialise the list into a line
// oriented text format, and [Load] and [LoadFile] read it back. Each record is
// a single line:
//
//	<key>:<value>\n
//
// Both fields are escaped with backslashes so that a separator, a backslash, a
// carriage return or a line feed inside a field survives unchanged. Parsing
// splits on the first separator that is not itself escaped; cutting on the raw
// byte would divide a key such as "a:b" in half. [SkipList.Dump] copies the
// entries under the shared lock and releases that lock before encoding or
// writing, so a slow or re-entrant writer does not stall the list.
// [SkipList.DumpFile] writes the snapshot to a temporary file in the
// destination directory, flushes it and renames it over the target. A crash
// mid-write leaves the previous file intact.
//
// [Load] rejects a malformed record, an invalid escape, a value the codec
// cannot parse, or a duplicate key, and it discards the partial list. The C++
// loader reused the previous key when a line had no separator, dropped empty
// values, and parsed every key with stoi.
//
// # Relationship to the C++ original
//
// This package is a Go port of the Skiplist-CPP project by Carl, which is
// distributed under the GNU General Public License version 3. The licence of
// this derivative work is unchanged; see the LICENSE file and the "Defects
// corrected from Skiplist-CPP" section of the README for the list of defects
// that were fixed during the port.
package skiplist
