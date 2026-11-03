package skiplist

import (
	"cmp"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// wordSource is a math/rand/v2 Source that repeats one word. A zero word makes
// Float64 return 0, so every node is promoted to the level cap. The maximum
// word makes Float64 return the greatest value in [0, 1), so nothing is
// promoted and every node stays on the bottom lane.
type wordSource uint64

func (w wordSource) Uint64() uint64 { return uint64(w) }

func alwaysTall() *rand.Rand  { return rand.New(wordSource(0)) }
func alwaysShort() *rand.Rand { return rand.New(wordSource(^uint64(0))) }

func TestInsertDoesNotOverwrite(t *testing.T) {
	list := New[int, string]()
	if !list.Insert(1, "keep") {
		t.Fatal("Insert of a new key returned false")
	}
	if list.Insert(1, "replace") {
		t.Fatal("Insert overwrote an existing key")
	}
	got, ok := list.Get(1)
	if !ok || got != "keep" {
		t.Fatalf("Get(1) = %q, %t", got, ok)
	}
	if list.Len() != 1 {
		t.Fatalf("Len = %d", list.Len())
	}
	checkInvariants(t, list)
}

func TestSetReplacesAndReportsPrevious(t *testing.T) {
	list := New[int, string]()
	previous, existed := list.Set(1, "first")
	if existed || previous != "" {
		t.Fatalf("Set of a new key = %q, %t", previous, existed)
	}
	previous, existed = list.Set(1, "second")
	if !existed || previous != "first" {
		t.Fatalf("Set of an existing key = %q, %t", previous, existed)
	}
	got, ok := list.Get(1)
	if !ok || got != "second" {
		t.Fatalf("Get(1) = %q, %t", got, ok)
	}

	missing := New[string, struct{ N int }]()
	value, ok := missing.Get("absent")
	if ok || value.N != 0 {
		t.Fatalf("missing value = %+v, %t", value, ok)
	}
}

func TestDelete(t *testing.T) {
	list := New[int, string](WithMaxLevel(4), WithRandSource(alwaysTall()))
	list.Set(1, "a")
	list.Set(2, "b")
	list.Set(3, "c")

	if value, deleted := list.Delete(2); !deleted || value != "b" {
		t.Fatalf("Delete(2) = %q, %t", value, deleted)
	}
	if list.Has(2) {
		t.Fatal("deleted key is still present")
	}
	if value, deleted := list.Delete(2); deleted || value != "" {
		t.Fatalf("Delete of a missing key = %q, %t", value, deleted)
	}
	if list.Len() != 2 {
		t.Fatalf("Len = %d", list.Len())
	}

	list.Delete(1)
	list.Delete(3)
	if list.Len() != 0 || list.Level() != 0 {
		t.Fatalf("empty list len=%d level=%d", list.Len(), list.Level())
	}
	list.Set(4, "d")
	if got, ok := list.Get(4); !ok || got != "d" {
		t.Fatalf("insert after emptying the list = %q, %t", got, ok)
	}
	checkInvariants(t, list)
}

func TestDeleteEdges(t *testing.T) {
	empty := New[int, string]()
	if value, deleted := empty.Delete(1); deleted || value != "" {
		t.Fatalf("Delete on an empty list = %q, %t", value, deleted)
	}

	// maxLevel above stackUpdateLimit forces the predecessor scratch array
	// onto the heap. The linking result must match the stack path.
	list := New[int, string](WithMaxLevel(stackUpdateLimit+1), WithRandSource(alwaysShort()))
	list.Set(1, "a")
	list.Set(2, "b")
	if value, deleted := list.Delete(1); !deleted || value != "a" {
		t.Fatalf("Delete(1) = %q, %t", value, deleted)
	}
	if list.Has(1) || !list.Has(2) || list.Len() != 1 {
		t.Fatalf("len=%d has1=%t has2=%t", list.Len(), list.Has(1), list.Has(2))
	}
	checkInvariants(t, list)
}

func TestClearDropsHeight(t *testing.T) {
	list := New[int, int](WithMaxLevel(6), WithRandSource(alwaysTall()))
	for i := 0; i < 10; i++ {
		list.Set(i, i)
	}
	if list.Level() != 6 {
		t.Fatalf("Level = %d", list.Level())
	}
	list.Clear()
	if list.Len() != 0 || list.Level() != 0 || list.Has(3) {
		t.Fatalf("after Clear len=%d level=%d", list.Len(), list.Level())
	}
	list.Set(1, 1)
	checkInvariants(t, list)
}

func TestFloorAndCeiling(t *testing.T) {
	shapes := []struct {
		name string
		opts []Option
	}{
		{name: "single lane", opts: []Option{WithMaxLevel(1)}},
		{name: "never promoted", opts: []Option{WithMaxLevel(8), WithRandSource(alwaysShort())}},
		{name: "always promoted", opts: []Option{WithMaxLevel(8), WithRandSource(alwaysTall())}},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			list := New[int, string](shape.opts...)
			if _, _, ok := list.Floor(1); ok {
				t.Fatal("Floor on an empty list succeeded")
			}
			if _, _, ok := list.Ceiling(1); ok {
				t.Fatal("Ceiling on an empty list succeeded")
			}
			if _, _, ok := list.Min(); ok {
				t.Fatal("Min on an empty list succeeded")
			}
			if _, _, ok := list.Max(); ok {
				t.Fatal("Max on an empty list succeeded")
			}

			list.Set(1, "a")
			list.Set(5, "b")
			list.Set(10, "c")

			cases := []struct {
				probe    int
				floorKey string
				floorOK  bool
				ceilKey  string
				ceilOK   bool
			}{
				{probe: 0, floorOK: false, ceilKey: "a", ceilOK: true},
				{probe: 1, floorKey: "a", floorOK: true, ceilKey: "a", ceilOK: true},
				{probe: 5, floorKey: "b", floorOK: true, ceilKey: "b", ceilOK: true},
				{probe: 6, floorKey: "b", floorOK: true, ceilKey: "c", ceilOK: true},
				{probe: 10, floorKey: "c", floorOK: true, ceilKey: "c", ceilOK: true},
				{probe: 11, floorKey: "c", floorOK: true, ceilOK: false},
			}
			for _, tc := range cases {
				key, value, ok := list.Floor(tc.probe)
				if ok != tc.floorOK || (ok && (key != map[string]int{"a": 1, "b": 5, "c": 10}[tc.floorKey] || value != tc.floorKey)) {
					t.Fatalf("Floor(%d) = (%d, %q, %t), want value %q ok %t", tc.probe, key, value, ok, tc.floorKey, tc.floorOK)
				}
				key, value, ok = list.Ceiling(tc.probe)
				if ok != tc.ceilOK || (ok && value != tc.ceilKey) {
					t.Fatalf("Ceiling(%d) = (%d, %q, %t), want value %q ok %t", tc.probe, key, value, ok, tc.ceilKey, tc.ceilOK)
				}
			}
			if key, value, ok := list.Min(); !ok || key != 1 || value != "a" {
				t.Fatalf("Min = %d, %q, %t", key, value, ok)
			}
			if key, value, ok := list.Max(); !ok || key != 10 || value != "c" {
				t.Fatalf("Max = %d, %q, %t", key, value, ok)
			}
			checkInvariants(t, list)
		})
	}
}

func TestOrderedViews(t *testing.T) {
	list := New[int, string](WithMaxLevel(5), WithRandSource(alwaysShort()))
	for _, key := range []int{10, 1, 5} {
		list.Set(key, strconv.Itoa(key))
	}

	if got, want := keysOf(list), []int{1, 5, 10}; !slices.Equal(got, want) {
		t.Fatalf("Keys = %v", got)
	}
	var values []string
	for value := range list.Values() {
		values = append(values, value)
	}
	if !slices.Equal(values, []string{"1", "5", "10"}) {
		t.Fatalf("Values = %v", values)
	}

	var ranged []int
	for key := range list.Range(5, 10) {
		ranged = append(ranged, key)
	}
	if !slices.Equal(ranged, []int{5}) {
		t.Fatalf("Range[5, 10) = %v", ranged)
	}
	ranged = nil
	for key := range list.Range(1, 1) {
		ranged = append(ranged, key)
	}
	if ranged != nil {
		t.Fatalf("Range[1, 1) = %v", ranged)
	}
	ranged = nil
	for key := range list.Range(10, 1) {
		ranged = append(ranged, key)
	}
	if ranged != nil {
		t.Fatalf("Range[10, 1) = %v", ranged)
	}

	var from []int
	for key := range list.RangeFrom(5) {
		from = append(from, key)
	}
	if !slices.Equal(from, []int{5, 10}) {
		t.Fatalf("RangeFrom(5) = %v", from)
	}
	from = nil
	for key := range list.RangeFrom(11) {
		from = append(from, key)
	}
	if from != nil {
		t.Fatalf("RangeFrom(11) = %v", from)
	}
}

func TestReverseComparator(t *testing.T) {
	list := NewWithComparator[int, string](
		func(a, b int) int { return cmp.Compare(b, a) },
		WithMaxLevel(4),
		WithRandSource(alwaysShort()),
	)
	list.Set(1, "a")
	list.Set(2, "b")
	list.Set(3, "c")

	if got := keysOf(list); !slices.Equal(got, []int{3, 2, 1}) {
		t.Fatalf("Keys = %v", got)
	}
	if key, _, ok := list.Floor(2); !ok || key != 2 {
		t.Fatalf("Floor(2) = %d, %t", key, ok)
	}
	if _, _, ok := list.Floor(4); ok {
		t.Fatal("Floor(4) found a key that sorts after the probe")
	}
	if key, _, ok := list.Ceiling(4); !ok || key != 3 {
		t.Fatalf("Ceiling(4) = %d, %t", key, ok)
	}
	if got := keysOfRange(list, 3, 1); !slices.Equal(got, []int{3, 2}) {
		t.Fatalf("Range(3, 1) = %v", got)
	}
	checkInvariants(t, list)
}

func TestCaseInsensitiveComparator(t *testing.T) {
	list := NewWithComparator[string, int](func(a, b string) int {
		return strings.Compare(strings.ToLower(a), strings.ToLower(b))
	})
	if !list.Insert("Alpha", 1) {
		t.Fatal("Insert failed")
	}
	if list.Insert("alpha", 2) {
		t.Fatal("case-folded duplicate was inserted")
	}
	got, ok := list.Get("ALPHA")
	if !ok || got != 1 {
		t.Fatalf("Get(ALPHA) = %d, %t", got, ok)
	}
}

func TestListsAreIndependent(t *testing.T) {
	left := New[int, int]()
	right := New[int, int]()
	left.Set(1, 1)
	if right.Has(1) || right.Len() != 0 {
		t.Fatal("a write to one list was visible in the other")
	}
}

func TestHeightDistribution(t *testing.T) {
	t.Run("always tall", func(t *testing.T) {
		const maxLevel = 5
		list := New[int, int](WithMaxLevel(maxLevel), WithRandSource(alwaysTall()))
		for i := 0; i < 3; i++ {
			list.Set(i, i)
		}
		if list.Level() != maxLevel {
			t.Fatalf("Level = %d, want %d", list.Level(), maxLevel)
		}
		for node := list.header.forward[0]; node != nil; node = node.forward[0] {
			if len(node.forward) != maxLevel {
				t.Fatalf("height = %d, want %d", len(node.forward), maxLevel)
			}
		}
		for i := 0; i < 3; i++ {
			list.Delete(i)
		}
		if list.Level() != 0 || list.Len() != 0 {
			t.Fatalf("len=%d level=%d", list.Len(), list.Level())
		}
		checkInvariants(t, list)
	})

	t.Run("always short", func(t *testing.T) {
		list := New[int, int](WithMaxLevel(6), WithRandSource(alwaysShort()))
		for i := 0; i < 8; i++ {
			list.Set(i, i)
		}
		if list.Level() != 1 {
			t.Fatalf("Level = %d, want 1", list.Level())
		}
		for node := list.header.forward[0]; node != nil; node = node.forward[0] {
			if len(node.forward) != 1 {
				t.Fatalf("height = %d, want 1", len(node.forward))
			}
		}
		checkInvariants(t, list)
	})

	t.Run("geometric", func(t *testing.T) {
		const n = 5000
		list := New[int, int](WithMaxLevel(12), WithRandSource(rand.New(rand.NewPCG(1, 2))))
		for i := 0; i < n; i++ {
			if !list.Insert(i, i) {
				t.Fatalf("duplicate %d", i)
			}
		}
		bottomOnly := 0
		for node := list.header.forward[0]; node != nil; node = node.forward[0] {
			if len(node.forward) == 1 {
				bottomOnly++
			}
		}
		// P(height == 1) = 1/2. Ten percent either way is many standard deviations.
		if bottomOnly < n*40/100 || bottomOnly > n*60/100 {
			t.Fatalf("bottom-only nodes = %d out of %d", bottomOnly, n)
		}
		if list.Level() < 4 {
			t.Fatalf("Level = %d, want a mixed tower", list.Level())
		}
		checkInvariants(t, list)
	})
}

func TestRandomOperationsMatchMap(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 99))
	list := New[int, int](WithMaxLevel(8), WithRandSource(rand.New(rand.NewPCG(7, 8))))
	ref := map[int]int{}

	for step := 0; step < 5000; step++ {
		if step == 2500 {
			list.Clear()
			clear(ref)
		}
		key := rng.IntN(200) - 50
		switch rng.IntN(6) {
		case 0, 1:
			list.Set(key, step)
			ref[key] = step
		case 2:
			_, exists := ref[key]
			added := list.Insert(key, step)
			if added == exists {
				t.Fatalf("step %d: Insert(%d) added=%t mapHas=%t", step, key, added, exists)
			}
			if added {
				ref[key] = step
			}
		case 3:
			got, ok := list.Get(key)
			want, wok := ref[key]
			if ok != wok || (ok && got != want) {
				t.Fatalf("step %d: Get(%d) = %d, %t; map %d, %t", step, key, got, ok, want, wok)
			}
		case 4:
			got, deleted := list.Delete(key)
			want, wok := ref[key]
			if deleted != wok || (deleted && got != want) {
				t.Fatalf("step %d: Delete(%d) = %d, %t; map %d, %t", step, key, got, deleted, want, wok)
			}
			delete(ref, key)
		default:
			if list.Has(key) != (func() bool { _, ok := ref[key]; return ok })() {
				t.Fatalf("step %d: Has(%d) disagrees with the map", step, key)
			}
		}
		if list.Len() != len(ref) {
			t.Fatalf("step %d: Len %d, map %d", step, list.Len(), len(ref))
		}

		probe := rng.IntN(200) - 50
		assertFloor(t, step, list, ref, probe)
		assertCeiling(t, step, list, ref, probe)
		if step%500 == 0 {
			assertSnapshot(t, step, list, ref)
			assertRange(t, step, list, ref, probe, probe+15)
			checkInvariants(t, list)
		}
	}
	checkInvariants(t, list)
}

func TestConcurrentDisjointKeys(t *testing.T) {
	const workers = 8
	const ops = 400
	list := New[int, int]()
	var wg sync.WaitGroup
	wg.Add(workers + 1)

	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := worker*ops + i
				list.Set(key, key)
				if got, ok := list.Get(key); !ok || got != key {
					t.Errorf("Get(%d) = %d, %t", key, got, ok)
				}
				if i%7 == 0 {
					if _, deleted := list.Delete(key); !deleted {
						t.Errorf("Delete(%d) reported a miss", key)
					}
				} else if list.Insert(key, key+1) {
					t.Errorf("Insert replaced %d", key)
				} else if got, ok := list.Get(key); !ok || got != key {
					t.Errorf("Get after Insert(%d) = %d, %t", key, got, ok)
				}
				_, _, _ = list.Min()
				_, _, _ = list.Max()
				_, _, _ = list.Floor(key)
				_, _, _ = list.Ceiling(key)
				_ = list.Len()
			}
		}(worker)
	}

	go func() {
		defer wg.Done()
		for n := 0; n < 50; n++ {
			var previous int
			var have bool
			for key, value := range list.All() {
				if have && key <= previous {
					t.Errorf("iteration produced %d then %d", previous, key)
				}
				if key != value {
					t.Errorf("key %d stored %d", key, value)
				}
				previous = key
				have = true
			}
			_ = list.Snapshot()
		}
	}()

	wg.Wait()
	for worker := 0; worker < workers; worker++ {
		for i := 0; i < ops; i++ {
			key := worker*ops + i
			_, ok := list.Get(key)
			if ok != (i%7 != 0) {
				t.Fatalf("final presence of %d = %t", key, ok)
			}
		}
	}
	checkInvariants(t, list)
}

func TestIteratorReleasesLock(t *testing.T) {
	list := New[int, string](WithMaxLevel(1))
	for i := 1; i <= 10; i++ {
		list.Set(i, strconv.Itoa(i))
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for key := range list.Keys() {
			if key == 5 {
				break
			}
		}
		list.Set(11, "11")
		for value := range list.Values() {
			_ = value
			break
		}
		for key := range list.All() {
			_ = key
			break
		}
		for key := range list.Range(1, 4) {
			_ = key
			break
		}
		for key := range list.RangeFrom(8) {
			_ = key
			break
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("an iterator leaked its read lock and the following write blocked")
	}
	if _, ok := list.Get(11); !ok {
		t.Fatal("the write that followed iteration did not land")
	}
}

func TestStringRendersLanes(t *testing.T) {
	list := New[int, string](WithMaxLevel(3), WithProbability(0.5), WithRandSource(alwaysShort()))
	list.Set(1, "a")
	list.Set(2, "b")
	const want = "SkipList(len=2, level=1, maxLevel=3, probability=0.50)\nL0 | 1:a -> 2:b -> nil\n"
	if got := list.String(); got != want {
		t.Fatalf("String =\n%s\nwant\n%s", got, want)
	}
}

func TestNilAndZeroPanic(t *testing.T) {
	var missing *SkipList[int, int]
	var zero SkipList[int, int]
	calls := []func(){
		func() { missing.Len() },
		func() { zero.Len() },
		func() { zero.Insert(1, 1) },
		func() { zero.Set(1, 1) },
		func() { zero.Get(1) },
		func() { zero.Delete(1) },
		func() { zero.All() },
		func() { zero.String() },
		func() { zero.MaxLevel() },
		func() { zero.Probability() },
		func() { _ = zero.Dump(nil, NewFormat(IntCodec(), IntCodec())) },
	}
	for _, call := range calls {
		message, panicked := panicText(call)
		if !panicked || !strings.Contains(message, "New") {
			t.Fatalf("panic %q, panicked=%t", message, panicked)
		}
	}

	mustPanic(t, func() { NewWithComparator[int, int](nil) })
	mustPanic(t, func() { New[int, int](nil) })
	mustPanic(t, func() { New[int, int](WithMaxLevel(0)) })
	mustPanic(t, func() { New[int, int](WithMaxLevel(-1)) })
	mustPanic(t, func() { New[int, int](WithMaxLevel(MaxLevelLimit + 1)) })
	mustPanic(t, func() { New[int, int](WithProbability(0)) })
	mustPanic(t, func() { New[int, int](WithProbability(1)) })
	mustPanic(t, func() { New[int, int](WithProbability(math.NaN())) })
	mustPanic(t, func() { New[int, int](WithProbability(math.Inf(1))) })
	mustPanic(t, func() { New[int, int](WithRandSource(nil)) })
}

func TestOptionsLastValueWins(t *testing.T) {
	list := New[int, int](
		WithMaxLevel(3),
		WithMaxLevel(5),
		WithProbability(0.25),
		WithProbability(0.3),
	)
	if list.MaxLevel() != 5 || list.Probability() != 0.3 {
		t.Fatalf("max=%d probability=%v", list.MaxLevel(), list.Probability())
	}

	capped := New[int, int](WithMaxLevel(MaxLevelLimit))
	capped.Set(1, 1)
	if capped.MaxLevel() != MaxLevelLimit {
		t.Fatalf("MaxLevel = %d", capped.MaxLevel())
	}
	checkInvariants(t, capped)
}

func TestGetDoesNotAllocate(t *testing.T) {
	list := New[int, int](WithMaxLevel(8), WithRandSource(alwaysShort()))
	for i := 0; i < 1000; i++ {
		list.Insert(i, i)
	}
	allocs := testing.AllocsPerRun(100, func() {
		list.Get(500)
	})
	if allocs != 0 {
		t.Fatalf("Get allocated %v times per call", allocs)
	}
}

func TestSetExistingDoesNotAllocate(t *testing.T) {
	list := New[int, int](WithMaxLevel(8), WithRandSource(alwaysShort()))
	list.Set(1, 1)
	allocs := testing.AllocsPerRun(100, func() {
		list.Set(1, 2)
	})
	if allocs != 0 {
		t.Fatalf("Set of an existing key allocated %v times per call", allocs)
	}
}

func FuzzOperations(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 2048 {
			data = data[:2048]
		}
		list := New[int, int](WithMaxLevel(6), WithRandSource(rand.New(rand.NewPCG(1, 1))))
		ref := map[int]int{}
		for i := 0; i+1 < len(data); i += 2 {
			key := int(int8(data[i]))
			value := int(data[i+1])
			switch data[i+1] % 4 {
			case 0:
				list.Set(key, value)
				ref[key] = value
			case 1:
				_, exists := ref[key]
				added := list.Insert(key, value)
				if added == exists {
					t.Fatalf("Insert(%d) added=%t mapHas=%t", key, added, exists)
				}
				if added {
					ref[key] = value
				}
			case 2:
				_, _ = list.Delete(key)
				delete(ref, key)
			default:
				got, ok := list.Get(key)
				want, wok := ref[key]
				if ok != wok || (ok && got != want) {
					t.Fatalf("Get(%d) = %d, %t; map %d, %t", key, got, ok, want, wok)
				}
			}
		}
		if list.Len() != len(ref) {
			t.Fatalf("Len %d, map %d", list.Len(), len(ref))
		}
		checkInvariants(t, list)
	})
}

func keysOf[K, V any](list *SkipList[K, V]) []K {
	var keys []K
	for key := range list.Keys() {
		keys = append(keys, key)
	}
	return keys
}

func keysOfRange[K, V any](list *SkipList[K, V], from, to K) []K {
	var keys []K
	for key := range list.Range(from, to) {
		keys = append(keys, key)
	}
	return keys
}

func assertFloor(t *testing.T, step int, list *SkipList[int, int], ref map[int]int, probe int) {
	t.Helper()
	gotKey, gotValue, gotOK := list.Floor(probe)
	wantKey, wantValue, wantOK := 0, 0, false
	for key, value := range ref {
		if key <= probe && (!wantOK || key > wantKey) {
			wantKey, wantValue, wantOK = key, value, true
		}
	}
	if gotOK != wantOK || (gotOK && (gotKey != wantKey || gotValue != wantValue)) {
		t.Fatalf("step %d: Floor(%d) = (%d, %d, %t), want (%d, %d, %t)",
			step, probe, gotKey, gotValue, gotOK, wantKey, wantValue, wantOK)
	}
}

func assertCeiling(t *testing.T, step int, list *SkipList[int, int], ref map[int]int, probe int) {
	t.Helper()
	gotKey, gotValue, gotOK := list.Ceiling(probe)
	wantKey, wantValue, wantOK := 0, 0, false
	for key, value := range ref {
		if key >= probe && (!wantOK || key < wantKey) {
			wantKey, wantValue, wantOK = key, value, true
		}
	}
	if gotOK != wantOK || (gotOK && (gotKey != wantKey || gotValue != wantValue)) {
		t.Fatalf("step %d: Ceiling(%d) = (%d, %d, %t), want (%d, %d, %t)",
			step, probe, gotKey, gotValue, gotOK, wantKey, wantValue, wantOK)
	}
}

func assertSnapshot(t *testing.T, step int, list *SkipList[int, int], ref map[int]int) {
	t.Helper()
	snapshot := list.Snapshot()
	if len(snapshot) != len(ref) {
		t.Fatalf("step %d: snapshot %d, map %d", step, len(snapshot), len(ref))
	}
	var previous int
	var have bool
	for _, entry := range snapshot {
		if have && entry.Key <= previous {
			t.Fatalf("step %d: snapshot order %d then %d", step, previous, entry.Key)
		}
		if entry.Value != ref[entry.Key] {
			t.Fatalf("step %d: snapshot %d=%d, map %d", step, entry.Key, entry.Value, ref[entry.Key])
		}
		previous = entry.Key
		have = true
	}
}

func assertRange(t *testing.T, step int, list *SkipList[int, int], ref map[int]int, from, to int) {
	t.Helper()
	var got []int
	for key := range list.Range(from, to) {
		got = append(got, key)
	}
	var want []int
	for key := range ref {
		if key >= from && key < to {
			want = append(want, key)
		}
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("step %d: Range[%d, %d) = %v, want %v", step, from, to, got, want)
	}
}

func mustPanic(t *testing.T, fn func()) {
	t.Helper()
	if _, panicked := panicText(fn); !panicked {
		t.Fatal("expected a panic")
	}
}

func panicText(fn func()) (message string, panicked bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicked = true
			message = fmt.Sprint(recovered)
		}
	}()
	fn()
	return "", false
}

// checkInvariants walks every lane and checks the structural promises insert
// and delete must keep: strict order, contiguous heights, no cycles, and a
// length that matches the bottom lane.
func checkInvariants[K, V any](t *testing.T, s *SkipList[K, V]) {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.ready || s.header == nil {
		t.Fatal("list is not initialised")
	}
	if len(s.header.forward) != s.maxLevel {
		t.Fatalf("header lanes = %d, max level = %d", len(s.header.forward), s.maxLevel)
	}
	if s.level < 0 || s.level > s.maxLevel {
		t.Fatalf("level %d outside [0, %d]", s.level, s.maxLevel)
	}

	present := make([]map[*node[K, V]]struct{}, s.maxLevel)
	ordered := make([][]*node[K, V], s.maxLevel)
	for level := 0; level < s.maxLevel; level++ {
		present[level] = map[*node[K, V]]struct{}{}
		var previous *node[K, V]
		for current := s.header.forward[level]; current != nil; current = current.forward[level] {
			if _, dup := present[level][current]; dup {
				t.Fatalf("cycle on level %d", level)
			}
			present[level][current] = struct{}{}
			if len(current.forward) <= level {
				t.Fatalf("node %v on level %d has height %d", current.key, level, len(current.forward))
			}
			if previous != nil && s.compare(previous.key, current.key) >= 0 {
				t.Fatalf("level %d is not strictly ordered: %v then %v", level, previous.key, current.key)
			}
			previous = current
			ordered[level] = append(ordered[level], current)
		}
		if level >= s.level && len(ordered[level]) != 0 {
			t.Fatalf("level %d is above height %d but holds %d nodes", level, s.level, len(ordered[level]))
		}
		if level == s.level-1 && len(ordered[level]) == 0 {
			t.Fatalf("top level %d is empty", level)
		}
	}

	if len(ordered[0]) != s.length {
		t.Fatalf("length %d, bottom lane %d", s.length, len(ordered[0]))
	}
	tallest := 0
	for _, current := range ordered[0] {
		height := len(current.forward)
		if height < 1 || height > s.maxLevel {
			t.Fatalf("height %d outside [1, %d]", height, s.maxLevel)
		}
		if height > tallest {
			tallest = height
		}
		for level := 0; level < height; level++ {
			if _, ok := present[level][current]; !ok {
				t.Fatalf("node %v missing from level %d", current.key, level)
			}
		}
		for level := height; level < s.maxLevel; level++ {
			if _, ok := present[level][current]; ok {
				t.Fatalf("node %v present on level %d above its height %d", current.key, level, height)
			}
		}
	}
	if tallest != s.level {
		t.Fatalf("list level %d, tallest node %d", s.level, tallest)
	}
}
