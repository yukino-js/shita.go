package skiplist

import "testing"

func BenchmarkGet(b *testing.B) {
	const n = 100_000
	list := populatedList(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.Get(i % n)
	}
}

func BenchmarkGetParallel(b *testing.B) {
	const n = 100_000
	list := populatedList(n)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			list.Get(i % n)
			i++
		}
	})
}

func BenchmarkInsert(b *testing.B) {
	list := New[int, int]()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		list.Insert(i, i)
	}
}

func BenchmarkSetExisting(b *testing.B) {
	list := New[int, int]()
	list.Set(1, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.Set(1, i)
	}
}

func populatedList(n int) *SkipList[int, int] {
	list := New[int, int]()
	for i := 0; i < n; i++ {
		list.Insert(i, i)
	}
	return list
}
