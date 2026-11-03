package yukino_log

import "testing"

func BenchmarkInfof(b *testing.B) {
	logger := New(Options{
		Output: func(p []byte) (int, error) { return len(p), nil },
	})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		logger.Infof("hello %d %s", i, "world")
	}
}

func BenchmarkInfo(b *testing.B) {
	logger := New(Options{
		Output: func(p []byte) (int, error) { return len(p), nil },
	})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		logger.Info("hello ", i, " world")
	}
}
