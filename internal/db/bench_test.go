package db

import (
	"fmt"
	"testing"
)

// BenchmarkPut_SyncAlways measures Put throughput with fsync after every write.
func BenchmarkPut_SyncAlways(b *testing.B) {
	benchmarkPut(b, SyncAlways)
}

// BenchmarkPut_SyncInterval measures Put throughput with a 100ms background sync.
func BenchmarkPut_SyncInterval(b *testing.B) {
	benchmarkPut(b, SyncInterval)
}

// BenchmarkPut_SyncNever measures Put throughput with OS-driven flushing.
func BenchmarkPut_SyncNever(b *testing.B) {
	benchmarkPut(b, SyncNever)
}

func benchmarkPut(b *testing.B, policy SyncPolicy) {
	b.Helper()
	for _, sz := range []int{100, 1024, 10 * 1024} {
		sz := sz
		b.Run(fmt.Sprintf("value-%dB", sz), func(b *testing.B) {
			dir := b.TempDir()
			store, err := Open(Options{
				Dir:        dir,
				SyncPolicy: policy,
			})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer store.Close()

			val := make([]byte, sz)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("k%08d", i))
				if err := store.Put(key, val); err != nil {
					b.Fatalf("Put: %v", err)
				}
			}
			b.SetBytes(int64(sz))
		})
	}
}

// BenchmarkGet_Random measures random read throughput from a pre-populated DB.
func BenchmarkGet_Random(b *testing.B) {
	for _, sz := range []int{100, 1024, 10 * 1024} {
		sz := sz
		b.Run(fmt.Sprintf("value-%dB", sz), func(b *testing.B) {
			dir := b.TempDir()
			store, err := Open(Options{
				Dir:        dir,
				SyncPolicy: SyncNever,
			})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer store.Close()

			const N = 10000
			val := make([]byte, sz)
			for i := 0; i < N; i++ {
				store.Put([]byte(fmt.Sprintf("k%08d", i)), val)
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("k%08d", i%N))
				if _, err := store.Get(key); err != nil {
					b.Fatalf("Get: %v", err)
				}
			}
			b.SetBytes(int64(sz))
		})
	}
}

// BenchmarkOpen_Recovery_1e5 measures recovery time for ~100k keys.
func BenchmarkOpen_Recovery_1e5(b *testing.B) {
	benchmarkRecovery(b, 100_000)
}

// BenchmarkOpen_Recovery_1e6 measures recovery time for ~1M keys.
func BenchmarkOpen_Recovery_1e6(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping in short mode")
	}
	benchmarkRecovery(b, 1_000_000)
}

func benchmarkRecovery(b *testing.B, n int) {
	b.Helper()
	dir := b.TempDir()

	// Pre-populate.
	store, _ := Open(Options{Dir: dir, SyncPolicy: SyncNever})
	val := make([]byte, 100)
	for i := 0; i < n; i++ {
		store.Put([]byte(fmt.Sprintf("k%08d", i)), val)
	}
	store.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s, err := Open(Options{Dir: dir, SyncPolicy: SyncNever})
		if err != nil {
			b.Fatalf("Open: %v", err)
		}
		s.Close()
	}
}
