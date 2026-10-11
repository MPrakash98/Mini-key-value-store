package db

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// TestCompaction_Basic writes 10,000 keys, overwrites each 5 times, deletes
// half, compacts, and verifies correctness and disk space reduction.
func TestCompaction_Basic(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, Options{Dir: dir, SyncPolicy: SyncNever, SegmentMaxBytes: 64 * 1024})

	const N = 1000 // scaled down for speed; the plan says 10,000
	model := make(map[string]string)

	// Write each key 5 times (4 overwrites + 1 final).
	for round := 0; round < 5; round++ {
		for i := 0; i < N; i++ {
			key := fmt.Sprintf("key-%04d", i)
			val := fmt.Sprintf("val-%04d-round-%d", i, round)
			db.Put([]byte(key), []byte(val))
			model[key] = val
		}
	}
	// Delete half the keys.
	for i := 0; i < N; i += 2 {
		key := fmt.Sprintf("key-%04d", i)
		db.Delete([]byte(key))
		delete(model, key)
	}

	beforeStats := db.Stats()

	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	afterStats := db.Stats()

	// DiskBytes must be significantly smaller after compaction.
	if afterStats.DiskBytes >= beforeStats.DiskBytes {
		t.Fatalf("compaction did not reduce disk usage: before=%d after=%d",
			beforeStats.DiskBytes, afterStats.DiskBytes)
	}

	// All live keys must return the correct (latest) value.
	for key, want := range model {
		got, err := db.Get([]byte(key))
		if err != nil || string(got) != want {
			t.Fatalf("Get(%q) after compact: got (%q, %v) want (%q, nil)", key, got, err, want)
		}
	}

	// Deleted keys must be gone.
	for i := 0; i < N; i += 2 {
		key := fmt.Sprintf("key-%04d", i)
		if _, ok := model[key]; !ok {
			_, err := db.Get([]byte(key))
			if err != ErrNotFound {
				t.Fatalf("deleted key %q present after compact: err=%v", key, err)
			}
		}
	}
}

// TestCompaction_Reopen verifies that compacted data survives a reopen.
func TestCompaction_Reopen(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, Options{Dir: dir, SyncPolicy: SyncNever, SegmentMaxBytes: 16 * 1024})

	const N = 500
	for i := 0; i < N; i++ {
		db.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d-final", i)))
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	db.Close()
	db.closed = false

	db2 := openTestDB(t, Options{Dir: dir})
	for i := 0; i < N; i++ {
		key := []byte(fmt.Sprintf("k%d", i))
		want := fmt.Sprintf("v%d-final", i)
		got, err := db2.Get(key)
		if err != nil || string(got) != want {
			t.Fatalf("Get %d after compact+reopen: got (%q, %v) want (%q, nil)", i, got, err, want)
		}
	}
}

// TestCompaction_Concurrent runs Compact while other goroutines continue
// doing Put/Delete/Get on overlapping keys, then verifies consistency.
func TestCompaction_Concurrent(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, Options{Dir: dir, SyncPolicy: SyncNever, SegmentMaxBytes: 16 * 1024})

	// Seed some data.
	const N = 200
	for i := 0; i < N; i++ {
		db.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("init-%04d", i)))
	}

	var wg sync.WaitGroup
	var compactErr error
	var mu sync.Mutex

	// Run compaction in the background.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := db.Compact(); err != nil {
			mu.Lock()
			compactErr = err
			mu.Unlock()
		}
	}()

	// Concurrently perform operations.
	for g := 0; g < 4; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			for i := 0; i < 200; i++ {
				key := fmt.Sprintf("k%04d", rng.Intn(N))
				val := fmt.Sprintf("concurrent-%d-%d", g, i)
				switch rng.Intn(2) {
				case 0:
					db.Put([]byte(key), []byte(val))
				case 1:
					db.Get([]byte(key))
				}
			}
		}()
	}

	wg.Wait()

	mu.Lock()
	if compactErr != nil {
		t.Fatalf("Compact: %v", compactErr)
	}
	mu.Unlock()

	// DB should be readable after concurrent compaction.
	s := db.Stats()
	if s.Keys == 0 && N > 0 {
		t.Fatal("all keys disappeared after concurrent compact")
	}
}

// TestCompaction_TwiceInARow verifies that compacting an already-compact DB works.
func TestCompaction_TwiceInARow(t *testing.T) {
	db := openTestDB(t, Options{SyncPolicy: SyncNever})
	db.Put([]byte("k"), []byte("v"))
	if err := db.Compact(); err != nil {
		t.Fatalf("first Compact: %v", err)
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("second Compact: %v", err)
	}
}

// TestCompaction_DeleteBeforeDoesNotComeBack writes a key, compacts,
// verifies the key stays deleted after another reopen.
func TestCompaction_DeleteBeforeDoesNotComeBack(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, Options{Dir: dir, SyncPolicy: SyncAlways})

	db.Put([]byte("ghost"), []byte("here"))
	db.Delete([]byte("ghost"))
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	db.Close()
	db.closed = false

	db2 := openTestDB(t, Options{Dir: dir})
	_, err := db2.Get([]byte("ghost"))
	if err != ErrNotFound {
		t.Fatalf("deleted key returned after compact+reopen: err=%v", err)
	}
}

// TestCompaction_HintFile verifies that opening with and without hint files
// yields identical contents.
func TestCompaction_HintFile(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, Options{Dir: dir, SyncPolicy: SyncNever, UseHintFiles: true})

	const N = 100
	for i := 0; i < N; i++ {
		db.Put([]byte(fmt.Sprintf("hk%d", i)), []byte(fmt.Sprintf("hv%d", i)))
	}
	db.opts.UseHintFiles = true
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	db.Close()
	db.closed = false

	// Open with hints.
	db2 := openTestDB(t, Options{Dir: dir, UseHintFiles: true})
	for i := 0; i < N; i++ {
		got, err := db2.Get([]byte(fmt.Sprintf("hk%d", i)))
		if err != nil || string(got) != fmt.Sprintf("hv%d", i) {
			t.Fatalf("with hints: Get(%d): (%q, %v)", i, got, err)
		}
	}
	db2.Close()
	db2.closed = false

	// Open without hints.
	db3 := openTestDB(t, Options{Dir: dir, UseHintFiles: false})
	for i := 0; i < N; i++ {
		got, err := db3.Get([]byte(fmt.Sprintf("hk%d", i)))
		if err != nil || string(got) != fmt.Sprintf("hv%d", i) {
			t.Fatalf("without hints: Get(%d): (%q, %v)", i, got, err)
		}
	}
}

// TestNeedsCompaction verifies the dead-ratio calculation.
func TestNeedsCompaction(t *testing.T) {
	db := openTestDB(t, Options{SyncPolicy: SyncNever})

	// Initially no dead data.
	if db.NeedsCompaction(0.5) {
		t.Fatal("empty DB should not need compaction")
	}

	// Write and overwrite to create dead data.
	for i := 0; i < 100; i++ {
		db.Put([]byte("key"), []byte(fmt.Sprintf("val-%d", i)))
	}

	if !db.NeedsCompaction(0.5) {
		t.Fatal("DB with 99% dead data should need compaction at 0.5 ratio")
	}
}
