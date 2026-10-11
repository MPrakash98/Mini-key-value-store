package db

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// openTestDB is a convenience that opens a DB in a temp directory.
func openTestDB(t *testing.T, opts Options) *DB {
	t.Helper()
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if !db.closed {
			db.Close()
		}
	})
	return db
}

// TestPutGetDelete covers basic single-key semantics.
func TestPutGetDelete(t *testing.T) {
	db := openTestDB(t, Options{SyncPolicy: SyncAlways})

	// Put and Get.
	if err := db.Put([]byte("name"), []byte("Asha")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil || string(val) != "Asha" {
		t.Fatalf("Get: got %q, err %v", val, err)
	}

	// Overwrite.
	if err := db.Put([]byte("name"), []byte("Priya")); err != nil {
		t.Fatalf("Put overwrite: %v", err)
	}
	val, err = db.Get([]byte("name"))
	if err != nil || string(val) != "Priya" {
		t.Fatalf("Get after overwrite: got %q, err %v", val, err)
	}

	// Delete.
	if err := db.Delete([]byte("name")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err = db.Get([]byte("name"))
	if err != ErrNotFound {
		t.Fatalf("Get after Delete: expected ErrNotFound, got %v", err)
	}
}

// TestEmptyKeyRejected ensures an empty key returns ErrKeyEmpty.
func TestEmptyKeyRejected(t *testing.T) {
	db := openTestDB(t, Options{})
	if err := db.Put([]byte{}, []byte("v")); err != ErrKeyEmpty {
		t.Fatalf("expected ErrKeyEmpty, got %v", err)
	}
}

// TestEmptyValueAllowed ensures a PUT with an empty value is different from a DELETE.
func TestEmptyValueAllowed(t *testing.T) {
	db := openTestDB(t, Options{})
	if err := db.Put([]byte("emptyval"), []byte{}); err != nil {
		t.Fatalf("Put empty value: %v", err)
	}
	val, err := db.Get([]byte("emptyval"))
	if err != nil {
		t.Fatalf("Get empty value: %v", err)
	}
	if len(val) != 0 {
		t.Fatalf("expected empty slice, got %q", val)
	}
	// Now delete and verify it's gone.
	db.Delete([]byte("emptyval"))
	_, err = db.Get([]byte("emptyval"))
	if err != ErrNotFound {
		t.Fatalf("after Delete: expected ErrNotFound, got %v", err)
	}
}

// TestModelBased runs 10,000 random operations against a plain map[string]string model.
func TestModelBased(t *testing.T) {
	db := openTestDB(t, Options{SyncPolicy: SyncNever})
	model := make(map[string]string)
	rng := rand.New(rand.NewSource(42))

	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}

	for i := 0; i < 10000; i++ {
		key := keys[rng.Intn(len(keys))]
		op := rng.Intn(3) // 0=put, 1=get, 2=delete

		switch op {
		case 0:
			val := fmt.Sprintf("val-%d", rng.Int())
			if err := db.Put([]byte(key), []byte(val)); err != nil {
				t.Fatalf("Put: %v", err)
			}
			model[key] = val
		case 1:
			got, err := db.Get([]byte(key))
			want, exists := model[key]
			if exists {
				if err != nil || string(got) != want {
					t.Fatalf("Get(%q): got (%q, %v), want (%q, nil)", key, got, err, want)
				}
			} else {
				if err != ErrNotFound {
					t.Fatalf("Get(%q): expected ErrNotFound, got (%q, %v)", key, got, err)
				}
			}
		case 2:
			if err := db.Delete([]byte(key)); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			delete(model, key)
		}
	}

	// Final consistency check.
	for key, want := range model {
		got, err := db.Get([]byte(key))
		if err != nil || string(got) != want {
			t.Fatalf("final check Get(%q): got (%q, %v), want (%q, nil)", key, got, err, want)
		}
	}
}

// TestConcurrency runs 8 goroutines doing Put/Get on non-overlapping keys.
func TestConcurrency(t *testing.T) {
	db := openTestDB(t, Options{SyncPolicy: SyncNever})
	const goroutines = 8
	const opsPerGoroutine = 500

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			prefix := fmt.Sprintf("g%d-", g)
			for i := 0; i < opsPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("%skey-%d", prefix, i))
				val := []byte(fmt.Sprintf("%sval-%d", prefix, i))
				if err := db.Put(key, val); err != nil {
					t.Errorf("goroutine %d Put: %v", g, err)
					return
				}
				got, err := db.Get(key)
				if err != nil {
					t.Errorf("goroutine %d Get: %v", g, err)
					return
				}
				if string(got) != string(val) {
					t.Errorf("goroutine %d Get: got %q want %q", g, got, val)
				}
			}
		}()
	}
	wg.Wait()
}

// TestCloseError verifies that Put after Close returns ErrClosed.
func TestCloseError(t *testing.T) {
	db := openTestDB(t, Options{})
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db.closed = false // hack to prevent the cleanup from erroring
	if err := db.Put([]byte("k"), []byte("v")); err != ErrClosed {
		t.Fatalf("Put after Close: expected ErrClosed, got %v", err)
	}
	db.closed = true
}

// TestSyncInterval verifies the background sync goroutine runs without error.
func TestSyncInterval(t *testing.T) {
	db := openTestDB(t, Options{
		SyncPolicy: SyncInterval,
		SyncEvery:  10 * time.Millisecond,
	})
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("k%d", i))
		if err := db.Put(key, []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	// Let the background sync fire at least once.
	time.Sleep(50 * time.Millisecond)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db.closed = false // prevent cleanup double-close
}

// TestStats verifies that Stats returns sensible values.
func TestStats(t *testing.T) {
	db := openTestDB(t, Options{SyncPolicy: SyncNever})
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("2"))
	db.Delete([]byte("a"))

	s := db.Stats()
	if s.Keys != 1 {
		t.Fatalf("Stats.Keys: got %d want 1", s.Keys)
	}
	if s.Segments < 1 {
		t.Fatalf("Stats.Segments: got %d want >=1", s.Segments)
	}
	if s.DiskBytes == 0 {
		t.Fatalf("Stats.DiskBytes: got 0, expected non-zero")
	}
}
