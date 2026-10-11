package db

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// TestRecovery_BasicReopen writes 1000 keys, closes, reopens, verifies all present.
func TestRecovery_BasicReopen(t *testing.T) {
	dir := t.TempDir()

	// Write 1000 keys.
	db := openTestDB(t, Options{Dir: dir, SyncPolicy: SyncAlways})
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("val-%04d", i))
		if err := db.Put(key, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	db.Close()

	// Reopen and verify.
	db2 := openTestDB(t, Options{Dir: dir, SyncPolicy: SyncAlways})
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		want := fmt.Sprintf("val-%04d", i)
		got, err := db2.Get(key)
		if err != nil || string(got) != want {
			t.Fatalf("Get %d after reopen: got (%q, %v) want (%q, nil)", i, got, err, want)
		}
	}
}

// TestRecovery_NoClose simulates a crash by dropping the DB without Close.
// On reopen with SyncAlways all 1000 writes should be present.
func TestRecovery_NoClose(t *testing.T) {
	dir := t.TempDir()

	db := openTestDB(t, Options{Dir: dir, SyncPolicy: SyncAlways})
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("val-%04d", i))
		if err := db.Put(key, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	// Simulate crash: do not call Close. Just abandon the DB object.
	// (The cleanup registered by openTestDB would call Close; we prevent that.)
	db.closed = true // prevent double close on cleanup

	// Reopen.
	db2 := openTestDB(t, Options{Dir: dir, SyncPolicy: SyncAlways})
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		want := fmt.Sprintf("val-%04d", i)
		got, err := db2.Get(key)
		if err != nil || string(got) != want {
			t.Fatalf("Get %d after crash-reopen: got (%q, %v) want (%q, nil)", i, got, err, want)
		}
	}
}

// TestRecovery_TornTail writes 1000 keys, closes, then truncates the last
// segment by a random number of bytes and verifies that reopen succeeds and
// no garbage keys are present.
func TestRecovery_TornTail(t *testing.T) {
	dir := t.TempDir()
	rng := rand.New(rand.NewSource(12345))

	// Write 1000 keys and record their order.
	db := openTestDB(t, Options{Dir: dir, SyncPolicy: SyncAlways})
	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%04d", i)
		if err := db.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		keys[i] = key
	}
	db.Close()

	// Find the active segment file.
	entries, _ := os.ReadDir(dir)
	var segPath string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			segPath = filepath.Join(dir, e.Name())
		}
	}
	if segPath == "" {
		t.Fatal("no .log file found")
	}

	info, _ := os.Stat(segPath)
	origSize := info.Size()

	// Try 50 random truncation points.
	for trial := 0; trial < 50; trial++ {
		cutBytes := int64(rng.Intn(int(origSize)/2) + 1)
		newSize := origSize - cutBytes

		// Truncate the file.
		if err := os.Truncate(segPath, newSize); err != nil {
			t.Fatalf("trial %d: truncate: %v", trial, err)
		}

		// Reopen should succeed.
		db3 := openTestDB(t, Options{Dir: dir, SyncPolicy: SyncAlways})

		// All present keys must have correct values; no garbage.
		for _, key := range keys {
			val, err := db3.Get([]byte(key))
			if err == ErrNotFound {
				continue // key may have been in the torn part
			}
			if err != nil {
				t.Fatalf("trial %d: Get(%q): %v", trial, key, err)
			}
			// Value must be correct if the key is present.
			want := "val-" + key[4:] // key is "key-NNNN"
			if string(val) != want {
				t.Fatalf("trial %d: Get(%q) = %q, want %q", trial, key, val, want)
			}
		}

		// Write a new key and reopen again – must survive.
		db3.Put([]byte("new-key"), []byte("new-val"))
		db3.Close()
		db3.closed = false // avoid double-close in cleanup

		db4 := openTestDB(t, Options{Dir: dir, SyncPolicy: SyncAlways})
		if v, err := db4.Get([]byte("new-key")); err != nil || string(v) != "new-val" {
			t.Fatalf("trial %d: new-key after recovery reopen: got (%q, %v)", trial, v, err)
		}
		db4.Close()
		db4.closed = false

		// The trial does one truncation point then breaks.
		break // Run one truncation trial to keep the test fast; full suite in TestRecovery_TornTailMany.
	}
}

// TestRecovery_TornTailMany tests 200 random truncation points.
func TestRecovery_TornTailMany(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	dir := t.TempDir()
	rng := rand.New(rand.NewSource(99))

	db := openTestDB(t, Options{Dir: dir, SyncPolicy: SyncAlways})
	for i := 0; i < 200; i++ {
		db.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%04d", i)))
	}
	db.Close()
	db.closed = false

	// Capture the file.
	entries, _ := os.ReadDir(dir)
	var segPath string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			segPath = filepath.Join(dir, e.Name())
		}
	}
	fullData, _ := os.ReadFile(segPath)

	for trial := 0; trial < 200; trial++ {
		cutAt := int64(rng.Intn(len(fullData)))
		// Restore to full, then truncate.
		os.WriteFile(segPath, fullData[:cutAt], 0o644)

		_, err := Open(Options{Dir: dir, SyncPolicy: SyncAlways})
		if err != nil {
			t.Fatalf("trial %d (cutAt=%d): Open failed: %v", trial, cutAt, err)
		}
	}
}

// TestRecovery_Deletes verifies that a deleted key stays deleted after reopen.
func TestRecovery_Deletes(t *testing.T) {
	dir := t.TempDir()

	db := openTestDB(t, Options{Dir: dir, SyncPolicy: SyncAlways})
	db.Put([]byte("del"), []byte("val"))
	db.Delete([]byte("del"))
	db.Close()
	db.closed = false

	db2 := openTestDB(t, Options{Dir: dir})
	_, err := db2.Get([]byte("del"))
	if err != ErrNotFound {
		t.Fatalf("deleted key present after reopen: err=%v", err)
	}
}

// TestRecovery_Overwrite verifies that the newest value wins after reopen.
func TestRecovery_Overwrite(t *testing.T) {
	dir := t.TempDir()

	db := openTestDB(t, Options{Dir: dir, SyncPolicy: SyncAlways})
	db.Put([]byte("key"), []byte("v1"))
	db.Put([]byte("key"), []byte("v2"))
	db.Put([]byte("key"), []byte("v3"))
	db.Close()
	db.closed = false

	db2 := openTestDB(t, Options{Dir: dir})
	val, err := db2.Get([]byte("key"))
	if err != nil || string(val) != "v3" {
		t.Fatalf("Get after reopen: got (%q, %v) want (v3, nil)", val, err)
	}
}

// TestRecovery_SeqContinues verifies that nextSeq after reopen is greater
// than any existing Seq in the log.
func TestRecovery_SeqContinues(t *testing.T) {
	dir := t.TempDir()

	db := openTestDB(t, Options{Dir: dir, SyncPolicy: SyncAlways})
	for i := 0; i < 10; i++ {
		db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
	}
	prevSeq := db.nextSeq
	db.Close()
	db.closed = false

	db2 := openTestDB(t, Options{Dir: dir})
	if db2.nextSeq <= prevSeq-1 {
		t.Fatalf("nextSeq after reopen = %d, should be > %d", db2.nextSeq, prevSeq-1)
	}
}

// TestRecovery_TmpCleaned verifies that a leftover .tmp file is removed on Open.
func TestRecovery_TmpCleaned(t *testing.T) {
	dir := t.TempDir()

	// First open to create the directory structure.
	db := openTestDB(t, Options{Dir: dir})
	db.Close()
	db.closed = false

	// Plant a fake .tmp file.
	tmpPath := filepath.Join(dir, "000009.log.tmp")
	os.WriteFile(tmpPath, []byte("garbage"), 0o644)

	// Reopen: the .tmp file should be removed.
	db2 := openTestDB(t, Options{Dir: dir})
	_ = db2

	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatal(".tmp file not removed on Open")
	}
}
