package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestRotation_ManyFiles writes 2000 records with a tiny segment max (4 KiB)
// and verifies that many .log files are created and all keys are readable.
func TestRotation_ManyFiles(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, Options{
		Dir:             dir,
		SyncPolicy:      SyncNever,
		SegmentMaxBytes: 4 * 1024, // 4 KiB
	})

	const N = 2000
	for i := 0; i < N; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		val := []byte(fmt.Sprintf("val-%05d", i))
		if err := db.Put(key, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Should have created multiple segments.
	s := db.Stats()
	if s.Segments < 2 {
		t.Fatalf("expected multiple segments, got %d", s.Segments)
	}

	// All keys readable.
	for i := 0; i < N; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		want := fmt.Sprintf("val-%05d", i)
		got, err := db.Get(key)
		if err != nil || string(got) != want {
			t.Fatalf("Get %d: got (%q, %v) want (%q, nil)", i, got, err, want)
		}
	}

	// Reopen and verify again.
	db.Close()
	db.closed = false

	db2 := openTestDB(t, Options{Dir: dir, SegmentMaxBytes: 4 * 1024})
	for i := 0; i < N; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		want := fmt.Sprintf("val-%05d", i)
		got, err := db2.Get(key)
		if err != nil || string(got) != want {
			t.Fatalf("Get %d after reopen: got (%q, %v) want (%q, nil)", i, got, err, want)
		}
	}
}

// TestRotation_OverwriteAcrossSegments verifies that an overwritten key whose
// old copy is in an earlier segment returns the newest value after reopen.
func TestRotation_OverwriteAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, Options{
		Dir:             dir,
		SyncPolicy:      SyncAlways,
		SegmentMaxBytes: 512, // very small to force rotation quickly
	})

	// Write the key, force rotation by writing filler, then overwrite.
	db.Put([]byte("target"), []byte("old"))
	for i := 0; i < 20; i++ {
		db.Put([]byte(fmt.Sprintf("filler-%d", i)), []byte("filler-value"))
	}
	db.Put([]byte("target"), []byte("new"))
	db.Close()
	db.closed = false

	db2 := openTestDB(t, Options{Dir: dir, SegmentMaxBytes: 512})
	got, err := db2.Get([]byte("target"))
	if err != nil || string(got) != "new" {
		t.Fatalf("Get after rotation+reopen: got (%q, %v) want (new, nil)", got, err)
	}
}

// TestRotation_OversizedRecord verifies that a single record larger than
// SegmentMaxBytes is stored and retrieved correctly.
func TestRotation_OversizedRecord(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, Options{
		Dir:             dir,
		SyncPolicy:      SyncNever,
		SegmentMaxBytes: 100, // tiny limit
	})

	// A value that is definitely larger than 100 bytes.
	bigVal := make([]byte, 10000)
	for i := range bigVal {
		bigVal[i] = byte(i % 251)
	}

	if err := db.Put([]byte("bigkey"), bigVal); err != nil {
		t.Fatalf("Put oversized: %v", err)
	}
	got, err := db.Get([]byte("bigkey"))
	if err != nil {
		t.Fatalf("Get oversized: %v", err)
	}
	if len(got) != len(bigVal) {
		t.Fatalf("value length: got %d want %d", len(got), len(bigVal))
	}
	for i, b := range got {
		if b != bigVal[i] {
			t.Fatalf("value mismatch at byte %d", i)
			break
		}
	}
}

// TestRotation_StatsSegmentCount verifies Stats().Segments after rotation.
func TestRotation_StatsSegmentCount(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, Options{
		Dir:             dir,
		SyncPolicy:      SyncNever,
		SegmentMaxBytes: 256,
	})

	for i := 0; i < 100; i++ {
		db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("vvvvvvvv"))
	}

	// Count actual .log files.
	entries, _ := os.ReadDir(dir)
	var logCount int
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			logCount++
		}
	}

	s := db.Stats()
	if s.Segments != logCount {
		t.Fatalf("Stats.Segments=%d, actual .log files=%d", s.Segments, logCount)
	}
}
