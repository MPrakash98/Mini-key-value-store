package vfs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOSFS(t *testing.T) {
	dir := t.TempDir()
	fs := OS()

	// MkdirAll: create a nested directory.
	subDir := filepath.Join(dir, "sub", "dir")
	if err := fs.MkdirAll(subDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	// OpenFile: create and write.
	name := filepath.Join(subDir, "test.dat")
	f, err := fs.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile create: %v", err)
	}
	if _, err := f.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Stat: verify file size.
	fi, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() != 5 {
		t.Fatalf("Stat size: got %d want 5", fi.Size())
	}

	// ReadAt: read back the written bytes.
	buf := make([]byte, 5)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(buf) != "hello" {
		t.Fatalf("ReadAt: got %q want %q", buf, "hello")
	}

	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Rename: move the file.
	newName := filepath.Join(subDir, "renamed.dat")
	if err := fs.Rename(name, newName); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// SyncDir: should succeed (may be no-op on Windows).
	if err := fs.SyncDir(subDir); err != nil {
		t.Fatalf("SyncDir: %v", err)
	}

	// Remove: delete the renamed file.
	if err := fs.Remove(newName); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := os.Stat(newName); !os.IsNotExist(err) {
		t.Fatalf("file still exists after Remove")
	}

	// ReadDir: should list nothing (we removed the only file).
	entries, err := fs.ReadDir(subDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("ReadDir: expected 0 entries, got %d", len(entries))
	}
}
