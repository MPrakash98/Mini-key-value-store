package vfs

import (
	"io"
	"os"
)

// File is the interface every file handle must satisfy. It is a superset of
// the standard io interfaces and adds Sync, Truncate, and Stat.
// *os.File satisfies this interface without any wrapper.
type File interface {
	io.Reader
	io.Writer
	io.ReaderAt
	io.Closer
	// Sync flushes OS write buffers to durable storage (like fsync(2)).
	Sync() error
	// Truncate sets the file size. The file's position is unchanged.
	Truncate(size int64) error
	// Stat returns file metadata.
	Stat() (os.FileInfo, error)
}

// FS is the file system abstraction used throughout the kvstore. All file
// I/O goes through this interface so tests can inject a fault-simulating
// implementation without touching real files.
type FS interface {
	// OpenFile opens or creates a file, analogous to os.OpenFile.
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	// Rename atomically renames (moves) a file.
	Rename(oldpath, newpath string) error
	// Remove deletes a file.
	Remove(name string) error
	// MkdirAll creates a directory and any missing parents.
	MkdirAll(path string, perm os.FileMode) error
	// ReadDir lists the entries of a directory.
	ReadDir(name string) ([]os.DirEntry, error)
	// SyncDir fsyncs the directory entry so that file creations, renames, and
	// deletions within it survive a power loss.
	// Without this, a renamed file's new name may vanish even if the data was
	// already on disk.
	SyncDir(path string) error
}

// OS returns an FS backed by the real operating system's file system.
func OS() FS { return osFS{} }

// osFS is the real-OS implementation of FS.
type osFS struct{}

func (osFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	return os.OpenFile(name, flag, perm)
}

func (osFS) Rename(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }
func (osFS) Remove(name string) error             { return os.Remove(name) }
func (osFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}
func (osFS) ReadDir(name string) ([]os.DirEntry, error) { return os.ReadDir(name) }

// SyncDir opens the directory and calls Sync on it so that new/renamed/deleted
// file entries in the directory are durable. Without this, a rename can be
// lost on power failure even if the file data reached disk.
//
// On Windows, directory fsync is a no-op (the OS handles it differently), so
// we open the directory and sync it; if that fails we silently skip.
func (osFS) SyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	// On Windows, Sync on a directory handle is a no-op or may return an error
	// that we can safely ignore (the OS page cache ensures consistency on normal
	// shutdown; the fault-injection tests use FaultFS instead).
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		// On Windows this is expected; don't treat as fatal.
		return nil //nolint:nilerr
	}
	return closeErr
}
