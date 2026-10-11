package segment

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"

	"kvstore/internal/record"
	"kvstore/internal/vfs"
)

// FatalError signals an unrecoverable I/O condition (e.g. a partial write
// followed by a failed truncation, or a failed fsync). The DB must enter
// fail-stop mode on receiving this: refuse all further writes.
type FatalError struct {
	Err error
}

func (e *FatalError) Error() string { return "fatal segment error: " + e.Err.Error() }
func (e *FatalError) Unwrap() error { return e.Err }

// IsFatal reports whether err is (or wraps) a FatalError.
func IsFatal(err error) bool {
	var fe *FatalError
	return errors.As(err, &fe)
}

// Segment represents one log-segment file.
// Only the active (highest-ID) segment is writable; all others are read-only.
type Segment struct {
	id       uint32
	path     string
	f        vfs.File
	size     int64 // tracked manually; O_APPEND seeks can't be trusted
	writable bool
}

// FileName returns the canonical file name for a segment ID.
// Example: FileName(42) == "000042.log".
func FileName(id uint32) string {
	return fmt.Sprintf("%06d.log", id)
}

// filePath constructs the full path for a segment.
func filePath(dir string, id uint32) string {
	return dir + string(os.PathSeparator) + FileName(id)
}

// Create opens a brand-new segment file. Fails if the file already exists.
func Create(fs vfs.FS, dir string, id uint32) (*Segment, error) {
	path := filePath(dir, id)
	f, err := fs.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("segment create %s: %w", path, err)
	}
	return &Segment{id: id, path: path, f: f, size: 0, writable: true}, nil
}

// OpenAppend opens an existing segment for appending. The current file size
// is read from Stat so the caller can pass it as the initial position for the
// next Append call.
func OpenAppend(fs vfs.FS, dir string, id uint32) (*Segment, error) {
	path := filePath(dir, id)
	f, err := fs.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("segment open-append %s: %w", path, err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("segment stat %s: %w", path, err)
	}
	return &Segment{id: id, path: path, f: f, size: fi.Size(), writable: true}, nil
}

// OpenRead opens an existing segment for reading only.
func OpenRead(fs vfs.FS, dir string, id uint32) (*Segment, error) {
	path := filePath(dir, id)
	f, err := fs.OpenFile(path, os.O_RDONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("segment open-read %s: %w", path, err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("segment stat %s: %w", path, err)
	}
	return &Segment{id: id, path: path, f: f, size: fi.Size(), writable: false}, nil
}

// ID returns the segment's numeric identifier.
func (s *Segment) ID() uint32 { return s.id }

// Size returns the number of bytes currently in the segment.
func (s *Segment) Size() int64 { return s.size }

// Path returns the file system path of this segment.
func (s *Segment) Path() string { return s.path }

// Append writes b at the end of the segment and returns the starting offset.
//
// Critical behaviour on partial write:
// If Write() writes only some bytes before failing, the file contains a
// garbage tail. We immediately try to truncate back to the pre-write size.
// If truncation also fails, we return a FatalError – the DB must stop writing.
func (s *Segment) Append(b []byte) (offset int64, err error) {
	if !s.writable {
		return 0, fmt.Errorf("segment %d: append on read-only segment", s.id)
	}
	start := s.size
	n, writeErr := s.f.Write(b)
	if writeErr != nil {
		if n > 0 {
			// Partial write: attempt to undo it.
			if truncErr := s.f.Truncate(start); truncErr != nil {
				// Truncation failed: the file is in an unknown state.
				return 0, &FatalError{
					Err: fmt.Errorf("partial write (%d/%d bytes) and truncate failed: %w", n, len(b), truncErr),
				}
			}
			// Truncation succeeded: file is back to its pre-write size.
		}
		return 0, writeErr
	}
	s.size += int64(n)
	return start, nil
}

// ReadAt fills p from the given offset. Returns io.ErrUnexpectedEOF if there
// are not enough bytes starting at off.
func (s *Segment) ReadAt(p []byte, off int64) error {
	_, err := s.f.ReadAt(p, off)
	return err
}

// Sync flushes OS write buffers to durable storage.
// On error the caller must treat the segment (and the DB) as broken.
func (s *Segment) Sync() error {
	if err := s.f.Sync(); err != nil {
		// A failed fsync means we do not know what reached disk.
		// Wrap in FatalError so the DB enters fail-stop mode.
		return &FatalError{Err: fmt.Errorf("sync segment %d: %w", s.id, err)}
	}
	return nil
}

// Truncate sets the segment's size, discarding data beyond the new length.
// Used during recovery to remove a torn tail.
func (s *Segment) Truncate(size int64) error {
	if err := s.f.Truncate(size); err != nil {
		return fmt.Errorf("truncate segment %d to %d: %w", s.id, size, err)
	}
	s.size = size
	return nil
}

// Close closes the underlying file handle.
func (s *Segment) Close() error {
	return s.f.Close()
}

// Scan reads all records sequentially from r, calling fn for each valid record.
//
// fn receives:
//   - off:  the byte offset at which this record starts (relative to the start
//     of r, assuming r starts at offset 0 in the segment).
//   - rec:  the decoded record.
//   - size: the total bytes this record occupies on disk.
//
// Return values:
//   - good: the offset just after the last successfully decoded record.
//   - err:  nil (clean EOF), record.ErrTruncated, record.ErrCorrupt, or an
//     error returned by fn.
//
// Policy: we stop at the first bad record. Any valid data appearing after a
// bad record in the same file is dropped. This matches standard WAL behaviour:
// a bad record means the writer died there.
func Scan(r io.Reader, fn func(off int64, rec record.Record, size int) error) (good int64, err error) {
	// Wrap in a buffered reader for efficiency (large files).
	br := bufio.NewReaderSize(r, 1<<20)
	var off int64
	for {
		rec, size, decErr := record.Decode(br)
		if decErr != nil {
			if errors.Is(decErr, io.EOF) {
				// Clean end of file: all records were read successfully.
				return off, nil
			}
			// ErrTruncated or ErrCorrupt: stop here and return the error.
			return off, decErr
		}
		if fnErr := fn(off, rec, size); fnErr != nil {
			return off, fnErr
		}
		off += int64(size)
		good = off
	}
}
