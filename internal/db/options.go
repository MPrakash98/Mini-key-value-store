package db

import (
	"time"

	"kvstore/internal/vfs"
)

// SyncPolicy controls when the active segment is fsynced to durable storage.
type SyncPolicy int

const (
	// SyncAlways fsyncs after every write. Safest, slowest.
	// On power loss, every Put that returned nil is guaranteed to be durable.
	SyncAlways SyncPolicy = iota

	// SyncInterval fsyncs in the background every SyncEvery interval.
	// On power loss, writes acknowledged in the last SyncEvery window may be lost.
	SyncInterval

	// SyncNever leaves flushing to the OS page cache.
	// Fastest, but any unsynced writes may be lost on power loss.
	SyncNever
)

const (
	// DefaultSegmentMaxBytes is the default maximum segment file size (64 MiB).
	// When the active segment would exceed this, it is rotated.
	DefaultSegmentMaxBytes int64 = 64 << 20

	// DefaultSyncEvery is the background fsync interval for SyncInterval.
	DefaultSyncEvery = 100 * time.Millisecond
)

// Options configures an Open call.
type Options struct {
	// Dir is the data directory. Required.
	Dir string

	// FS is the file system implementation. Defaults to vfs.OS().
	FS vfs.FS

	// SegmentMaxBytes is the maximum segment file size before rotation.
	// Defaults to 64 MiB.
	SegmentMaxBytes int64

	// SyncPolicy controls fsync behaviour. Defaults to SyncAlways.
	SyncPolicy SyncPolicy

	// SyncEvery is the interval between background fsyncs (SyncInterval only).
	// Defaults to 100 ms.
	SyncEvery time.Duration

	// UseHintFiles enables reading hint files for fast recovery of compacted segments.
	// Defaults to true.
	UseHintFiles bool
}

// defaults fills in zero-value fields with their default values.
func (o *Options) defaults() {
	if o.FS == nil {
		o.FS = vfs.OS()
	}
	if o.SegmentMaxBytes <= 0 {
		o.SegmentMaxBytes = DefaultSegmentMaxBytes
	}
	if o.SyncEvery <= 0 {
		o.SyncEvery = DefaultSyncEvery
	}
}

// Stats is a point-in-time snapshot of the database's internal state.
type Stats struct {
	// Keys is the number of live keys in the index.
	Keys int
	// Segments is the number of open segment files.
	Segments int
	// DiskBytes is the total size of all segment files.
	DiskBytes int64
	// LiveBytes is the sum of the on-disk sizes of all live index entries.
	LiveBytes int64
	// NextSeq is the sequence number that will be assigned to the next write.
	NextSeq uint64
}

// RecoveryInfo is populated by Open and describes what happened during recovery.
type RecoveryInfo struct {
	// Keys is the number of live keys recovered.
	Keys int
	// Segments is the number of segments scanned.
	Segments int
	// TruncatedBytes is the number of bytes removed from the tail of the last segment.
	TruncatedBytes int64
	// Duration is how long recovery took.
	Duration time.Duration
}
