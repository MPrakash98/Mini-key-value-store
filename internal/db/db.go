package db

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"kvstore/internal/index"
	"kvstore/internal/record"
	"kvstore/internal/segment"
)

// DB is the main key-value store engine.
//
// Invariants (never break these):
//  1. Log before index: the index never points to data not in the log.
//  2. Only the active (highest-ID) segment is written.
//  3. Seq is strictly increasing.
//  4. After any Sync or partial-write error, the DB enters fail-stop mode.
type DB struct {
	opts       Options
	mu         sync.RWMutex
	idx        index.Index
	segs       map[uint32]*segment.Segment // all open segment handles (read + active)
	active     *segment.Segment
	nextSeq    uint64
	nextFileID uint32
	liveBytes  int64   // sum of Entry.Size over all live index entries
	dirty      bool    // active segment has unsynced writes
	closed     bool
	broken     error   // set on fatal I/O error; non-nil → fail-stop
	compactMu  sync.Mutex
	stopSync   chan struct{}
	wg         sync.WaitGroup
	recovery   RecoveryInfo
}

// Open opens or creates a key-value store in the given directory.
// Full crash recovery is performed before returning.
func Open(opts Options) (*DB, error) {
	opts.defaults()
	db := &DB{
		opts:     opts,
		idx:      index.NewHash(),
		segs:     make(map[uint32]*segment.Segment),
		stopSync: make(chan struct{}),
	}
	if err := db.opts.FS.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("db open: mkdir: %w", err)
	}
	if err := recover(db); err != nil {
		return nil, err
	}
	if opts.SyncPolicy == SyncInterval {
		db.wg.Add(1)
		go db.syncLoop()
	}
	return db, nil
}

// Put inserts or updates the key with the given value.
//
// Write path (always in this order):
//  1. Validate key/value lengths.
//  2. Encode record with the current Seq.
//  3. Rotate active segment if needed.
//  4. Append to the active segment.
//  5. Fsync per SyncPolicy.
//  6. Update the in-memory index.
//  7. Advance nextSeq.
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.putLocked(key, value)
}

// putLocked must be called with db.mu held.
func (db *DB) putLocked(key, value []byte) error {
	if db.closed {
		return ErrClosed
	}
	if db.broken != nil {
		return ErrBroken
	}
	// Validate lengths.
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if len(key) > record.MaxKeySize {
		return ErrKeyTooLong
	}
	if len(value) > record.MaxValueSize {
		return ErrValueTooLong
	}

	seq := db.nextSeq
	buf := record.Encode(record.Record{Seq: seq, Type: record.TypePut, Key: key, Value: value})

	// Phase 7: rotate if this record would push us over the size limit.
	if db.active.Size() > 0 && db.active.Size()+int64(len(buf)) > db.opts.SegmentMaxBytes {
		if err := db.rotateLocked(); err != nil {
			return err
		}
	}

	off, err := db.active.Append(buf)
	if err != nil {
		if segment.IsFatal(err) {
			db.broken = err
		}
		return err
	}

	if db.opts.SyncPolicy == SyncAlways {
		if serr := db.active.Sync(); serr != nil {
			// Failed fsync: we don't know what reached disk. Enter fail-stop.
			db.broken = serr
			return serr
		}
	} else {
		db.dirty = true
	}

	// Only update the index AFTER a successful append (and sync if required).
	// This preserves the "log before index" invariant.
	old, existed := db.idx.Put(string(key), index.Entry{
		FileID: db.active.ID(),
		Offset: off,
		Size:   uint32(len(buf)),
		Seq:    seq,
	})
	if existed {
		db.liveBytes -= int64(old.Size)
	}
	db.liveBytes += int64(len(buf))
	db.nextSeq++
	return nil
}

// Get retrieves the value associated with key.
// Returns ErrNotFound if the key does not exist or has been deleted.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	e, ok := db.idx.Get(string(key))
	if !ok {
		return nil, ErrNotFound
	}

	seg, ok := db.segs[e.FileID]
	if !ok {
		return nil, fmt.Errorf("db: index points to unknown segment %d", e.FileID)
	}

	buf := make([]byte, e.Size)
	if err := seg.ReadAt(buf, e.Offset); err != nil {
		return nil, fmt.Errorf("db: read segment %d at %d: %w", e.FileID, e.Offset, err)
	}

	rec, err := record.DecodeBytes(buf)
	if err != nil {
		return nil, fmt.Errorf("db: decode record at seg %d off %d: %w", e.FileID, e.Offset, err)
	}

	// Sanity: the index must point to a PUT record with the correct key.
	if rec.Type != record.TypePut {
		return nil, fmt.Errorf("db: corrupt index: expected TypePut at seg %d off %d", e.FileID, e.Offset)
	}
	if !bytes.Equal(rec.Key, key) {
		return nil, fmt.Errorf("db: corrupt index: key mismatch at seg %d off %d", e.FileID, e.Offset)
	}

	return rec.Value, nil
}

// Delete removes a key from the store by appending a tombstone record.
// Deleting a key that does not exist is a no-op (returns nil, writes nothing).
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.deleteLocked(key)
}

// deleteLocked must be called with db.mu held.
func (db *DB) deleteLocked(key []byte) error {
	if db.closed {
		return ErrClosed
	}
	if db.broken != nil {
		return ErrBroken
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if len(key) > record.MaxKeySize {
		return ErrKeyTooLong
	}

	// Deleting a key not in the index is a no-op.
	old, existed := db.idx.Get(string(key))
	if !existed {
		return nil
	}

	seq := db.nextSeq
	buf := record.Encode(record.Record{Seq: seq, Type: record.TypeDelete, Key: key})

	// Rotate if needed.
	if db.active.Size() > 0 && db.active.Size()+int64(len(buf)) > db.opts.SegmentMaxBytes {
		if err := db.rotateLocked(); err != nil {
			return err
		}
	}

	_, err := db.active.Append(buf)
	if err != nil {
		if segment.IsFatal(err) {
			db.broken = err
		}
		return err
	}

	if db.opts.SyncPolicy == SyncAlways {
		if serr := db.active.Sync(); serr != nil {
			db.broken = serr
			return serr
		}
	} else {
		db.dirty = true
	}

	db.idx.Delete(string(key))
	db.liveBytes -= int64(old.Size)
	db.nextSeq++
	return nil
}

// Sync forces an fsync of the active segment regardless of SyncPolicy.
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.broken != nil {
		return ErrBroken
	}
	if err := db.active.Sync(); err != nil {
		db.broken = err
		return err
	}
	db.dirty = false
	return nil
}

// Close syncs, stops background goroutines, and closes all file handles.
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	db.closed = true
	// Signal the sync goroutine to stop.
	close(db.stopSync)
	db.mu.Unlock()

	// Wait for background goroutines.
	db.wg.Wait()

	db.mu.Lock()
	defer db.mu.Unlock()

	// Final sync of the active segment.
	if db.broken == nil && db.dirty {
		db.active.Sync() //nolint:errcheck – best effort on Close
	}

	// Close all segment handles.
	var firstErr error
	for _, seg := range db.segs {
		if err := seg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Stats returns a point-in-time snapshot of the DB's internal state.
func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var diskBytes int64
	for _, seg := range db.segs {
		diskBytes += seg.Size()
	}
	return Stats{
		Keys:      db.idx.Len(),
		Segments:  len(db.segs),
		DiskBytes: diskBytes,
		LiveBytes: db.liveBytes,
		NextSeq:   db.nextSeq,
	}
}

// RecoveryInfo returns information about what happened during the last Open.
func (db *DB) RecoveryInfo() RecoveryInfo { return db.recovery }

// syncLoop is the background goroutine for SyncInterval policy.
// It fsyncs the active segment every SyncEvery, if there are dirty writes.
func (db *DB) syncLoop() {
	defer db.wg.Done()
	t := time.NewTicker(db.opts.SyncEvery)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			db.mu.Lock()
			if db.dirty && db.broken == nil {
				if err := db.active.Sync(); err != nil {
					db.broken = err
				} else {
					db.dirty = false
				}
			}
			db.mu.Unlock()
		case <-db.stopSync:
			return
		}
	}
}

// diskBytes returns total size of all segment files. Caller must hold at least RLock.
func (db *DB) diskBytes() int64 {
	var total int64
	for _, seg := range db.segs {
		total += seg.Size()
	}
	return total
}

// NeedsCompaction reports whether the dead-data ratio meets or exceeds deadRatio.
// deadRatio is a fraction in [0, 1]; e.g. 0.5 means ≥50% dead data.
func (db *DB) NeedsCompaction(deadRatio float64) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	total := db.diskBytes()
	if total == 0 {
		return false
	}
	dead := total - db.liveBytes
	return float64(dead)/float64(total) >= deadRatio
}
