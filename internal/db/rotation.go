package db

import (
	"fmt"

	"kvstore/internal/segment"
)

// rotateLocked creates a new active segment, making the current one read-only.
//
// Must be called with db.mu held (write lock).
//
// Order of operations (crash safety):
//  1. Sync the current active segment – it must be fully durable before we leave it.
//  2. Create the new segment file.
//  3. SyncDir so the new file's directory entry is durable.
//  4. The old active handle stays open in db.segs for future ReadAt calls.
//  5. Switch db.active to the new segment.
func (db *DB) rotateLocked() error {
	// 1. Sync the old segment before we abandon it.
	if err := db.active.Sync(); err != nil {
		db.broken = err
		return fmt.Errorf("rotate: sync old segment %d: %w", db.active.ID(), err)
	}

	// 2. Create the new segment.
	newID := db.nextFileID
	db.nextFileID++
	newSeg, err := segment.Create(db.opts.FS, db.opts.Dir, newID)
	if err != nil {
		return fmt.Errorf("rotate: create segment %d: %w", newID, err)
	}

	// 3. SyncDir so the new file's name survives a crash.
	if err := db.opts.FS.SyncDir(db.opts.Dir); err != nil {
		newSeg.Close()
		return fmt.Errorf("rotate: syncdir after create segment %d: %w", newID, err)
	}

	// 4. The old active handle stays in db.segs.
	//    (It is already registered from when it was created.)

	// 5. Switch to the new active segment.
	db.segs[newID] = newSeg
	db.active = newSeg
	db.dirty = false
	return nil
}
