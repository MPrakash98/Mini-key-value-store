package db

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"

	"kvstore/internal/index"
	"kvstore/internal/record"
	"kvstore/internal/segment"
)

// move records a single PUT record being relocated from its old position to
// the merged segment during compaction.
type move struct {
	key     string
	oldFile uint32
	oldOff  int64
	newOff  int64
	size    int
	seq     uint64
}

// Compact merges all segment files that are older than the current active
// segment into a single compacted file, reclaiming space used by stale and
// deleted records.
//
// Design (crash-safe at every point):
//  1. The output is written to NNNNNN.log.tmp, then atomically renamed.
//     A crash before the rename leaves a .tmp file; Open() deletes .tmp files.
//  2. The merged segment gets an ID lower than the new active segment, so the
//     active is always the highest ID (Phase 4.1 invariant).
//  3. Records are copied with their original Seq.  The newest-seq-wins rule in
//     recovery handles any duplicates left by a partial crash.
//  4. Tombstones are kept unless the key is live again (a newer PUT exists).
func (db *DB) Compact() error {
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	// ---- Phase A: freeze inputs (brief write lock) ---------------------------
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	if db.broken != nil {
		db.mu.Unlock()
		return ErrBroken
	}

	mergeID := db.nextFileID
	db.nextFileID++

	// Rotate so that new writes go to a fresh active segment (mergeID+1).
	if err := db.rotateLocked(); err != nil {
		db.nextFileID-- // roll back the reserved ID
		db.mu.Unlock()
		return fmt.Errorf("compact: rotate: %w", err)
	}

	// Collect all segment IDs older than mergeID (they are now immutable).
	var inputIDs []uint32
	for id := range db.segs {
		if id < mergeID {
			inputIDs = append(inputIDs, id)
		}
	}
	db.mu.Unlock()

	if len(inputIDs) == 0 {
		// Nothing to compact: release the reserved mergeID.
		// The newly created active segment stays; its ID is fine.
		log.Printf("compact: nothing to compact")
		return nil
	}

	// Sort so we process segments in order (for deterministic Seq ordering).
	sortUint32(inputIDs)

	// ---- Phase B: copy live records (no big lock held) ----------------------
	tmpPath := filepath.Join(db.opts.Dir, fmt.Sprintf("%06d.log.tmp", mergeID))
	tmpFile, err := db.opts.FS.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("compact: create tmp: %w", err)
	}

	bw := bufio.NewWriterSize(tmpFile, 1<<20)
	var newOff int64
	var moves []move

	for _, id := range inputIDs {
		db.mu.RLock()
		seg, ok := db.segs[id]
		db.mu.RUnlock()
		if !ok {
			continue
		}

		// Open the file for sequential reading.
		rawPath := filepath.Join(db.opts.Dir, segment.FileName(id))
		f, openErr := os.Open(rawPath)
		if openErr != nil {
			tmpFile.Close()
			db.opts.FS.Remove(tmpPath)
			return fmt.Errorf("compact: open segment %d: %w", id, openErr)
		}
		_ = seg // keep reference

		br := bufio.NewReaderSize(f, 1<<20)
		scanErr := scanForCompaction(br, id, db, bw, &newOff, &moves)
		f.Close()
		if scanErr != nil {
			tmpFile.Close()
			db.opts.FS.Remove(tmpPath)
			return fmt.Errorf("compact: scan segment %d: %w", id, scanErr)
		}
	}

	// Flush the buffered writer and fsync before renaming.
	if err := bw.Flush(); err != nil {
		tmpFile.Close()
		db.opts.FS.Remove(tmpPath)
		return fmt.Errorf("compact: flush tmp: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		db.opts.FS.Remove(tmpPath)
		return fmt.Errorf("compact: sync tmp: %w", err)
	}
	tmpFile.Close()

	if newOff == 0 {
		// Nothing was written (all records are dead/tombstoned away).
		db.opts.FS.Remove(tmpPath)
		// Still need to remove the input segments from db.segs.
		db.swapCompacted(mergeID, nil, moves, inputIDs)
		return nil
	}

	// Atomic rename: tmp → final name.
	finalPath := filepath.Join(db.opts.Dir, segment.FileName(mergeID))
	if err := db.opts.FS.Rename(tmpPath, finalPath); err != nil {
		db.opts.FS.Remove(tmpPath)
		return fmt.Errorf("compact: rename tmp: %w", err)
	}
	if err := db.opts.FS.SyncDir(db.opts.Dir); err != nil {
		return fmt.Errorf("compact: syncdir after rename: %w", err)
	}

	// (Phase 8b) Write hint file for fast recovery of this compacted segment.
	db.writeHintFile(mergeID, moves)

	// ---- Phase C: swap (write lock) -----------------------------------------
	mergedSeg, err := segment.OpenRead(db.opts.FS, db.opts.Dir, mergeID)
	if err != nil {
		return fmt.Errorf("compact: open merged segment: %w", err)
	}
	db.swapCompacted(mergeID, mergedSeg, moves, inputIDs)

	log.Printf("compact: merged %d segments into %s (%d bytes live)",
		len(inputIDs), segment.FileName(mergeID), newOff)
	return nil
}

// scanForCompaction scans one input segment and writes live records to bw.
func scanForCompaction(br io.Reader, id uint32, db *DB, bw *bufio.Writer,
	newOff *int64, moves *[]move) error {

	_, err := segment.Scan(br, func(off int64, rec record.Record, size int) error {
		db.mu.RLock()
		e, ok := db.idx.Get(string(rec.Key))
		db.mu.RUnlock()

		var keep bool
		if rec.Type == record.TypePut {
			// Keep only if THIS exact record is the one the index currently points to.
			keep = ok && e.FileID == id && e.Offset == off
		} else { // TypeDelete (tombstone)
			// Keep the tombstone unless the key is live again (a newer PUT exists).
			keep = !ok
		}

		if !keep {
			return nil
		}

		b := record.Encode(rec) // re-encode with original Seq
		if _, writeErr := bw.Write(b); writeErr != nil {
			return writeErr
		}
		if rec.Type == record.TypePut {
			*moves = append(*moves, move{
				key:     string(rec.Key),
				oldFile: id,
				oldOff:  off,
				newOff:  *newOff,
				size:    len(b),
				seq:     rec.Seq,
			})
		}
		*newOff += int64(len(b))
		return nil
	})
	return err
}

// swapCompacted holds the write lock while atomically updating the index and
// removing the old input segments. If mergedSeg is nil, no merged file exists.
func (db *DB) swapCompacted(mergeID uint32, mergedSeg *segment.Segment, moves []move, inputIDs []uint32) {
	db.mu.Lock()
	if mergedSeg != nil {
		db.segs[mergeID] = mergedSeg
	}

	// Compare-and-swap: only repoint the index if a concurrent write hasn't
	// superseded this record while compaction was running without the lock.
	for _, m := range moves {
		e, ok := db.idx.Get(m.key)
		if ok && e.FileID == m.oldFile && e.Offset == m.oldOff {
			db.idx.Put(m.key, index.Entry{
				FileID: mergeID,
				Offset: m.newOff,
				Size:   uint32(m.size),
				Seq:    m.seq,
			})
		}
	}

	// Remove old segment handles while holding the lock so no reader touches a
	// closed file (invariant 10).
	for _, id := range inputIDs {
		if seg, ok := db.segs[id]; ok {
			seg.Close()
			delete(db.segs, id)
		}
	}
	// Recompute liveBytes.
	var live int64
	db.idx.ForEach(func(_ string, e index.Entry) bool {
		live += int64(e.Size)
		return true
	})
	db.liveBytes = live
	db.mu.Unlock()

	// Delete the old segment files and their hints outside the lock.
	for _, id := range inputIDs {
		path := filepath.Join(db.opts.Dir, segment.FileName(id))
		db.opts.FS.Remove(path) //nolint:errcheck
		hintPath := filepath.Join(db.opts.Dir, fmt.Sprintf("%06d.hint", id))
		db.opts.FS.Remove(hintPath) //nolint:errcheck
	}
	if err := db.opts.FS.SyncDir(db.opts.Dir); err != nil {
		log.Printf("compact: syncdir after delete: %v", err)
	}
}

// ---- Phase 8b: Hint files ---------------------------------------------------

// hintHeaderSize is the fixed part of a hint entry before the key.
// Layout: CRC32(4) | Seq(8) | Type(1) | KeyLen(4) | Offset(8) | RecordSize(4) = 29 bytes
const hintHeaderSize = 29

// writeHintFile writes a .hint file for the compacted segment.
// The hint file allows fast recovery: it contains the index data without the
// values, so recovery can skip reading big values.
//
// The hint file is written AFTER the log file is renamed (log-first, then hint).
// If we crash between log rename and hint write, recovery falls back to the log.
func (db *DB) writeHintFile(mergeID uint32, moves []move) {
	if !db.opts.UseHintFiles || len(moves) == 0 {
		return
	}

	hintTmp := filepath.Join(db.opts.Dir, fmt.Sprintf("%06d.hint.tmp", mergeID))
	hintFinal := filepath.Join(db.opts.Dir, fmt.Sprintf("%06d.hint", mergeID))

	f, err := db.opts.FS.OpenFile(hintTmp, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		log.Printf("compact: create hint tmp: %v", err)
		return
	}

	bw := bufio.NewWriterSize(f, 1<<20)
	for _, m := range moves {
		if !writeHintEntry(bw, m) {
			f.Close()
			db.opts.FS.Remove(hintTmp)
			return
		}
	}
	if err := bw.Flush(); err != nil {
		log.Printf("compact: flush hint: %v", err)
		f.Close()
		db.opts.FS.Remove(hintTmp)
		return
	}
	if err := f.Sync(); err != nil {
		log.Printf("compact: sync hint: %v", err)
		f.Close()
		db.opts.FS.Remove(hintTmp)
		return
	}
	f.Close()

	if err := db.opts.FS.Rename(hintTmp, hintFinal); err != nil {
		log.Printf("compact: rename hint: %v", err)
		db.opts.FS.Remove(hintTmp)
		return
	}
	db.opts.FS.SyncDir(db.opts.Dir) //nolint:errcheck
}

// writeHintEntry serialises one hint entry into bw.
func writeHintEntry(bw *bufio.Writer, m move) bool {
	keyBytes := []byte(m.key)
	buf := make([]byte, hintHeaderSize+len(keyBytes))

	binary.BigEndian.PutUint64(buf[4:12], m.seq)
	buf[12] = record.TypePut
	binary.BigEndian.PutUint32(buf[13:17], uint32(len(keyBytes)))
	binary.BigEndian.PutUint64(buf[17:25], uint64(m.newOff))
	binary.BigEndian.PutUint32(buf[25:29], uint32(m.size))
	copy(buf[hintHeaderSize:], keyBytes)

	// CRC covers bytes 4..end.
	binary.BigEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:]))

	_, err := bw.Write(buf)
	return err == nil
}

// readHintFile reads a .hint file and applies its entries to the index.
// Returns (true, nil) only if the whole file was valid.
// Any CRC error causes the entire hint to be discarded (the caller falls back
// to scanning the .log file).
func readHintFile(
	hintPath string,
	fileID uint32,
	idx index.Index,
	tombs map[string]uint64,
	maxSeq *uint64,
) (bool, error) {

	f, err := os.Open(hintPath)
	if err != nil {
		return false, err
	}
	defer f.Close()

	br := bufio.NewReaderSize(f, 1<<20)
	for {
		var header [hintHeaderSize]byte
		_, readErr := io.ReadFull(br, header[:])
		if readErr != nil {
			if readErr == io.EOF {
				break // clean end of hint file
			}
			return false, fmt.Errorf("hint read header: %w", readErr)
		}

		storedCRC := binary.BigEndian.Uint32(header[0:4])
		seq := binary.BigEndian.Uint64(header[4:12])
		recType := header[12]
		keyLen := binary.BigEndian.Uint32(header[13:17])
		off := int64(binary.BigEndian.Uint64(header[17:25]))
		recSize := binary.BigEndian.Uint32(header[25:29])

		if recType > 1 || keyLen == 0 || keyLen > record.MaxKeySize {
			return false, fmt.Errorf("hint: invalid header")
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(br, keyBytes); err != nil {
			return false, fmt.Errorf("hint read key: %w", err)
		}

		// Verify CRC over header[4:] + key.
		h := crc32.NewIEEE()
		h.Write(header[4:])
		h.Write(keyBytes)
		if h.Sum32() != storedCRC {
			return false, fmt.Errorf("hint: CRC mismatch")
		}

		key := string(keyBytes)
		if seq > *maxSeq {
			*maxSeq = seq
		}

		cur, ok := idx.Get(key)
		tombSeq := tombs[key]

		if recType == record.TypePut {
			if (!ok || seq > cur.Seq) && seq > tombSeq {
				idx.Put(key, index.Entry{
					FileID: fileID,
					Offset: off,
					Size:   recSize,
					Seq:    seq,
				})
				delete(tombs, key)
			}
		} else {
			if (!ok || seq > cur.Seq) && seq > tombSeq {
				if ok {
					idx.Delete(key)
				}
				tombs[key] = seq
			}
		}
	}
	return true, nil
}

// applyHintFile is the real implementation of the stub in recovery.go.
// It tries to apply the hint file for segment id; returns (true, nil) on success.
// The stub in recovery.go calls this function by name – they are in the same package.
func applyHintFileReal(db *DB, dir string, id uint32, tombs map[string]uint64, maxSeq *uint64) (bool, error) {
	hintPath := filepath.Join(dir, fmt.Sprintf("%06d.hint", id))
	return readHintFile(hintPath, id, db.idx, tombs, maxSeq)
}

// sortUint32 sorts a []uint32 slice in ascending order.
func sortUint32(s []uint32) {
	// Simple insertion sort – segments are few; O(n²) is fine here.
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
