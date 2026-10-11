package db

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"kvstore/internal/index"
	"kvstore/internal/record"
	"kvstore/internal/segment"
)

// recover rebuilds the in-memory index from all segment files on disk.
// It handles:
//   - No existing segments: creates the first one.
//   - Torn tail in the last segment: truncates and continues.
//   - Corruption in any segment other than the last: returns an error (never
//     silently drops committed data).
//   - Leftover .tmp files (crashed compaction): deleted on startup.
//   - Newest-sequence-wins when duplicate keys appear across files.
func recover(db *DB) error {
	start := time.Now()
	dir := db.opts.Dir
	fs := db.opts.FS

	entries, err := fs.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("db open: readdir %s: %w", dir, err)
	}

	// --- Step 1: clean up leftover temp files and collect log IDs ------------
	var logIDs []uint32
	logIDSet := make(map[uint32]bool)

	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".tmp") {
			path := filepath.Join(dir, name)
			log.Printf("recovery: removing leftover temp file %s", name)
			fs.Remove(path) //nolint:errcheck
			continue
		}
		if strings.HasSuffix(name, ".log") {
			id, parseErr := parseSegmentID(name)
			if parseErr == nil {
				logIDs = append(logIDs, id)
				logIDSet[id] = true
			}
		}
	}

	// Remove orphan .hint files (hint without a matching .log).
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".hint") {
			stem := strings.TrimSuffix(name, ".hint")
			id64, parseErr := strconv.ParseUint(stem, 10, 32)
			if parseErr != nil || !logIDSet[uint32(id64)] {
				log.Printf("recovery: removing orphan hint file %s", name)
				fs.Remove(filepath.Join(dir, name)) //nolint:errcheck
			}
		}
	}

	// --- Step 2: no segments at all – create the first one -------------------
	if len(logIDs) == 0 {
		seg, createErr := segment.Create(fs, dir, 1)
		if createErr != nil {
			return fmt.Errorf("db open: create first segment: %w", createErr)
		}
		db.active = seg
		db.segs[1] = seg
		db.nextFileID = 2
		db.nextSeq = 1
		db.recovery = RecoveryInfo{Duration: time.Since(start)}
		return nil
	}

	sort.Slice(logIDs, func(i, j int) bool { return logIDs[i] < logIDs[j] })
	maxID := logIDs[len(logIDs)-1]

	// --- Step 3: scan all segments and rebuild the index ---------------------
	tombs := make(map[string]uint64) // key → seq of the newest DELETE seen
	var maxSeq uint64
	var truncatedBytes int64

	for _, id := range logIDs {
		isLast := id == maxID

		// Try hint file for non-last, non-active segments when enabled.
		if !isLast && db.opts.UseHintFiles {
			applied, _ := applyHintFileReal(db, dir, id, tombs, &maxSeq)
			if applied {
				seg, openErr := segment.OpenRead(fs, dir, id)
				if openErr != nil {
					return fmt.Errorf("recovery: open segment %d read-only: %w", id, openErr)
				}
				db.segs[id] = seg
				continue
			}
			// Hint failed or absent – fall through to full log scan.
		}

		// Open the raw file for scanning.
		rawPath := filepath.Join(dir, segment.FileName(id))
		f, openErr := os.Open(rawPath)
		if openErr != nil {
			return fmt.Errorf("recovery: open segment %d: %w", id, openErr)
		}

		br := bufio.NewReaderSize(f, 1<<20)
		scanGood, scanErr := segment.Scan(br, func(off int64, rec record.Record, size int) error {
			applyRecord(db.idx, tombs, &maxSeq, id, off, rec, size)
			return nil
		})
		f.Close()

		if scanErr == nil {
			// Clean scan – open a read-only handle for future Get calls.
			seg, openErr := segment.OpenRead(fs, dir, id)
			if openErr != nil {
				return fmt.Errorf("recovery: open segment %d read-only: %w", id, openErr)
			}
			db.segs[id] = seg
			continue
		}

		if errors.Is(scanErr, record.ErrTruncated) || errors.Is(scanErr, record.ErrCorrupt) {
			if !isLast {
				// Corruption in a sealed (immutable) segment is fatal.
				return fmt.Errorf("recovery: corruption in segment %d at offset %d: %w",
					id, scanGood, scanErr)
			}
			// Torn tail in the last segment: open for append and truncate.
			seg, openErr := segment.OpenAppend(fs, dir, id)
			if openErr != nil {
				return fmt.Errorf("recovery: open active segment %d: %w", id, openErr)
			}
			origSize := seg.Size()
			if truncErr := seg.Truncate(scanGood); truncErr != nil {
				seg.Close()
				return fmt.Errorf("recovery: truncate segment %d: %w", id, truncErr)
			}
			if syncErr := seg.Sync(); syncErr != nil {
				seg.Close()
				return fmt.Errorf("recovery: sync after truncate segment %d: %w", id, syncErr)
			}
			truncatedBytes = origSize - scanGood
			if truncatedBytes > 0 {
				log.Printf("recovery: truncated %d bytes of damaged tail from %s",
					truncatedBytes, segment.FileName(id))
			}
			db.active = seg
			db.segs[id] = seg
			continue
		}

		return fmt.Errorf("recovery: scan segment %d: %w", id, scanErr)
	}

	// --- Step 4: if active was not set, open the highest-ID segment ----------
	if db.active == nil {
		// Replace the existing read-only handle with an append-capable one.
		if old, ok := db.segs[maxID]; ok {
			old.Close()
		}
		seg, openErr := segment.OpenAppend(fs, dir, maxID)
		if openErr != nil {
			return fmt.Errorf("recovery: open active segment %d: %w", maxID, openErr)
		}
		db.active = seg
		db.segs[maxID] = seg
	}

	// --- Step 5: finalise state ----------------------------------------------
	db.nextSeq = maxSeq + 1
	db.nextFileID = maxID + 1

	// Recompute liveBytes by iterating the index.
	var live int64
	db.idx.ForEach(func(_ string, e index.Entry) bool {
		live += int64(e.Size)
		return true
	})
	db.liveBytes = live

	db.recovery = RecoveryInfo{
		Keys:           db.idx.Len(),
		Segments:       len(db.segs),
		TruncatedBytes: truncatedBytes,
		Duration:       time.Since(start),
	}
	return nil
}

// applyRecord applies one scanned record to the index using the
// newest-sequence-wins rule. Tombstones (DELETE records) are tracked in tombs.
//
// Invariant: the index is never ahead of the log. This function is called
// during log scanning, so every record it indexes is already on disk.
func applyRecord(idx index.Index, tombs map[string]uint64, maxSeq *uint64,
	fileID uint32, off int64, rec record.Record, size int) {

	key := string(rec.Key)
	s := rec.Seq

	if s > *maxSeq {
		*maxSeq = s
	}

	cur, ok := idx.Get(key)
	tombSeq := tombs[key] // 0 if absent; higher means a newer DELETE was seen

	if rec.Type == record.TypePut {
		// Update the index only if this PUT is newer than both the current
		// index entry AND any tombstone for this key.
		if (!ok || s > cur.Seq) && s > tombSeq {
			idx.Put(key, index.Entry{
				FileID: fileID,
				Offset: off,
				Size:   uint32(size),
				Seq:    s,
			})
			delete(tombs, key) // this PUT supersedes the old tombstone
		}
	} else { // TypeDelete (tombstone)
		if (!ok || s > cur.Seq) && s > tombSeq {
			if ok {
				idx.Delete(key)
			}
			tombs[key] = s
		}
	}
}

// parseSegmentID parses the numeric ID from a file name like "000042.log".
func parseSegmentID(name string) (uint32, error) {
	stem := strings.TrimSuffix(name, ".log")
	if len(stem) != 6 {
		return 0, fmt.Errorf("unexpected segment name: %s", name)
	}
	id, err := strconv.ParseUint(stem, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}


