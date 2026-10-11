# Mini Key-Value Store in Go: Complete Build Plan

> **Project:** A small, persistent, crash-safe key-value store (Bitcask-style: write-ahead log + in-memory index + compaction).
> **Language:** Go 1.22 or newer, standard library only (one optional dependency in a stretch goal).
> **Audience:** A developer or an LLM coding assistant building this step by step.

---

## 0. How to Use This Document (READ FIRST)

### 0.1 Rules for the LLM / developer doing the work

1. **Work on ONE phase at a time**, in order. Never start Phase N+1 until Phase N's *Gate* passes.
2. Every phase has: **Goal → Files → Spec → Steps → Tests → Gate (acceptance checklist)**.
3. **Write the tests in the same phase as the code.** A phase without passing tests is not done.
4. Run these commands after every phase. All must pass:
   ```bash
   gofmt -l .            # must print nothing
   go vet ./...
   go test -race ./...
   ```
5. If a test fails, **fix the code, not the test**, unless the test itself is provably wrong. If you change a test, explain why in a comment.
6. **Do not change the on-disk format** (Section 4) without updating this document and bumping the format version.
7. No global mutable state. Pass dependencies in (file system, options).
8. Wrap errors with context: `fmt.Errorf("open segment %d: %w", id, err)`.
9. Keep functions short (aim under 50 lines). Add a short comment explaining *why* for anything non-obvious (especially around fsync and ordering).
10. When unsure between "simple" and "clever," **choose simple**. This is a learning and demo project.

### 0.2 Prompt template to give an LLM for each phase

```text
You are implementing Phase <N> of KV_STORE_BUILD_PLAN.md.
Read Sections 1-5 (overview, rules, formats, layout) and then Phase <N> fully.
Implement only what Phase <N> asks. Write the listed tests.
When done, run gofmt, go vet, go test -race ./... and show me the output.
Then tick each item in the Phase <N> Gate checklist and say which test proves it.
Do not start any other phase.
```

---

## 1. What We Are Building

### 1.1 Required features

| # | Requirement | Where it is built |
|---|---|---|
| 1 | Storage engine (append-only log segments) | Phases 2-3, 5 |
| 2 | Write-ahead log + durability (fsync policies) | Phases 3, 5 |
| 3 | In-memory index | Phase 4 |
| 4 | File I/O (append, positioned reads, rotation) | Phases 2, 3, 7 |
| 5 | Crash recovery (torn-write detection, truncation) | Phase 6 |
| 6 | Compaction (garbage collection) | Phase 8 |
| 7 | Crash and fault-injection testing | Phase 9 |
| 8 | Benchmarking | Phase 10 |
| 9 | CLI to demo it | Phase 11 |
| S1 | Stretch: range queries | Stretch A |
| S2 | Stretch: snapshots | Stretch B |
| S3 | Stretch: simple replication | Stretch C |

### 1.2 Non-goals (say these out loud in your README)

- No transactions across multiple keys.
- No leader election or consensus (replication is a simple log-shipping demo).
- No compression or encryption.
- No Windows support guarantee (directory fsync and file locking differ). Linux and macOS are the targets.

### 1.3 How it works in plain words (the mental model)

Imagine a notebook where you **only ever write at the end of the page and never erase**.

- To **save** `name=Asha`, you write a new line at the end: `PUT name Asha`.
- To **change** it, you write another line at the end: `PUT name Priya`. The old line is now stale (garbage) but still there.
- To **delete** it, you write `DELETE name` at the end. This is called a **tombstone**.
- To **find** a key fast, you keep a small *index card* in your head (RAM): for every key, *which page and which position* has its latest line.
- If the power goes out, you re-read the notebook from the start and rebuild the index card. This is **recovery**.
- Over time the notebook fills with stale lines. **Compaction** copies only the latest lines into a fresh notebook and throws the old one away.

Why append-only? Appending is fast (sequential disk writes) and safe (we never half-overwrite old data). The log **is** the database. This is why the "WAL" and the "storage" are the same file in this design.

---

## 2. Technology Decisions

| Decision | Choice | Reason |
|---|---|---|
| Language | Go 1.22+ | Easy file I/O, goroutines, built-in testing and benchmarking |
| Module name | `kvstore` | Short imports: `kvstore/internal/record` |
| Dependencies | None (stdlib only) | More of the engine is your own work |
| Storage design | Bitcask (log + hash index) | Simplest design that still needs WAL, recovery, compaction |
| Checksum | CRC32 (IEEE) from `hash/crc32` | Detects torn and corrupt writes |
| Byte order | Big-endian | Consistent everywhere |
| Concurrency | One `sync.RWMutex` | Simple and correct |
| Testing | `testing` package + fault-injecting file system | Proves crash safety |

**Stdlib packages you will use:** `os`, `io`, `bufio`, `bytes`, `encoding/binary`, `hash/crc32`, `sort`, `sync`, `sync/atomic`, `time`, `errors`, `fmt`, `flag`, `net` (stretch), `testing`, `math/rand`.

---

## 3. Architecture

```text
                Put / Get / Delete / (Scan)
                          │
                 ┌────────▼─────────┐
                 │  DB  (RWMutex)   │
                 └───┬──────────┬───┘
      in-memory      │          │ append (write path)
      index          │          ▼
 key → {file,off,    │    ┌──────────────┐
        size,seq}    │    │ Active segment│  000003.log  ← the ONLY file we write to
                     │    └──────────────┘
                     │    ┌──────────────┐
      read path ─────┼───►│ Old segments  │  000001.log, 000002.log  (immutable)
      (ReadAt)       │    └──────────────┘
                     │
        Compaction: merges old segments → one new segment (+ optional .hint file)
```

### 3.1 The rule that makes everything safe

> **Write path order: (1) append to log → (2) fsync if the policy says so → (3) update the in-memory index → (4) reply to the caller.**
> The index is **never** ahead of the disk.

---

## 4. On-Disk Formats (the contract)

### 4.1 Directory contents

```text
data/
├── 000001.log          segment files (IDs increase; the highest ID is the ACTIVE segment)
├── 000002.log
├── 000003.log
├── 000002.hint         optional: fast-recovery index for a compacted segment
├── 000007.log.tmp      temporary file while compaction runs (deleted on startup)
└── LOCK                optional: prevents two processes opening the same directory
```

- File names use `fmt.Sprintf("%06d.log", id)`.
- **Highest-ID `.log` file = active segment.** (Compaction output is guaranteed a lower ID than the active segment. See Phase 8.)

### 4.2 Record format (every log record)

```text
 0        4        12     13        17        21
 ┌────────┬────────┬──────┬─────────┬─────────┬─────────┬───────────┐
 │ CRC32  │  Seq   │ Type │ KeyLen  │ ValLen  │   Key   │   Value   │
 │ 4 B    │  8 B   │ 1 B  │  4 B    │  4 B    │ KeyLen  │  ValLen   │
 └────────┴────────┴──────┴─────────┴─────────┴─────────┴───────────┘
 header = 21 bytes
```

- **CRC32** covers *every byte after the CRC field* (Seq through Value).
- **Seq**: a global, ever-increasing 64-bit number. Newest write has the highest Seq.
- **Type**: `0` = PUT, `1` = DELETE (tombstone). A tombstone has `ValLen = 0`.
- **Limits** (protect us from garbage lengths in a corrupt header): `MaxKeySize = 1 MiB`, `MaxValueSize = 64 MiB`. Empty keys are **not allowed**. Empty values **are** allowed for PUT.

### 4.3 Hint file format (optional, Phase 8b)

A hint file `NNNNNN.hint` describes the records inside `NNNNNN.log` **without the values**, so recovery can skip reading big values.

```text
Per entry:  CRC32(4) | Seq(8) | Type(1) | KeyLen(4) | Offset(8) | RecordSize(4) | Key
```
CRC32 covers everything after the CRC field. If **any** hint entry is bad, ignore the whole hint file and scan the `.log` instead. A hint is only a speed-up, never a source of truth.

---

## 5. Project Layout

```text
kvstore/
├── go.mod
├── Makefile
├── README.md
├── docs/
│   ├── DESIGN.md            (Phase 12)
│   └── BENCHMARKS.md        (Phase 10)
├── cmd/
│   ├── kvcli/main.go        interactive REPL (Phase 11)
│   ├── kvbench/main.go      load generator (Phase 10)
│   ├── kvcrash/main.go      kill -9 crash tester (Phase 9)
│   └── kvserver/main.go     (Stretch C)
├── internal/
│   ├── record/              encode/decode + CRC            (Phase 1)
│   ├── vfs/                 file system abstraction        (Phase 2)
│   ├── segment/             one log file: append/read/scan (Phase 3)
│   ├── index/               key → location                 (Phase 4)
│   └── db/                  the engine                     (Phases 5-8)
└── tests/
    └── crash/               crash and fault tests          (Phase 9)
```

The public API lives in `internal/db` and is re-exported by a thin top-level package `kvstore` if you want a clean import (optional).

---

## 6. Phases

---

### Phase 0: Project Setup

**Goal:** An empty but buildable project with tooling.

**Steps**
1. `mkdir kvstore && cd kvstore && go mod init kvstore`
2. Set `go 1.22` in `go.mod`.
3. Create the directory layout from Section 5 (empty packages need at least a `doc.go` with `package xxx`).
4. Create a `Makefile`:
   ```makefile
   .PHONY: fmt vet test bench
   fmt:   ; gofmt -l -w .
   vet:   ; go vet ./...
   test:  ; go test -race ./...
   bench: ; go test -bench=. -benchmem ./...
   ```
5. `git init`, add a `.gitignore` containing `data/`, `*.log`, `*.tmp`, `bench-results/`.

**Gate**
- [ ] `go build ./...` succeeds.
- [ ] `make test` runs (even with zero tests).

---

### Phase 1: The `record` Package (encode / decode / CRC)

**Goal:** Turn a key/value operation into bytes and back, detecting any damage.

**File:** `internal/record/record.go`

**Spec**
```go
package record

const (
    HeaderSize        = 21
    TypePut      byte = 0
    TypeDelete   byte = 1
    MaxKeySize        = 1 << 20  // 1 MiB
    MaxValueSize      = 64 << 20 // 64 MiB
)

var (
    ErrTruncated = errors.New("record: truncated")      // ran out of bytes mid-record
    ErrCorrupt   = errors.New("record: corrupt")        // bad CRC or impossible header
)

type Record struct {
    Seq   uint64
    Type  byte
    Key   []byte
    Value []byte
}

func (r Record) EncodedSize() int
func Encode(r Record) []byte
// Decode reads ONE record from rd. Returns (record, bytesConsumed, error).
//   io.EOF        -> zero bytes were available (clean end of file)
//   ErrTruncated  -> some bytes were read, then the stream ended early
//   ErrCorrupt    -> full bytes present but CRC/header invalid
func Decode(rd io.Reader) (Record, int, error)
func DecodeBytes(b []byte) (Record, error) // convenience wrapper around Decode
```

**Reference implementation of `Encode`**
```go
func Encode(r Record) []byte {
    buf := make([]byte, HeaderSize+len(r.Key)+len(r.Value))
    binary.BigEndian.PutUint64(buf[4:12], r.Seq)
    buf[12] = r.Type
    binary.BigEndian.PutUint32(buf[13:17], uint32(len(r.Key)))
    binary.BigEndian.PutUint32(buf[17:21], uint32(len(r.Value)))
    copy(buf[HeaderSize:], r.Key)
    copy(buf[HeaderSize+len(r.Key):], r.Value)
    binary.BigEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:]))
    return buf
}
```

**Decode algorithm (follow exactly)**
1. `io.ReadFull` the 21-byte header.
   - If it returns `io.EOF` (zero bytes read) → return `io.EOF`.
   - If it returns `io.ErrUnexpectedEOF` → return `ErrTruncated`.
2. Parse `Type`, `KeyLen`, `ValLen`.
3. **Validate BEFORE allocating**: `Type <= 1`, `1 <= KeyLen <= MaxKeySize`, `ValLen <= MaxValueSize`, and if Type is DELETE then `ValLen == 0`. Otherwise return `ErrCorrupt`. *(This prevents a corrupt header from making us allocate gigabytes.)*
4. `io.ReadFull` the body (`KeyLen + ValLen` bytes). Any EOF here → `ErrTruncated`.
5. Compute CRC32 over `header[4:] + body`; compare with the stored CRC. Mismatch → `ErrCorrupt`.
6. Return the record and `HeaderSize + KeyLen + ValLen` as bytes consumed.

**Tests** (`record_test.go`)
- Round trip: PUT, DELETE, empty value, 1-byte key, large value (1 MiB).
- Truncation at **every** byte offset of an encoded record (from 1 up to len-1) → must return `ErrTruncated`, never panic. Offset 0 → `io.EOF`.
- Flip **every single byte** of an encoded record one at a time → must return `ErrCorrupt` (or `ErrTruncated` is impossible here since length is unchanged; if the flip changes a length field the result may be `ErrCorrupt` or `ErrTruncated`, accept either, but never a "successful" decode with wrong data).
- Header with `KeyLen = 0`, `KeyLen > Max`, `Type = 7` → `ErrCorrupt`.
- Decoding several records back-to-back from one `bytes.Reader` returns them in order and consumed counts add up.
- A `FuzzDecode` test: random bytes must never panic.

**Gate**
- [ ] All tests above pass with `-race`.
- [ ] Fuzz runs 30 seconds without panics: `go test -fuzz=FuzzDecode -fuzztime=30s ./internal/record`.

---

### Phase 2: The `vfs` Package (file system abstraction)

**Goal:** All file access goes through an interface, so tests can later simulate crashes and failures. **No other package may call `os.OpenFile` etc. directly.**

**File:** `internal/vfs/vfs.go`, `internal/vfs/os.go`

**Spec**
```go
package vfs

type File interface {
    io.Reader
    io.Writer
    io.ReaderAt
    io.Closer
    Sync() error
    Truncate(size int64) error
    Stat() (os.FileInfo, error)
}

type FS interface {
    OpenFile(name string, flag int, perm os.FileMode) (File, error)
    Rename(oldpath, newpath string) error
    Remove(name string) error
    MkdirAll(path string, perm os.FileMode) error
    ReadDir(name string) ([]os.DirEntry, error)
    SyncDir(path string) error // fsync a DIRECTORY so created/renamed/deleted entries survive a crash
}

func OS() FS // real implementation backed by package os
```

**Notes**
- `SyncDir` on the OS implementation: `os.Open(dir)`, call `f.Sync()`, close. This is required after creating, renaming, or deleting files. Without it, the *file name itself* can vanish after power loss even if the contents were synced.
- `*os.File` already satisfies `File`.

**Tests**
- Using `t.TempDir()`: create, write, sync, read back, rename, remove, `SyncDir` returns nil.

**Gate**
- [ ] `go vet ./...` clean; test passes.

---

### Phase 3: The `segment` Package (one log file)

**Goal:** Manage one segment file: append records, read at an offset, scan all records.

**File:** `internal/segment/segment.go`

**Spec**
```go
package segment

type Segment struct { /* ID, path, vfs.File, size, writable */ }

func FileName(id uint32) string          // "000042.log"
func Create(fs vfs.FS, dir string, id uint32) (*Segment, error)       // O_CREATE|O_EXCL|O_RDWR|O_APPEND
func OpenAppend(fs vfs.FS, dir string, id uint32) (*Segment, error)   // O_RDWR|O_APPEND, size = current file size
func OpenRead(fs vfs.FS, dir string, id uint32) (*Segment, error)     // O_RDONLY

func (s *Segment) ID() uint32
func (s *Segment) Size() int64
// Append writes the bytes at the end and returns the offset where they start.
func (s *Segment) Append(b []byte) (offset int64, err error)
// ReadAt fills p from the given offset. io.ErrUnexpectedEOF if not enough bytes.
func (s *Segment) ReadAt(p []byte, off int64) error
func (s *Segment) Sync() error
func (s *Segment) Truncate(size int64) error
func (s *Segment) Close() error

// Scan reads records from r one by one. For each valid record it calls fn(offset, rec, sizeOnDisk).
// It returns the offset just AFTER the last good record ("good") and the error that stopped it:
//   nil                 -> reached a clean end of file
//   record.ErrTruncated -> file ends in the middle of a record (torn tail)
//   record.ErrCorrupt   -> bad CRC / bad header
//   error from fn       -> passed through
func Scan(r io.Reader, fn func(off int64, rec record.Record, size int) error) (good int64, err error)
```

**CRITICAL behaviour: partial-write rollback in `Append`.**
If `Write` fails after writing only *some* bytes, the file now has garbage in the middle. If we keep appending after it, recovery would stop at the garbage and **lose every later record**. So:

```text
Append(b):
  start := s.size
  n, err := s.f.Write(b)
  if err != nil:
      // try to undo the partial write
      if n > 0 { if terr := s.f.Truncate(start); terr != nil { return 0, &FatalError{...} } }
      return 0, err
  s.size += n
  return start, nil
```
`FatalError` (a distinct error type) tells the DB to go into **broken (fail-stop) mode**: refuse all further writes. Same rule for a failed `Sync()`: **do not retry** (after a failed fsync you cannot know what reached disk). Return a `FatalError`.

**Tests**
- Append 3 records; `ReadAt` each at the returned offsets and decode them.
- `Scan` over a file of N records returns N callbacks with correct offsets, `good == file size`, `err == nil`.
- `Scan` on a file with a half-written last record → `good` equals the end of the last full record, `err == ErrTruncated`.
- `Scan` on a file with a flipped byte in record 2 of 5 → `good` = end of record 1, `err == ErrCorrupt`.
- (Later in Phase 9) Append with an injected partial write leaves the file at its old size.

**Gate**
- [ ] All tests pass. Scan's contract is documented in comments.

---

### Phase 4: The `index` Package (in-memory index)

**Goal:** Map each key to the location of its newest PUT record.

**File:** `internal/index/index.go`

**Spec**
```go
package index

type Entry struct {
    FileID uint32 // which segment
    Offset int64  // where the record starts
    Size   uint32 // total record size on disk (header + key + value)
    Seq    uint64 // sequence number of the record
}

// Index is an interface so we can swap a hash map for an ordered structure in Stretch A.
type Index interface {
    Get(key string) (Entry, bool)
    Put(key string, e Entry) (old Entry, existed bool)
    Delete(key string) (old Entry, existed bool)
    Len() int
}

func NewHash() Index // backed by map[string]Entry
```

The index itself is **not** thread-safe. The DB protects it with its own lock.

**Why `string` keys?** Go maps cannot use `[]byte` as keys. `string(key)` makes a copy, which is exactly what we want (the caller may reuse its slice).

**Tests:** put/get/overwrite/delete/len; Put returns the previous entry.

**Gate**
- [ ] Tests pass.

---

### Phase 5: The `db` Package, Core Engine (Open / Put / Get / Delete / Close)

**Goal:** A working store with durability policies. Recovery is a *simple stub* here (full version in Phase 6), so you can first see writes and reads work.

**Files:** `internal/db/db.go`, `internal/db/options.go`, `internal/db/errors.go`

**Spec**
```go
package db

type SyncPolicy int
const (
    SyncAlways   SyncPolicy = iota // fsync after EVERY write. Safest, slowest.
    SyncInterval                   // background fsync every SyncEvery. May lose up to that window on power loss.
    SyncNever                      // leave it to the OS. Fastest. May lose recent data on power loss.
)

type Options struct {
    Dir             string
    FS              vfs.FS        // default: vfs.OS()
    SegmentMaxBytes int64         // default 64 MiB; rotate when the active segment would exceed this
    SyncPolicy      SyncPolicy    // default SyncAlways (safe by default)
    SyncEvery       time.Duration // default 100ms (used by SyncInterval)
    UseHintFiles    bool          // default true (Phase 8b)
}

var (
    ErrNotFound   = errors.New("db: key not found")
    ErrKeyEmpty   = errors.New("db: key must not be empty")
    ErrKeyTooLong = errors.New("db: key too large")
    ErrValueTooLong = errors.New("db: value too large")
    ErrClosed     = errors.New("db: closed")
    ErrBroken     = errors.New("db: store is in fail-stop mode after an I/O error; reopen it")
    ErrReadOnly   = errors.New("db: read-only")   // used by followers in Stretch C
)

type DB struct { ... }

func Open(opts Options) (*DB, error)
func (db *DB) Put(key, value []byte) error
func (db *DB) Get(key []byte) ([]byte, error)
func (db *DB) Delete(key []byte) error   // deleting a missing key is a no-op returning nil (writes nothing)
func (db *DB) Sync() error               // force fsync of the active segment now
func (db *DB) Close() error              // sync, stop background goroutines, close all files
func (db *DB) Stats() Stats
```

**Struct fields (suggested)**
```go
type DB struct {
    opts       Options
    mu         sync.RWMutex
    idx        index.Index
    segs       map[uint32]*segment.Segment // read handles for ALL segments (including active)
    active     *segment.Segment
    nextSeq    uint64
    nextFileID uint32
    liveBytes  int64   // sum of Entry.Size over all live index entries
    dirty      bool    // active segment has unsynced writes
    closed     bool
    broken     error   // set on fatal I/O error
    compactMu  sync.Mutex // allows only one compaction at a time
    stopSync   chan struct{}
    wg         sync.WaitGroup
}
```

**Put algorithm (hold `db.mu.Lock()` for all of it)**
1. If `closed` → `ErrClosed`; if `broken != nil` → `ErrBroken`.
2. Validate: key length 1..MaxKeySize, value length <= MaxValueSize.
3. `seq := db.nextSeq`; `buf := record.Encode(Record{seq, TypePut, key, value})`.
4. **Rotate** if `active.Size() > 0 && active.Size()+len(buf) > SegmentMaxBytes` (Phase 7; before then, skip).
5. `off, err := active.Append(buf)`. On `FatalError` set `db.broken`. On any error return it (index **unchanged**).
6. If `SyncAlways`: `active.Sync()`. On error set `db.broken` and return the error. *(The record is in the file but we do not know if it is durable; we refuse further writes.)*
   Else `db.dirty = true`.
7. Update index: `old, existed := idx.Put(string(key), Entry{active.ID(), off, len(buf), seq})`; adjust `liveBytes` (`+= len(buf)`, and `-= old.Size` if existed).
8. `db.nextSeq++`. Return nil.

**Get algorithm (`RLock`)**
1. `e, ok := idx.Get(string(key))`; if `!ok` → `ErrNotFound`.
2. `seg := db.segs[e.FileID]`; `buf := make([]byte, e.Size)`; `seg.ReadAt(buf, e.Offset)`.
3. `rec, err := record.DecodeBytes(buf)`; on error return it wrapped (this catches disk corruption on read).
4. Sanity: `rec.Type == TypePut` and `bytes.Equal(rec.Key, key)`; otherwise return a corruption error.
5. Return `rec.Value` (it points into our private `buf`, so it is safe to hand out).

**Delete algorithm (`Lock`)**
Same as Put but: if key is not in the index → return nil without writing. Otherwise append a `TypeDelete` record (value empty), sync per policy, then `idx.Delete(key)` and `liveBytes -= old.Size`.

**SyncInterval goroutine**
```go
// started in Open when policy == SyncInterval
go func() {
    t := time.NewTicker(opts.SyncEvery)
    defer t.Stop()
    for {
        select {
        case <-t.C:
            db.mu.Lock()
            if db.dirty && db.broken == nil {
                if err := db.active.Sync(); err != nil { db.broken = err } else { db.dirty = false }
            }
            db.mu.Unlock()
        case <-db.stopSync:
            return
        }
    }
}()
```
`Close()` closes `stopSync`, waits on `wg`, does a final sync, closes all segment files.

**Open (Phase 5 version)**: `MkdirAll`; if no segments exist create segment 1 as active; else just open existing ones (full recovery comes in Phase 6).

**Tests** (`db_test.go`, use `t.TempDir()`)
- Put/Get/overwrite/Delete/Get-after-delete returns `ErrNotFound`.
- Empty key rejected; empty value allowed and round-trips (and is *different* from deleted).
- 10,000 random operations compared against a plain `map[string]string` model.
- Concurrency: 8 goroutines doing Put/Get on different keys with `-race`.
- `Close` then `Put` → `ErrClosed`.
- With `SyncAlways`, count of `Sync()` calls equals count of writes (use a counting `vfs` wrapper; build it now, small).

**Gate**
- [ ] All tests pass with `-race`.
- [ ] Put/Get/Delete behave exactly as in the model test.

---

### Phase 6: Crash Recovery

**Goal:** `Open()` rebuilds the index from the log files, survives a torn tail, and refuses to hide real corruption.

**File:** `internal/db/recovery.go`

**Open algorithm**
1. `MkdirAll(dir)`.
2. `ReadDir`. **Delete every `*.tmp` file** (leftover from an interrupted compaction). Delete orphan `.hint` files that have no matching `.log`.
3. Collect segment IDs from `NNNNNN.log` names; sort ascending. If none: create segment 1 and finish.
4. Create the recovery working state:
   ```go
   tombs := map[string]uint64{} // key -> seq of the newest DELETE seen so far
   maxSeq := uint64(0)
   ```
5. For each segment ID in ascending order:
   - `isLast := id == maxID`.
   - Open it read-only and `segment.Scan(bufio.NewReaderSize(f, 1<<20), fn)`.
   - Inside `fn(off, rec, size)` apply the **newest-sequence-wins rule** (below).
   - After the scan:
     - `err == nil` → good.
     - `err` is `ErrTruncated` or `ErrCorrupt` **and `isLast`** → this is the *torn tail*. Open the file for append, `Truncate(good)`, `Sync()`, and log: `"recovery: truncated N bytes of damaged tail from 000003.log"`. Stop scanning this file.
     - `err` is `ErrTruncated` or `ErrCorrupt` **and NOT `isLast`** → this is **real corruption in an immutable segment**. Return an error naming the file and offset. Never silently drop data.
6. After all segments are read: `nextSeq = maxSeq + 1`, `nextFileID = maxID + 1`. Open read handles for every segment; open the highest-ID segment with `OpenAppend` as the active one (its size is `good` after any truncation).
7. Recompute `liveBytes` from the index.
8. Start the sync goroutine if needed.

**Newest-sequence-wins rule (the key idea)**
Compaction (Phase 8) can leave files in an order where an older record appears in a later file. So recovery must not trust file order. It trusts **Seq**:

```go
// for PUT record with seq s and key k:
cur, ok := idx.Get(k)
if (!ok || s > cur.Seq) && s > tombs[k] {          // tombs[k] is 0 if absent
    idx.Put(k, Entry{id, off, size, s})
    delete(tombs, k)
}
// for DELETE record with seq s and key k:
cur, ok := idx.Get(k)
if (!ok || s > cur.Seq) && s > tombs[k] {
    if ok { idx.Delete(k) }
    tombs[k] = s
}
maxSeq = max(maxSeq, s)
```
Discard `tombs` after recovery.

**Why a torn tail only happens at the end of the LAST file:** only the active segment is ever being written. Older segments were fully written and synced before rotation.

**Policy note (write in DESIGN.md):** in the last segment we stop at the *first* bad record. Valid records after a bad one are dropped. This is the standard WAL policy: a bad record means the writer died there. An optional `StrictRecovery` option may refuse to open instead.

**Tests** (`recovery_test.go`)
1. Write 1000 keys, `Close`, reopen → all present and correct.
2. Write 1000 keys, **do not call Close** (simulate crash: drop the `DB` and reopen on the same dir with `SyncAlways`) → all 1000 present.
3. **Torn tail:** write 1000 keys, close, truncate the last segment by `k` bytes for many random `k` (1..record size+10) → reopen succeeds; the recovered keys are exactly a **prefix** of the written order (no gaps, no garbage); file is truncated to a record boundary; a new `Put` after recovery works and survives another reopen.
4. **Bit flip in the middle of the LAST segment** → everything before the flip is recovered, the rest dropped, no panic.
5. **Bit flip in an OLDER segment** (force rotation first, or create two files by hand) → `Open` returns an error mentioning the file.
6. Deletes survive reopen (a deleted key stays deleted).
7. Overwrites: the newest value wins after reopen.
8. Leftover `000009.log.tmp` is removed on `Open`.
9. Sequence numbers continue correctly: `nextSeq` after reopen is greater than any existing Seq.

**Gate**
- [ ] All recovery tests pass, including tests 3 and 5 with at least 200 random truncation points.

---

### Phase 7: Segment Rotation

**Goal:** Keep files a manageable size and make compaction possible.

**Implement `rotateLocked()` (called with `db.mu` held):**
1. `active.Sync()` (the old segment must be fully durable before we leave it).
2. Create the new segment: `id := db.nextFileID; db.nextFileID++`; `segment.Create(fs, dir, id)`.
3. `fs.SyncDir(dir)` (so the new file's name is durable).
4. The old active stays in `db.segs` as an immutable read-only segment (its existing handle still works for `ReadAt`).
5. Set `db.active = newSeg`, `db.segs[id] = newSeg`, `db.dirty = false`.

Call it from Put/Delete when the record would overflow `SegmentMaxBytes`. A single record larger than `SegmentMaxBytes` is allowed: rotate (if the active is non-empty) and write it alone.

**Tests**
- Tiny `SegmentMaxBytes` (e.g. 4 KiB), write 2000 records → many `.log` files exist; all keys readable; reopen → all keys still correct.
- Rotation + reopen with an overwritten key whose old copy is in an older segment → newest value wins.
- Oversized record larger than the segment limit is stored and read back.

**Gate**
- [ ] Rotation tests pass. `Stats().Segments` reports the right count.

---

### Phase 8: Compaction (garbage collection)

**Goal:** Reclaim disk space used by overwritten and deleted data, **safely, even if we crash at any moment.**

**File:** `internal/db/compaction.go`  
**API:** `func (db *DB) Compact() error` (manual trigger) and `func (db *DB) NeedsCompaction(deadRatio float64) bool` (returns true if `1 - liveBytes/totalDiskBytes >= deadRatio`).

#### Design rules (why this is crash-safe)

1. The merged output is written to `NNNNNN.log.tmp`, fsynced, and only then **atomically renamed**. A crash before the rename leaves a `.tmp` file that `Open` deletes.
2. The merged segment gets an ID **lower than the new active segment**, so the active segment is always the highest ID (Phase 4.1 rule).
3. Records are copied **with their original Seq**. Recovery's newest-seq-wins rule then makes duplicate data (from a crash after the rename but before old files are deleted) harmless.
4. **Tombstone rule:** a DELETE record is *dropped* only when the key is currently live again (the index has the key), because then a newer PUT with a higher Seq exists and wins anyway. If the key is *not* in the index, **keep the tombstone**. This is always safe, because a crash that leaves old PUT records on disk must not bring a deleted key back to life. (Dropping tombstones for good needs a manifest; it is out of scope. Mention this trade-off in DESIGN.md.)

#### Algorithm

**Phase A: freeze the inputs (hold `db.mu.Lock()` briefly)**
```text
compactMu.Lock(); defer compactMu.Unlock()          // one compaction at a time
db.mu.Lock()
  mergeID := db.nextFileID; db.nextFileID++          // reserve a LOWER id than the next active
  rotateLocked()                                     // creates a new active with id mergeID+1
  inputs := all segment IDs < mergeID, sorted ascending
db.mu.Unlock()
if len(inputs) == 0 { return nil }
```
The inputs are now immutable. New writes go to the new active segment.

**Phase B: copy live records (no big lock held)**
```text
tmp := create "NNNNNN.log.tmp" for mergeID
w := bufio.Writer(tmp); newOff := 0; moves := []move{}
for each input id (ascending):
    Scan(input) → for each (off, rec, size):
        db.mu.RLock()
        e, ok := idx.Get(string(rec.Key))
        db.mu.RUnlock()
        var keep bool
        if rec.Type == TypePut:    keep = ok && e.FileID == id && e.Offset == off   // is THIS exact record the live one?
        else /* DELETE */:         keep = !ok                                       // tombstone rule
        if keep:
            b := record.Encode(rec)           // identical bytes, original Seq
            w.Write(b)
            if rec.Type == TypePut { moves = append(moves, move{key, id, off, newOff, len(b), rec.Seq}) }
            newOff += len(b)
w.Flush(); tmp.Sync(); tmp.Close()
if newOff == 0 { remove tmp; (skip rename) }
else { fs.Rename(tmp, final "NNNNNN.log"); fs.SyncDir(dir) }
(optional) write the hint file here (Phase 8b)
```

**Phase C: swap (hold `db.mu.Lock()`)**
```text
open merged segment read-only; db.segs[mergeID] = merged
for each move m:
    e, ok := idx.Get(m.key)
    // Compare-and-swap: only repoint if a newer write has NOT replaced it meanwhile.
    if ok && e.FileID == m.oldFile && e.Offset == m.oldOff {
        idx.Put(m.key, Entry{mergeID, m.newOff, m.size, m.seq})
    }
close and delete each input from db.segs      // while still holding the lock, so no reader uses a closed file
db.mu.Unlock()
for each input: fs.Remove(path); then fs.SyncDir(dir)
```

#### Phase 8b: Hint files (optional but great for the demo)

- After the merged log is renamed, write `NNNNNN.hint.tmp` with one hint entry per record in the merged log (format in Section 4.3), `Sync`, then rename to `NNNNNN.hint` and `SyncDir`. **Order: log first, hint second.** If we crash in between, the log exists without a hint, which is fine (recovery scans the log).
- In recovery, for a segment that has a `.hint` file and `UseHintFiles` is true: read hint entries instead of scanning the log, and apply the same newest-seq-wins rule using the hint's `Offset`, `RecordSize`, `Seq`, `Type`, `Key`. If **any** entry fails its CRC → ignore the hint and scan the log.
- Hint files are only created for compacted segments; the active and freshly rotated segments are always scanned.
- When a segment is deleted, delete its `.hint` too.

#### Tests (`compaction_test.go`)
1. Write 10,000 keys, overwrite each 5 times, delete half. Record `DiskBytes`. `Compact()`. All live keys still return the **latest** value; deleted keys are still `ErrNotFound`; `DiskBytes` dropped significantly (assert at least 50% smaller for this workload).
2. Reopen after compaction → identical contents.
3. **Concurrent writes during compaction** (`-race`): while `Compact()` runs, other goroutines keep doing Put/Delete/Get on overlapping keys; afterwards the DB matches a model that applied the same operations. (This is the test that catches the compare-and-swap bug.)
4. Compacting twice in a row is fine; compacting an almost-empty DB is a no-op.
5. A key deleted *before* compaction does not come back after compaction **+ reopen**.
6. Hint file: reopen with hints vs. with `UseHintFiles=false` yields identical contents; corrupt the hint file → still opens correctly (falls back to the log).
7. Crash windows are tested in Phase 9.

**Gate**
- [ ] All compaction tests pass with `-race`, repeated: `go test -race -count=20 -run Compaction ./internal/db`.

---

### Phase 9: Crash and Fault-Injection Testing (the proof)

**Goal:** Demonstrate, with evidence, that acknowledged writes survive and corruption is never silently accepted.

#### 9.1 Build a fault-injecting file system (`internal/vfs/faultfs.go`)

`FaultFS` wraps another `FS`. It supports:

- **Fail after N bytes written** (global counter): the write that crosses the limit writes only part of its bytes, then returns an error. All later operations also fail ("the process is dead").
- **Fail on the Nth call** to `Sync`, `Rename`, `Remove`, `SyncDir`, or `OpenFile`.
- **Power-loss simulation:** track, per file, how many bytes have been `Sync`ed. `PowerLoss(rng)` truncates every tracked file to `syncedBytes + rand(0..unsyncedBytes)`, simulating "only some of the unsynced data reached the disk." (A real disk may keep any prefix of unsynced data.)

#### 9.2 Test scenarios (`tests/crash/`)

| # | Test | What it proves |
|---|---|---|
| 1 | **Torn-write sweep:** for each N in 1..totalBytes (sample every k bytes), run a workload with `FailAfterBytes(N)`, then reopen on the real files | After a crash at *any* byte, `Open` succeeds and the recovered data is a valid prefix of the operation history |
| 2 | **Power-loss, SyncAlways:** run N random operations, call `PowerLoss`, reopen | Every acknowledged write is present (acknowledged = `Put` returned nil) |
| 3 | **Power-loss, SyncNever/Interval:** same, but only assert *prefix consistency* | Data may be lost but never reordered or corrupted. This shows the durability trade-off |
| 4 | **Failed fsync:** `Sync` fails once | DB enters broken mode, later writes return `ErrBroken`, reopen works |
| 5 | **Partial write rollback:** write fails after half a record | File is truncated back; next writes succeed; reopen shows no garbage in the middle |
| 6 | **Compaction crash points:** inject a failure at *each* step (tmp create, each write, tmp sync, rename, syncdir, hint rename, each input remove) | After each failure + reopen, **all** live data is present and deleted keys stay deleted |
| 7 | **Model-based random test:** random Put/Delete/Compact/Reopen/Crash sequence compared with an in-memory `map` model for ≥ 200 seeds | General confidence; print the seed on failure so it is reproducible |

#### 9.3 Real `kill -9` test (`cmd/kvcrash`)

Two modes in one binary:
- `kvcrash writer -dir D`: opens DB with `SyncAlways`, loops writing key `k%08d` with a value derived from the number, and prints `ACK <n>` to stdout **after** each `Put` returns.
- `kvcrash verify -dir D -acked N`: reopens the DB and checks keys `0..N` exist with correct values, and that no key has a wrong value.
- `kvcrash run -rounds 50`: parent loop: start writer as a child process, read its `ACK` lines, kill it with `SIGKILL` after a random 50-500 ms, call verify with the last ACKed number, repeat on the same directory for many rounds.

> **Honest note for the write-up:** `kill -9` kills the *process*, but the OS page cache survives. So it proves process-crash safety and recovery logic. The `FaultFS` power-loss tests (9.2 #2-3) are what simulate real *power* loss.

**Gate**
- [ ] All scenarios pass; scenario 7 passes for 200 seeds.
- [ ] `kvcrash run -rounds 50` reports zero lost acknowledged writes.
- [ ] You can show the log line `recovery: truncated N bytes of damaged tail` at least once in a demo.

---

### Phase 10: Benchmarking

**Goal:** Measured numbers that show the cost of durability and the benefit of compaction.

#### 10.1 Go micro-benchmarks (`internal/db/bench_test.go`)
`BenchmarkPut_SyncAlways`, `BenchmarkPut_SyncInterval`, `BenchmarkPut_SyncNever`, `BenchmarkGet_Random`, `BenchmarkOpen_Recovery_{1e5,1e6}`, each with `b.ReportAllocs()` and sub-benchmarks for value sizes 100 B, 1 KiB, 10 KiB.

#### 10.2 Load generator `cmd/kvbench`
Flags:
```text
-dir        data directory (use a real disk, not tmpfs; say which disk in the report)
-n          number of operations
-value-size bytes (default 256)
-sync       always | interval | never
-workload   fillseq | fillrandom | readrandom | mixed (e.g. 80% read / 20% write)
-threads    number of goroutines (default 1)
-csv        write results to a CSV file
```
Per run, print: ops/sec, MB/s, and latency **p50 / p95 / p99 / max** (record each op's duration in a `[]time.Duration`, sort at the end; for very large runs sample 1 in N).

#### 10.3 Experiments to run and record in `docs/BENCHMARKS.md`

| Experiment | What to vary | What to report |
|---|---|---|
| Durability cost | sync = always / interval / never | write ops/s and p99 latency |
| Value size | 100 B / 1 KiB / 10 KiB | throughput (MB/s) |
| Read speed | 1e6 keys, random reads | ops/s, p50/p99 |
| Recovery time | 1e5 / 1e6 / 5e6 keys, with and without hint files | `Open()` seconds |
| Compaction | workload with 80% overwrites | disk size before/after, compaction time, write amplification (`bytes written during compaction / live bytes`) |
| Concurrency | 1 / 4 / 16 threads | throughput (shows the single-lock limit) |

Tips: use `SyncAlways` with smaller `-n` (for example 5,000) because it is slow on real disks. Warm up before measuring. Report machine specs (CPU, disk type, OS, Go version). Make a table and (optionally) a chart from the CSV. (Optional baseline: compare against `bbolt` or SQLite in a separate module so the main project stays dependency-free.)

**Gate**
- [ ] `docs/BENCHMARKS.md` contains all six tables with real numbers and a short "what this tells us" paragraph under each.

---

### Phase 11: CLI (`cmd/kvcli`)

**Goal:** Something you can demo live.

A REPL reading lines from stdin:

```text
put <key> <value>
get <key>
del <key>
compact
stats          → keys, segments, disk bytes, live bytes, dead ratio, next seq
sync
help
quit
```
Flags: `-dir`, `-sync always|interval|never`, `-segment-mb`.
Print recovery messages on startup (for example: `opened: 99812 keys, 4 segments, recovered in 38ms, truncated 37 bytes of torn tail`). Expose these as fields in a `RecoveryInfo` struct on the DB (`db.RecoveryInfo()`).

**Gate**
- [ ] Manual smoke test: put, get, kill the process, restart, get again.

---

### Phase 12: Documentation and Demo

1. **`docs/DESIGN.md`:** architecture diagram, record format, write path ordering, recovery algorithm, compaction algorithm and its crash argument, tombstone trade-off, sync-policy trade-offs, known limitations (single lock, whole index in RAM, no transactions).
2. **`README.md`:** what it is, quick start, API, benchmark summary table, how to run all tests and the crash test.
3. **Demo script (5-8 minutes):**
   1. Run `kvbench -sync always` vs `-sync never`: show the throughput gap.
   2. Insert 100k keys; run `kvcrash run` and show "0 lost acknowledged writes".
   3. Manually truncate the last segment with `truncate -s -37`, reopen, show the recovery message.
   4. Show `stats`, run `compact`, show the disk size drop.
   5. Show recovery time with and without hint files.
   6. (Stretch) range scan, snapshot, follower catching up.

---

## 7. Stretch Goals (do these only after Phases 0-12 pass)

### Stretch A: Range Queries

**Goal:** `Scan(start, end []byte, fn func(key, value []byte) bool) error` returns keys in sorted order, `start <= key < end` (nil end = unbounded).

**Steps**
1. Add to the `Index` interface: `Ascend(start, end string, fn func(key string, e Entry) bool)`.
2. Implement `NewOrdered()`. Two options (pick one):
   - **Recommended (educational):** write your own **skip list** with `Get/Put/Delete/Ascend`. (~150 lines.)
   - **Faster to build:** use `github.com/google/btree` (the only external dependency; keep it behind this interface).
3. Add a build option `Options.OrderedIndex bool` (default false). Hash index stays the default.
4. `DB.Scan` takes `RLock`, walks the index, and for each entry calls `ReadAt` + decode. **Important:** collect the entries first, release the lock, then read values? No. Reading while holding `RLock` is simplest and safe because compaction's swap needs the write lock. Keep `RLock` for the whole scan, and document that long scans block writers.
5. Add `scan [start] [end]` to the CLI.

**Tests:** inserted-in-random-order keys come back sorted; bounds are inclusive/exclusive as specified; deleted keys do not appear; scan matches a sorted-map model for 1000 random ranges; skip list passes a randomized test against a model.

**Benchmark:** Get latency and memory use: hash index vs ordered index (shows the O(1) vs O(log n) trade-off).

---

### Stretch B: Snapshots

**Goal:** A frozen, consistent view of the data that is not affected by later writes or compaction.

**Design (in-memory, point-in-time reads)**
1. Requires the ordered index from Stretch A with **copy-on-write clone** (the `google/btree` `Clone()` method gives this cheaply; with your own skip list, a full copy is acceptable for a demo).
2. `func (db *DB) NewSnapshot() (*Snapshot, error)`: under `Lock`, clone the index and record `snapshotSeq = nextSeq-1`. Increment `db.snapshotCount`. Register the snapshot in `db.snapshots`.
3. `Snapshot.Get(key)`, `Snapshot.Scan(...)`: use the cloned index, and read values from segment files via the same `ReadAt` path.
4. `Snapshot.Close()`: decrement the count; if zero, delete files that compaction marked obsolete.
5. **Compaction change:** in Phase C, instead of deleting input segments immediately, if `snapshotCount > 0`, move them to `db.obsolete` (keep the file handles open) and delete them when the last snapshot closes.

**Optional on-disk checkpoint:** `db.Checkpoint(dstDir)`: rotate, then **hard-link** (`os.Link`) all immutable segments into `dstDir` (cheap, since immutable files never change). Opening `dstDir` with `db.Open` yields the state at that moment.

**Tests:** take snapshot, then overwrite/delete keys and run `Compact()`, and the snapshot still returns the old values; closing the snapshot frees obsolete files; checkpoint directory opens as an independent DB.

---

### Stretch C: Simple Replication (log shipping, leader → followers)

**Goal:** A read-only follower stays up to date by streaming the leader's records.

**Why this is easy here:** records are self-describing, checksummed, and carry a global Seq. We can ship the **raw record bytes** as-is.

**Protocol (TCP, binary)**
```text
Follower → Leader:  HELLO  magic(4 "KVR1") | lastAppliedSeq(8)
Leader → Follower:  a stream of raw records (exactly the on-disk format), in increasing Seq order
                    then a live tail: new records as they are written
Follower → Leader (optional, sync mode):  ACK  seq(8)
```
The CRC in each record doubles as the message checksum.

**Leader steps**
1. Track `minSeq/maxSeq` per segment in memory (filled during recovery and appends) so catch-up can skip segments whose `maxSeq <= lastAppliedSeq`.
2. **Catch-up:** under `RLock`, list the segments to read; scan them in order and send records with `Seq > lastAppliedSeq`. (Compacted segments have gaps; that is fine because the follower applies newest-seq-wins.)
3. **Live tail:** add `db.Subscribe() (<-chan []byte, cancel func())`. Put/Delete send the encoded bytes to all subscribers *after* the local append. Use a buffered channel; if a follower is too slow and the buffer fills, drop it (it will reconnect and catch up).
4. Avoid gaps between catch-up and live tail: subscribe **first**, buffer, then catch up, then drain the buffer, skipping records already sent (compare Seq).
5. Optional **semi-sync mode**: `Options.WaitForFollowers = 1`. `Put` returns only after one follower ACKs (or a timeout). Demonstrates the latency-versus-durability trade-off.

**Follower steps**
1. Open its own DB with `ReadOnly` set (public `Put/Delete` return `ErrReadOnly`).
2. Add the internal method `ApplyReplicated(rec record.Record) error`: appends the **same bytes with the leader's Seq** (do not assign a new Seq), updates the index with the newest-seq-wins rule, and sets `nextSeq = max(nextSeq, rec.Seq+1)`.
3. Connect, send `HELLO` with `db.LastSeq()`, read records with `record.Decode`, apply each, reconnect with backoff on error.

**Binary:** `cmd/kvserver -role leader -listen :7000 -dir d1` and `-role follower -leader :7000 -dir d2`.

**Tests:** write 10k keys on the leader, start a follower, and eventually the follower's data equals the leader's; kill the follower mid-stream, restart, and it resumes from `LastSeq()` with no duplicates or gaps; leader compacts while a follower is catching up → follower still converges; follower rejects client writes.

**Write-up must say:** async replication can lose acknowledged writes if the leader dies before shipping; there is no automatic failover; followers are eventually consistent.

---

## 8. Invariants (never break these)

1. **Log before index.** The index never points to data that is not in the log file.
2. **Immutable after rotation.** Only the active (highest-ID) segment is ever written.
3. **Seq is strictly increasing** across all writes and never reused after being acknowledged.
4. **Newest Seq wins** when the same key appears more than once, regardless of which file it is in.
5. **A CRC mismatch is never ignored.** In the last segment it means truncate; in any other segment it means return an error.
6. **After any `Sync` or partial-write error, stop writing** (fail-stop). Never retry a failed fsync.
7. **Directory fsync after create / rename / delete** of segment files.
8. **Compaction output is complete and fsynced before it is renamed** into place, and is never visible under its final name while incomplete.
9. **A tombstone is dropped only if the key is live again.**
10. **Readers never touch a closed or deleted file** (segment removal happens under the write lock).

---

## 9. Common Pitfalls Checklist

- [ ] Using `bufio.Writer` and forgetting to `Flush` before `Sync`.
- [ ] Allocating based on a length read from a possibly corrupt header (always bound-check first).
- [ ] Updating the index before the append succeeded.
- [ ] Treating a torn tail as fatal, or treating mid-file corruption as a torn tail.
- [ ] Forgetting `SyncDir` after `Rename`, `Create`, or `Remove`.
- [ ] Using the file offset from `Seek` instead of tracking size yourself (`O_APPEND` moves it).
- [ ] Reusing the caller's `[]byte` as an index key (copy via `string(key)`).
- [ ] Returning a slice that points into a shared buffer.
- [ ] Compaction repointing the index without the compare-and-swap check.
- [ ] Holding `db.mu` while doing slow compaction I/O.
- [ ] Ordering segments by file ID only; always apply newest-Seq-wins.
- [ ] Not handling "empty value" vs "deleted key" (that is why `Type` exists).
- [ ] Running benchmarks on `tmpfs`/RAM disk and claiming durability numbers.

---

## 10. Final Definition of Done

- [ ] Phases 0-12 are complete and every Gate is ticked.
- [ ] `gofmt`, `go vet`, and `go test -race ./...` pass; compaction and crash tests pass repeatedly (`-count=20`).
- [ ] `kvcrash run -rounds 50` shows zero lost acknowledged writes.
- [ ] `docs/BENCHMARKS.md` has real numbers for all six experiments.
- [ ] `docs/DESIGN.md` explains formats, ordering rules, recovery, compaction, and limitations.
- [ ] README has a quick start and a demo script.
- [ ] At least one stretch goal is complete (recommended order: A → B → C).

### Requirement coverage map

| Original requirement | Evidence |
|---|---|
| Storage engine | Segments + append-only log (Phases 3, 5, 7) |
| Durability | Sync policies, fsync/dir-fsync rules, power-loss tests (Phases 5, 9) |
| Indexing | In-memory index (Phase 4), ordered index (Stretch A) |
| File I/O | `vfs` abstraction, positioned reads, append, rename, truncate (Phases 2, 3) |
| Crash recovery | Scan + CRC + torn-tail truncation + newest-seq-wins (Phase 6) |
| Compaction | Crash-safe merge with CAS swap and hint files (Phase 8) |
| Benchmarking | `testing.B` + `kvbench` + report (Phase 10) |
| Stretch goals | Range queries, snapshots, replication (Stretch A-C) |

---

## 11. Suggested Order of Work and Time

| Phase | Effort |
|---|---|
| 0, 1, 2 | ~1 day |
| 3, 4 | ~1 day |
| 5 | ~1.5 days |
| 6 | ~1.5 days |
| 7 | ~0.5 day |
| 8 (+8b) | ~2 days |
| 9 | ~2 days |
| 10, 11 | ~1.5 days |
| 12 | ~1 day |
| Stretch A / B / C | ~1.5 / 1.5 / 2.5 days |
