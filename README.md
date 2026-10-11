# kvstore — Mini Persistent Key-Value Store

A small, crash-safe, persistent key-value store written in Go (Bitcask-style):
append-only log segments + in-memory hash index + compaction.

---

## Quick Start

```bash
git clone <repo>
cd kvstore
go build ./...
go test -race ./...

# Interactive REPL
go run ./cmd/kvcli -dir data -sync always

# Load generator
go run ./cmd/kvbench -dir bench-data -n 50000 -sync never -workload fillrandom
```

---

## Features

| Feature | Status |
|---|---|
| Append-only log segments | ✅ |
| Write-ahead log + durability (fsync policies) | ✅ |
| In-memory hash index | ✅ |
| Crash recovery (torn-write detection + truncation) | ✅ |
| Segment rotation | ✅ |
| Compaction (crash-safe GC) | ✅ |
| Hint files (fast recovery) | ✅ |
| CLI REPL | ✅ |
| Benchmarks | ✅ |

---

## API

```go
store, err := db.Open(db.Options{
    Dir:        "data",
    SyncPolicy: db.SyncAlways,   // or SyncInterval, SyncNever
})

store.Put([]byte("key"), []byte("value"))
val, err := store.Get([]byte("key"))
store.Delete([]byte("key"))
store.Compact()          // garbage-collect dead records
store.Stats()            // keys, segments, disk/live bytes, next seq
store.Sync()             // force fsync now
store.Close()
```

---

## Sync Policies

| Policy | Durability | Speed |
|---|---|---|
| `SyncAlways` | Every acked write is durable | Slowest |
| `SyncInterval` | May lose last 100ms of writes | Fast |
| `SyncNever` | OS-driven; can lose recent writes | Fastest |

---

## On-disk Format

```
data/
├── 000001.log   ← immutable segment (old)
├── 000002.log   ← immutable segment (old)
├── 000003.log   ← ACTIVE segment (highest ID = writable)
├── 000002.hint  ← fast-recovery index for segment 2 (optional)
└── LOCK         ← process lock (future)
```

Each record:

```
CRC32(4) | Seq(8) | Type(1) | KeyLen(4) | ValLen(4) | Key | Value
```

---

## Running Tests

```bash
go test -race ./...
go test -bench=. -benchmem ./internal/db
go test -fuzz=FuzzDecode -fuzztime=30s ./internal/record
```

---

## Non-goals

- No transactions across multiple keys
- No compression or encryption
- No leader election or consensus
- Windows `SyncDir` is a best-effort no-op (data still safe via OS page cache)

---

## Architecture

```
            Put / Get / Delete
                    │
         ┌──────────▼──────────┐
         │   DB  (RWMutex)     │
         └───┬─────────────┬───┘
in-memory    │             │ append (write path)
index        │             ▼
key→{file,   │     ┌──────────────┐
 off,size,   │     │ Active seg   │  000003.log ← only writable file
 seq}        │     └──────────────┘
             │     ┌──────────────┐
read path ───┼────►│ Old segments │  000001.log, 000002.log (immutable)
(ReadAt)     │     └──────────────┘
             │
    Compaction: merges old → one new segment (+ optional .hint file)
```
