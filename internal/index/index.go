package index

// Entry holds the disk location of a key's most recent PUT record.
type Entry struct {
	FileID uint32 // which segment file holds this record
	Offset int64  // byte offset within that segment where the record starts
	Size   uint32 // total bytes on disk (header + key + value)
	Seq    uint64 // sequence number: higher = newer
}

// Index maps string keys to their latest disk Entry.
// The interface lets us swap a hash map for an ordered structure in Stretch A
// (range queries) without touching the DB engine.
//
// The Index is NOT thread-safe. The DB must protect it with its own lock.
type Index interface {
	// Get returns the Entry for key, or (zero, false) if not present.
	Get(key string) (Entry, bool)
	// Put inserts or updates key and returns the previous entry (if any).
	Put(key string, e Entry) (old Entry, existed bool)
	// Delete removes key and returns the removed entry (if any).
	Delete(key string) (old Entry, existed bool)
	// Len returns the number of live keys in the index.
	Len() int
	// ForEach calls fn for every entry in the index. Iteration stops if fn returns false.
	ForEach(fn func(key string, e Entry) bool)
}

// hashIndex is an Index backed by a plain Go map.
type hashIndex struct {
	m map[string]Entry
}

// NewHash returns a new hash-map-backed Index.
func NewHash() Index {
	return &hashIndex{m: make(map[string]Entry)}
}

func (h *hashIndex) Get(key string) (Entry, bool) {
	e, ok := h.m[key]
	return e, ok
}

func (h *hashIndex) Put(key string, e Entry) (Entry, bool) {
	old, existed := h.m[key]
	h.m[key] = e
	return old, existed
}

func (h *hashIndex) Delete(key string) (Entry, bool) {
	old, existed := h.m[key]
	if existed {
		delete(h.m, key)
	}
	return old, existed
}

func (h *hashIndex) Len() int { return len(h.m) }

func (h *hashIndex) ForEach(fn func(key string, e Entry) bool) {
	for k, e := range h.m {
		if !fn(k, e) {
			return
		}
	}
}
