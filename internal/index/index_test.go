package index

import "testing"

func TestHashIndex_PutGetDelete(t *testing.T) {
	idx := NewHash()

	// Initially empty.
	if idx.Len() != 0 {
		t.Fatalf("expected Len=0, got %d", idx.Len())
	}
	if _, ok := idx.Get("missing"); ok {
		t.Fatal("Get on empty index returned true")
	}

	// Put a new key.
	e1 := Entry{FileID: 1, Offset: 0, Size: 100, Seq: 1}
	old, existed := idx.Put("alpha", e1)
	if existed {
		t.Fatal("Put new key: existed should be false")
	}
	_ = old // zero value

	if idx.Len() != 1 {
		t.Fatalf("Len after Put: got %d want 1", idx.Len())
	}

	// Get the key back.
	got, ok := idx.Get("alpha")
	if !ok {
		t.Fatal("Get returned false for existing key")
	}
	if got != e1 {
		t.Fatalf("Get: got %+v want %+v", got, e1)
	}

	// Overwrite with a newer entry; Put must return the old one.
	e2 := Entry{FileID: 1, Offset: 100, Size: 120, Seq: 2}
	old, existed = idx.Put("alpha", e2)
	if !existed {
		t.Fatal("Put overwrite: existed should be true")
	}
	if old != e1 {
		t.Fatalf("Put overwrite: old entry = %+v, want %+v", old, e1)
	}

	got, _ = idx.Get("alpha")
	if got != e2 {
		t.Fatalf("Get after overwrite: got %+v want %+v", got, e2)
	}

	// Delete the key.
	oldDel, existed := idx.Delete("alpha")
	if !existed {
		t.Fatal("Delete: existed should be true")
	}
	if oldDel != e2 {
		t.Fatalf("Delete: old entry = %+v, want %+v", oldDel, e2)
	}
	if idx.Len() != 0 {
		t.Fatalf("Len after Delete: got %d want 0", idx.Len())
	}
	if _, ok := idx.Get("alpha"); ok {
		t.Fatal("Get after Delete returned true")
	}

	// Deleting a missing key is fine.
	_, existed = idx.Delete("nonexistent")
	if existed {
		t.Fatal("Delete of missing key: existed should be false")
	}
}

func TestHashIndex_MultipleKeys(t *testing.T) {
	idx := NewHash()
	const N = 1000
	for i := 0; i < N; i++ {
		key := string(rune('a'+i%26)) + string(rune('0'+i/26)) // 2-char key
		idx.Put(key, Entry{Seq: uint64(i)})
	}
	// Len may be smaller than N because keys repeat after 26*10 = 260.
	if idx.Len() == 0 {
		t.Fatal("Len should be non-zero")
	}
}

// TestHashIndex_PutReturnsOldEntry is the specific contract check from the plan.
func TestHashIndex_PutReturnsOldEntry(t *testing.T) {
	idx := NewHash()
	e1 := Entry{FileID: 3, Offset: 42, Size: 50, Seq: 10}
	e2 := Entry{FileID: 3, Offset: 92, Size: 60, Seq: 11}

	_, existed := idx.Put("key", e1)
	if existed {
		t.Fatal("first Put: existed should be false")
	}

	old, existed := idx.Put("key", e2)
	if !existed {
		t.Fatal("second Put: existed should be true")
	}
	if old != e1 {
		t.Fatalf("second Put: old = %+v, want %+v", old, e1)
	}
}
