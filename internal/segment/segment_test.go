package segment

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"kvstore/internal/record"
	"kvstore/internal/vfs"
)

func setup(t *testing.T) (vfs.FS, string) {
	t.Helper()
	dir := t.TempDir()
	fs := vfs.OS()
	return fs, dir
}

// encodeRecord is a test helper that encodes a PUT record.
func encodeRecord(t *testing.T, seq uint64, key, val string) []byte {
	t.Helper()
	return record.Encode(record.Record{
		Seq:   seq,
		Type:  record.TypePut,
		Key:   []byte(key),
		Value: []byte(val),
	})
}

// TestAppendAndReadAt appends 3 records and reads each one back at the
// returned offsets. Verifies that the decoded content is identical.
func TestAppendAndReadAt(t *testing.T) {
	fs, dir := setup(t)

	seg, err := Create(fs, dir, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer seg.Close()

	recs := []record.Record{
		{Seq: 1, Type: record.TypePut, Key: []byte("alpha"), Value: []byte("A")},
		{Seq: 2, Type: record.TypePut, Key: []byte("beta"), Value: []byte("BB")},
		{Seq: 3, Type: record.TypeDelete, Key: []byte("gamma")},
	}

	offsets := make([]int64, len(recs))
	sizes := make([]int, len(recs))
	for i, r := range recs {
		b := record.Encode(r)
		off, err := seg.Append(b)
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		offsets[i] = off
		sizes[i] = len(b)
	}

	// Read back each record and decode it.
	for i, r := range recs {
		buf := make([]byte, sizes[i])
		if err := seg.ReadAt(buf, offsets[i]); err != nil {
			t.Fatalf("ReadAt %d: %v", i, err)
		}
		got, err := record.DecodeBytes(buf)
		if err != nil {
			t.Fatalf("Decode %d: %v", i, err)
		}
		if got.Seq != r.Seq || got.Type != r.Type || !bytes.Equal(got.Key, r.Key) {
			t.Fatalf("record %d mismatch: got %+v want %+v", i, got, r)
		}
	}
}

// TestScan_CleanFile verifies that Scan over a well-formed file calls fn N
// times with correct offsets, and returns good == file size and err == nil.
func TestScan_CleanFile(t *testing.T) {
	fs, dir := setup(t)
	seg, _ := Create(fs, dir, 1)
	defer seg.Close()

	const N = 5
	recs := make([]record.Record, N)
	wantOffsets := make([]int64, N)
	var off int64
	for i := range recs {
		recs[i] = record.Record{Seq: uint64(i + 1), Type: record.TypePut, Key: []byte("k"), Value: []byte("v")}
		b := record.Encode(recs[i])
		wantOffsets[i] = off
		seg.Append(b)
		off += int64(len(b))
	}

	// Open for reading and scan.
	f, err := os.Open(filepath.Join(dir, FileName(1)))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	var gotCount int
	good, scanErr := Scan(f, func(callOff int64, rec record.Record, size int) error {
		if callOff != wantOffsets[gotCount] {
			t.Errorf("record %d: offset got %d want %d", gotCount, callOff, wantOffsets[gotCount])
		}
		gotCount++
		return nil
	})
	if scanErr != nil {
		t.Fatalf("Scan: unexpected error: %v", scanErr)
	}
	if gotCount != N {
		t.Fatalf("Scan: called fn %d times, expected %d", gotCount, N)
	}
	if good != seg.Size() {
		t.Fatalf("Scan: good=%d, expected file size=%d", good, seg.Size())
	}
}

// TestScan_TornTail appends a full record then a half-written one.
// Scan must return good == end of the full record and ErrTruncated.
func TestScan_TornTail(t *testing.T) {
	fs, dir := setup(t)
	seg, _ := Create(fs, dir, 1)

	full := record.Encode(record.Record{Seq: 1, Type: record.TypePut, Key: []byte("full"), Value: []byte("ok")})
	half := record.Encode(record.Record{Seq: 2, Type: record.TypePut, Key: []byte("half"), Value: []byte("truncated")})
	half = half[:len(half)/2] // cut it in half

	seg.Append(full)
	seg.Append(half) // raw write: bypass record encoding checks
	seg.Close()

	f, _ := os.Open(filepath.Join(dir, FileName(1)))
	defer f.Close()

	good, err := Scan(f, func(_ int64, _ record.Record, _ int) error { return nil })
	if err != record.ErrTruncated {
		t.Fatalf("expected ErrTruncated, got %v", err)
	}
	if good != int64(len(full)) {
		t.Fatalf("good=%d, expected %d", good, len(full))
	}
}

// TestScan_CorruptMiddle flips a byte in record 2 of 5 and verifies that
// Scan returns good == end of record 1 and ErrCorrupt.
func TestScan_CorruptMiddle(t *testing.T) {
	fs, dir := setup(t)
	seg, _ := Create(fs, dir, 1)

	var encoded [][]byte
	for i := 0; i < 5; i++ {
		b := record.Encode(record.Record{Seq: uint64(i), Type: record.TypePut, Key: []byte("key"), Value: []byte("val")})
		encoded = append(encoded, b)
		seg.Append(b)
	}
	seg.Close()

	// Flip a byte in the value area of record 2 (index 1).
	path := filepath.Join(dir, FileName(1))
	raw, _ := os.ReadFile(path)
	// Offset of record 2 = len(encoded[0]); flip last byte of its value.
	flipAt := len(encoded[0]) + len(encoded[1]) - 1
	raw[flipAt] ^= 0xFF
	os.WriteFile(path, raw, 0o644)

	f, _ := os.Open(path)
	defer f.Close()

	good, err := Scan(f, func(_ int64, _ record.Record, _ int) error { return nil })
	if err != record.ErrCorrupt {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
	if good != int64(len(encoded[0])) {
		t.Fatalf("good=%d, expected %d", good, len(encoded[0]))
	}
}

// TestOpenAppend verifies that OpenAppend picks up the current file size and
// new records can be appended after reopening.
func TestOpenAppend(t *testing.T) {
	fs, dir := setup(t)

	seg, _ := Create(fs, dir, 1)
	b := encodeRecord(t, 1, "persistent", "value")
	seg.Append(b)
	seg.Sync()
	seg.Close()

	seg2, err := OpenAppend(fs, dir, 1)
	if err != nil {
		t.Fatalf("OpenAppend: %v", err)
	}
	if seg2.Size() != int64(len(b)) {
		t.Fatalf("Size after reopen: got %d want %d", seg2.Size(), len(b))
	}
	// Append a second record.
	b2 := encodeRecord(t, 2, "more", "data")
	off, err := seg2.Append(b2)
	if err != nil {
		t.Fatalf("Append after reopen: %v", err)
	}
	if off != int64(len(b)) {
		t.Fatalf("Append offset: got %d want %d", off, len(b))
	}
	seg2.Close()
}

// TestFileName verifies the 6-digit zero-padded format.
func TestFileName(t *testing.T) {
	cases := []struct{ id uint32; want string }{
		{1, "000001.log"},
		{42, "000042.log"},
		{999999, "999999.log"},
	}
	for _, c := range cases {
		got := FileName(c.id)
		if got != c.want {
			t.Errorf("FileName(%d) = %q, want %q", c.id, got, c.want)
		}
	}
}

// TestSync verifies that calling Sync on a writable segment does not return
// an error under normal conditions.
func TestSync(t *testing.T) {
	fs, dir := setup(t)
	seg, _ := Create(fs, dir, 1)
	defer seg.Close()

	seg.Append(encodeRecord(t, 1, "k", "v"))
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

// TestTruncate verifies that Truncate reduces the segment size.
func TestTruncate(t *testing.T) {
	fs, dir := setup(t)
	seg, _ := Create(fs, dir, 1)
	defer seg.Close()

	b := encodeRecord(t, 1, "trunckey", "truncval")
	seg.Append(b)
	if seg.Size() == 0 {
		t.Fatal("size should be non-zero after append")
	}
	// Truncate to 0.
	if err := seg.Truncate(0); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if seg.Size() != 0 {
		t.Fatalf("Size after truncate: got %d want 0", seg.Size())
	}
}
