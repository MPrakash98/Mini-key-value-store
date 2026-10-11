package record

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"math/rand"
	"strings"
	"testing"
)

// ---- helpers ----------------------------------------------------------------

func mustEncode(t *testing.T, r Record) []byte {
	t.Helper()
	b := Encode(r)
	if len(b) != r.EncodedSize() {
		t.Fatalf("EncodedSize mismatch: got %d want %d", len(b), r.EncodedSize())
	}
	return b
}

func mustDecode(t *testing.T, b []byte) Record {
	t.Helper()
	rec, err := DecodeBytes(b)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	return rec
}

// recomputeCRC recalculates and writes the CRC32 in the first 4 bytes of a
// modified record buffer so we can test non-CRC corruption paths (e.g., an
// invalid field value that would otherwise be caught by the CRC check first).
func recomputeCRC(b []byte) {
	if len(b) < 4 {
		return
	}
	csum := crc32.ChecksumIEEE(b[4:])
	binary.BigEndian.PutUint32(b[0:4], csum)
}

// ---- round-trip tests -------------------------------------------------------

func TestRoundTrip_PUT(t *testing.T) {
	r := Record{Seq: 42, Type: TypePut, Key: []byte("hello"), Value: []byte("world")}
	b := mustEncode(t, r)
	got := mustDecode(t, b)
	if got.Seq != r.Seq || got.Type != r.Type || !bytes.Equal(got.Key, r.Key) || !bytes.Equal(got.Value, r.Value) {
		t.Fatalf("round-trip mismatch: %+v vs %+v", got, r)
	}
}

func TestRoundTrip_DELETE(t *testing.T) {
	r := Record{Seq: 7, Type: TypeDelete, Key: []byte("gone"), Value: nil}
	b := mustEncode(t, r)
	got := mustDecode(t, b)
	if got.Type != TypeDelete || !bytes.Equal(got.Key, r.Key) || len(got.Value) != 0 {
		t.Fatalf("round-trip mismatch: %+v vs %+v", got, r)
	}
}

func TestRoundTrip_EmptyValue(t *testing.T) {
	// An empty value (PUT with 0-byte value) is allowed and distinct from DELETE.
	r := Record{Seq: 1, Type: TypePut, Key: []byte("k"), Value: []byte{}}
	b := mustEncode(t, r)
	got := mustDecode(t, b)
	if got.Type != TypePut || !bytes.Equal(got.Key, r.Key) {
		t.Fatalf("round-trip mismatch: %+v vs %+v", got, r)
	}
}

func TestRoundTrip_OneByteKey(t *testing.T) {
	r := Record{Seq: 0, Type: TypePut, Key: []byte("x"), Value: []byte("val")}
	b := mustEncode(t, r)
	got := mustDecode(t, b)
	if !bytes.Equal(got.Key, r.Key) || !bytes.Equal(got.Value, r.Value) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestRoundTrip_LargeValue(t *testing.T) {
	// 1 MiB value – checks we can encode/decode without length overflows.
	val := make([]byte, 1<<20)
	rand.Read(val) //nolint:gosec – test randomness, not security
	r := Record{Seq: 999, Type: TypePut, Key: []byte("bigval"), Value: val}
	b := mustEncode(t, r)
	got := mustDecode(t, b)
	if !bytes.Equal(got.Value, val) {
		t.Fatal("large value round-trip failed")
	}
}

// ---- truncation tests -------------------------------------------------------

func TestTruncation_EveryOffset(t *testing.T) {
	r := Record{Seq: 55, Type: TypePut, Key: []byte("tkey"), Value: []byte("tval")}
	full := mustEncode(t, r)

	// Offset 0 → exactly io.EOF (zero bytes were available).
	_, _, err := Decode(bytes.NewReader(full[:0]))
	if err != io.EOF {
		t.Fatalf("offset 0: expected io.EOF, got %v", err)
	}

	// Offsets 1..len-1 → ErrTruncated (some bytes read but stream ended early).
	for cut := 1; cut < len(full); cut++ {
		_, _, err := Decode(bytes.NewReader(full[:cut]))
		if err != ErrTruncated {
			t.Fatalf("cut=%d: expected ErrTruncated, got %v", cut, err)
		}
	}
}

// ---- bit-flip tests ---------------------------------------------------------

func TestBitFlip_EveryByte(t *testing.T) {
	r := Record{Seq: 123, Type: TypePut, Key: []byte("flipkey"), Value: []byte("flipval")}
	orig := mustEncode(t, r)

	for i := range orig {
		flipped := make([]byte, len(orig))
		copy(flipped, orig)
		flipped[i] ^= 0xFF // flip all bits in byte i

		rec, err := DecodeBytes(flipped)
		if err == nil {
			// In the extremely unlikely event a flip produces a valid CRC, the
			// decoded content must still exactly match the original record.
			if !bytes.Equal(rec.Key, r.Key) || !bytes.Equal(rec.Value, r.Value) {
				t.Fatalf("byte %d: decoded a different record without error", i)
			}
		}
		// ErrCorrupt or ErrTruncated are both acceptable; no panic is the real goal.
	}
}

// ---- invalid header tests ---------------------------------------------------

func TestInvalidHeader_ZeroKeyLen(t *testing.T) {
	r := Record{Seq: 1, Type: TypePut, Key: []byte("k"), Value: []byte("v")}
	b := mustEncode(t, r)
	// Zero out KeyLen field (bytes 13-16), then recompute CRC so this
	// corruption reaches our KeyLen==0 guard rather than the CRC check.
	binary.BigEndian.PutUint32(b[13:17], 0)
	recomputeCRC(b)
	_, err := DecodeBytes(b)
	if err != ErrCorrupt {
		t.Fatalf("expected ErrCorrupt for keyLen=0, got %v", err)
	}
}

func TestInvalidHeader_KeyLenTooLarge(t *testing.T) {
	r := Record{Seq: 1, Type: TypePut, Key: []byte("k"), Value: []byte("v")}
	b := mustEncode(t, r)
	// Set KeyLen = MaxKeySize + 1, recompute CRC.
	binary.BigEndian.PutUint32(b[13:17], uint32(MaxKeySize+1))
	recomputeCRC(b)
	_, err := DecodeBytes(b)
	if err != ErrCorrupt {
		t.Fatalf("expected ErrCorrupt for keyLen>max, got %v", err)
	}
}

func TestInvalidHeader_InvalidType(t *testing.T) {
	r := Record{Seq: 1, Type: TypePut, Key: []byte("k"), Value: []byte("v")}
	b := mustEncode(t, r)
	// Set Type = 7 (invalid), recompute CRC.
	b[12] = 7
	recomputeCRC(b)
	_, err := DecodeBytes(b)
	if err != ErrCorrupt {
		t.Fatalf("expected ErrCorrupt for type=7, got %v", err)
	}
}

// ---- back-to-back decode test -----------------------------------------------

func TestSequentialDecode(t *testing.T) {
	records := []Record{
		{Seq: 1, Type: TypePut, Key: []byte("a"), Value: []byte("1")},
		{Seq: 2, Type: TypeDelete, Key: []byte("b"), Value: nil},
		{Seq: 3, Type: TypePut, Key: []byte("ccc"), Value: []byte("333")},
	}

	var buf bytes.Buffer
	totalBytes := 0
	for _, r := range records {
		b := Encode(r)
		buf.Write(b)
		totalBytes += len(b)
	}

	rd := bytes.NewReader(buf.Bytes())
	consumed := 0
	for i, want := range records {
		got, n, err := Decode(rd)
		if err != nil {
			t.Fatalf("record %d: unexpected error: %v", i, err)
		}
		consumed += n
		if got.Seq != want.Seq || got.Type != want.Type ||
			!bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) {
			t.Fatalf("record %d: got %+v want %+v", i, got, want)
		}
	}
	if consumed != totalBytes {
		t.Fatalf("consumed %d bytes, expected %d", consumed, totalBytes)
	}
	// Next read should return EOF.
	_, _, err := Decode(rd)
	if err != io.EOF {
		t.Fatalf("expected EOF after all records, got %v", err)
	}
}

// ---- fuzz test --------------------------------------------------------------

func FuzzDecode(f *testing.F) {
	// Seed with valid records so the fuzzer starts from known-good inputs.
	seeds := []Record{
		{Seq: 1, Type: TypePut, Key: []byte("fuzzkey"), Value: []byte("fuzzval")},
		{Seq: 2, Type: TypeDelete, Key: []byte("del"), Value: nil},
		{Seq: 0, Type: TypePut, Key: []byte("x"), Value: []byte{}},
	}
	for _, r := range seeds {
		f.Add(Encode(r))
	}
	f.Add([]byte{})
	f.Add([]byte("garbage"))
	f.Add([]byte(strings.Repeat("A", 100)))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must never panic regardless of input.
		_, _, _ = Decode(bytes.NewReader(data))
	})
}
