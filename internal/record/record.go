package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	// HeaderSize is the fixed size of every record header in bytes.
	// Layout: CRC32(4) | Seq(8) | Type(1) | KeyLen(4) | ValLen(4) = 21 bytes.
	HeaderSize = 21

	// TypePut indicates a key-value insertion or update.
	TypePut byte = 0

	// TypeDelete indicates a tombstone (key deletion).
	TypeDelete byte = 1

	// MaxKeySize is the largest allowed key in bytes (1 MiB).
	// We check this BEFORE allocating to avoid memory-bomb attacks from corrupt headers.
	MaxKeySize = 1 << 20 // 1 MiB

	// MaxValueSize is the largest allowed value in bytes (64 MiB).
	MaxValueSize = 64 << 20 // 64 MiB
)

var (
	// ErrTruncated is returned when the stream ends mid-record (torn write).
	ErrTruncated = errors.New("record: truncated")
	// ErrCorrupt is returned when a record is fully present but has a bad CRC
	// or contains an impossible header value.
	ErrCorrupt = errors.New("record: corrupt")
)

// Record holds the decoded contents of one log entry.
type Record struct {
	Seq   uint64
	Type  byte
	Key   []byte
	Value []byte
}

// EncodedSize returns the total number of bytes Encode would produce.
func (r Record) EncodedSize() int {
	return HeaderSize + len(r.Key) + len(r.Value)
}

// Encode serialises r into a self-describing, CRC-protected byte slice.
//
// Wire layout (big-endian):
//
//	bytes 0-3   : CRC32-IEEE of bytes 4..end
//	bytes 4-11  : Seq  (uint64)
//	byte  12    : Type (0=PUT, 1=DELETE)
//	bytes 13-16 : KeyLen  (uint32)
//	bytes 17-20 : ValLen  (uint32)
//	bytes 21..  : Key bytes, then Value bytes
func Encode(r Record) []byte {
	buf := make([]byte, HeaderSize+len(r.Key)+len(r.Value))
	binary.BigEndian.PutUint64(buf[4:12], r.Seq)
	buf[12] = r.Type
	binary.BigEndian.PutUint32(buf[13:17], uint32(len(r.Key)))
	binary.BigEndian.PutUint32(buf[17:21], uint32(len(r.Value)))
	copy(buf[HeaderSize:], r.Key)
	copy(buf[HeaderSize+len(r.Key):], r.Value)
	// CRC covers everything after the CRC field itself (bytes 4 to end).
	binary.BigEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:]))
	return buf
}

// Decode reads exactly one record from rd.
//
// Return values:
//
//	(rec, n, nil)          – success; n = total bytes consumed.
//	(_, 0, io.EOF)         – rd was at EOF before any byte was read (clean end-of-file).
//	(_, n, ErrTruncated)   – some bytes were read, then rd ended unexpectedly.
//	(_, n, ErrCorrupt)     – the full record is present but CRC/header is invalid.
func Decode(rd io.Reader) (Record, int, error) {
	var header [HeaderSize]byte

	// Step 1: read the fixed-size header.
	_, err := io.ReadFull(rd, header[:])
	if err != nil {
		if errors.Is(err, io.EOF) {
			// Zero bytes were available: clean end of file.
			return Record{}, 0, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			// We got some bytes but not the full header: torn write.
			return Record{}, 0, ErrTruncated
		}
		return Record{}, 0, fmt.Errorf("record: read header: %w", err)
	}

	// Step 2: parse the header fields (all big-endian).
	storedCRC := binary.BigEndian.Uint32(header[0:4])
	seq := binary.BigEndian.Uint64(header[4:12])
	recType := header[12]
	keyLen := binary.BigEndian.Uint32(header[13:17])
	valLen := binary.BigEndian.Uint32(header[17:21])

	// Step 3: validate BEFORE allocating – protects against corrupt lengths.
	if recType > 1 {
		return Record{}, HeaderSize, ErrCorrupt
	}
	if keyLen == 0 || keyLen > MaxKeySize {
		// Empty keys are not allowed; keys larger than 1 MiB are not allowed.
		return Record{}, HeaderSize, ErrCorrupt
	}
	if valLen > MaxValueSize {
		return Record{}, HeaderSize, ErrCorrupt
	}
	// Tombstones must have no value.
	if recType == TypeDelete && valLen != 0 {
		return Record{}, HeaderSize, ErrCorrupt
	}

	// Step 4: read the body (key + value).
	bodyLen := int(keyLen) + int(valLen)
	body := make([]byte, bodyLen)
	if bodyLen > 0 {
		_, err = io.ReadFull(rd, body)
		if err != nil {
			// Any EOF here (expected or not) means the record is truncated.
			return Record{}, HeaderSize, ErrTruncated
		}
	}

	// Step 5: verify CRC over header[4:] + body.
	h := crc32.NewIEEE()
	h.Write(header[4:])
	h.Write(body)
	if h.Sum32() != storedCRC {
		return Record{}, HeaderSize + bodyLen, ErrCorrupt
	}

	rec := Record{
		Seq:   seq,
		Type:  recType,
		Key:   body[:keyLen],
		Value: body[keyLen:],
	}
	return rec, HeaderSize + bodyLen, nil
}

// DecodeBytes is a convenience wrapper that decodes a single record from a
// byte slice. It returns ErrTruncated if b is shorter than a full record.
func DecodeBytes(b []byte) (Record, error) {
	br := byteReader(b)
	r, _, err := Decode(&br)
	return r, err
}

// byteReader is a minimal io.Reader backed by a byte slice so we can avoid
// importing "bytes" at the package level (keeps the import graph flat).
type byteReader []byte

func (b *byteReader) Read(p []byte) (int, error) {
	if len(*b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, *b)
	*b = (*b)[n:]
	return n, nil
}
