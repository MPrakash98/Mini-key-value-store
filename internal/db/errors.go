package db

import "errors"

var (
	// ErrNotFound is returned by Get when the key does not exist (or was deleted).
	ErrNotFound = errors.New("db: key not found")

	// ErrKeyEmpty is returned when the caller passes a zero-length key.
	ErrKeyEmpty = errors.New("db: key must not be empty")

	// ErrKeyTooLong is returned when the key exceeds MaxKeySize.
	ErrKeyTooLong = errors.New("db: key too large")

	// ErrValueTooLong is returned when the value exceeds MaxValueSize.
	ErrValueTooLong = errors.New("db: value too large")

	// ErrClosed is returned when a method is called on a closed DB.
	ErrClosed = errors.New("db: closed")

	// ErrBroken is returned when the DB is in fail-stop mode due to an I/O
	// error. Reopen the DB to recover (assuming the underlying issue is fixed).
	ErrBroken = errors.New("db: store is in fail-stop mode after an I/O error; reopen it")

	// ErrReadOnly is returned when a write is attempted on a read-only follower
	// (used in Stretch C – replication).
	ErrReadOnly = errors.New("db: read-only")
)
