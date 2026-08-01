// Package types provides shared interfaces used by both vdb-core and
// database engine adapters (e.g. vdb-mysql-driver). Placing interfaces
// here avoids the internal package import restriction.
package types

// RecordIter is a streaming iterator over map[string]any records.
// Implementations must be safe for concurrent use. Callers must call
// Close when done consuming the iterator.
type RecordIter interface {
	// Next returns the next record. Returns a non-nil error (typically io.EOF)
	// when the iterator is exhausted.
	Next() (map[string]any, error)
	// Close releases any resources held by the iterator. Must be idempotent.
	Close() error
}
