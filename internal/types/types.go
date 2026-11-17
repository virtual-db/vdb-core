// Package types holds the data types shared by every vdb-core package and by
// the engines built on the core. The root package re-exports each one as an
// alias (types.go), so an engine imports only github.com/virtual-db/vdb-core;
// the types live here only so internal packages can use them without an
// import cycle through the root package.
package types

import (
	"bytes"
	"encoding/binary"
	"time"
)

// ── Identity of tables, indexes, keys and rows (implementation plan doc 03 §3.1) ──

// TableID is assigned by the catalog; stable for the life of the process; never reused.
type TableID uint32

// IndexID is the position of an index in the table definition. 0 is the primary
// key or the identity key.
type IndexID uint16

// KeyImage is a sort image computed by the engine. The core compares images
// with bytes.Compare and never builds one.
type KeyImage []byte

// SourceKey is the exact bytes of a Source row's key columns, as the Source
// returned them, length-prefixed per column (see EncodeSourceKey).
type SourceKey []byte

// Value is one column in the engine's text form. Opaque to the core.
type Value struct {
	B    []byte
	Null bool
}

// Row is a list of columns in the order the request named them.
type Row []Value

// Key carries a key's sort image and its raw engine form (MySQL's key format).
type Key struct {
	Image KeyImage
	Raw   []byte
}

// Origin says where a row came from.
type Origin uint8

const (
	// OriginSource marks a row read from the Source.
	OriginSource Origin = iota
	// OriginDelta marks a row stored in the Delta.
	OriginDelta
)

// RowRef identifies the row being updated or deleted. Whole is set only for
// tables without a usable key (D-10).
type RowRef struct {
	Origin    Origin
	PK        Key
	SourceKey SourceKey
	Whole     Row
}

// ── Requests (doc 03 §3.2) ──

// SessionInfo identifies a mysqld connection.
type SessionInfo struct {
	ConnID  uint64
	Account string
	Control bool
}

// Isolation is a transaction isolation level.
type Isolation uint8

const (
	RepeatableRead Isolation = iota
	ReadCommitted
	ReadUncommitted
	Serializable
)

// LockMode is the row lock a read or write needs.
type LockMode uint8

const (
	LockNone LockMode = iota
	LockShared
	LockExclusive
)

// EvalContext holds the session settings the Source must mirror. Opaque to the core.
type EvalContext map[string]string

// StmtContext describes the statement that is starting.
type StmtContext struct {
	Database         string
	Isolation        Isolation
	Autocommit       bool
	LockIntent       LockMode
	ForeignKeyChecks bool
	LockWait         time.Duration
	Eval             EvalContext
	ReadOnly         bool
	Digest           string
}

// ImageRange is a range of sort images; an empty bound is open. Bounds are
// compared as prefixes, so a bound on fewer key parts covers every longer key
// that starts with it.
type ImageRange struct {
	Low, High         KeyImage
	LowIncl, HighIncl bool
}

// KeyRange is one range of a scan: images for the Delta, raw bounds for the Source.
type KeyRange struct {
	Images          ImageRange
	RawLow, RawHigh []byte
	LowParts        uint16 // number of key parts bound in RawLow
	HighParts       uint16 // number of key parts bound in RawHigh
	RawLowIncl      bool
	RawHighIncl     bool
}

// Direction of an ordered scan.
type Direction uint8

const (
	Forward Direction = iota
	Backward
)

// ColumnSet lists column positions in the table definition.
type ColumnSet []uint16

// ScanRequest is one ScanOpen.
type ScanRequest struct {
	Table   TableID
	Index   IndexID
	Ranges  []KeyRange
	Ordered bool
	Dir     Direction
	Columns ColumnSet
	Pushed  string
	Lock    LockMode
}

// SourceScan is what the core asks the Source adapter to scan (paged, D-84).
type SourceScan struct {
	Table   TableID
	Index   IndexID
	Ranges  []KeyRange
	Ordered bool
	Dir     Direction
	Columns ColumnSet
	Pushed  string
}

// WriteResult is the outcome of one row of a batched insert.
type WriteResult struct {
	Err   *Error
	Index IndexID
}

// ── Cursors (doc 03 §3.3) ──

// Cursor yields batches of rows until it returns io.EOF.
type Cursor interface {
	Next(maxRows int, maxBytes int) (Batch, error)
	Close() error
}

// Batch is a group of rows with what update_row and delete_row will send back.
type Batch struct {
	Rows   []Row
	Refs   []RowRef
	KeyIdx []uint32
}

// ── What the engine supplies (doc 03 §3.4, §3.5) ──

// Source is the engine's Source adapter.
type Source interface {
	Snapshot(s SessionInfo, ctx StmtContext) (SourceSnapshot, error)
}

// SourceSnapshot is the Source half of a snapshot pair.
type SourceSnapshot interface {
	Scan(req SourceScan) (SourceCursor, error)
	Lookup(t TableID, idx IndexID, keys []Key, cols ColumnSet) (SourceRows, error)
	Exists(t TableID, keys []SourceKey) ([]bool, error)
	Count(t TableID) (uint64, error)
	ByColumns(t TableID, cols ColumnSet, values []Row, want ColumnSet) (SourceCursor, error)
	Close() error
}

// SourceCursor yields Source rows.
type SourceCursor interface {
	Next(maxRows int) (SourceRows, error)
	Close() error
}

// SourceRows is a batch of Source rows. Keys holds the identity key bytes of
// each row; KeyIdx, for lookups, which requested key each row answers.
type SourceRows struct {
	Rows   []Row
	Keys   []SourceKey
	Images []KeyImage
	KeyIdx []uint32
}

// MemoryProbe returns the resident memory of every process to count.
type MemoryProbe func() (rss uint64, err error)

// RowProvider produces the rows of a provided table.
type RowProvider interface {
	Rows(s SessionInfo, cols ColumnSet) (Cursor, error)
}

// ImageFunc computes the sort image of every index for rows the core writes
// itself (foreign-key cascades). Supplied by the engine per call. images[i][idx].
type ImageFunc func(t TableID, rows []Row) ([][]KeyImage, error)

// WaitFunc is told the connection id of a lock holder before a call blocks.
type WaitFunc func(holder uint64)

// ── Errors (doc 03 §3.6) ──

// ErrorCode classifies a core error.
type ErrorCode uint16

const (
	ErrNone ErrorCode = iota
	ErrDuplicateKey
	ErrNoReferencedRow
	ErrRowIsReferenced
	ErrFKDepth
	ErrLockWaitTimeout
	ErrDeadlock
	ErrDeltaFull
	ErrLockMemory
	ErrRefused
	ErrSource
	ErrKilled
	ErrInternal
)

var codeNames = map[ErrorCode]string{
	ErrDuplicateKey: "duplicate key", ErrNoReferencedRow: "no referenced row",
	ErrRowIsReferenced: "row is referenced", ErrFKDepth: "foreign key cascade too deep",
	ErrLockWaitTimeout: "lock wait timeout", ErrDeadlock: "deadlock",
	ErrDeltaFull: "delta memory limit reached", ErrLockMemory: "lock memory limit reached",
	ErrRefused: "refused", ErrSource: "source error", ErrKilled: "killed", ErrInternal: "internal error",
}

// String names the code.
func (c ErrorCode) String() string { return codeNames[c] }

// Error is the one error type the core returns to the engine.
type Error struct {
	Code       ErrorCode
	Table      TableID
	Index      IndexID
	Constraint string
	Holder     uint64
	Msg        string
}

func (e *Error) Error() string {
	if e.Msg != "" {
		return e.Code.String() + ": " + e.Msg
	}
	return e.Code.String()
}

// Errorf builds an *Error.
func Errorf(code ErrorCode, msg string) *Error { return &Error{Code: code, Msg: msg} }

// AsError returns err as *Error, wrapping a foreign error as ErrInternal.
func AsError(err error) *Error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*Error); ok {
		return e
	}
	return &Error{Code: ErrInternal, Msg: err.Error()}
}

// ── Status ──

// QueryInfo describes a completed statement.
type QueryInfo struct {
	Query        string
	Digest       string
	Database     string
	Account      string
	Duration     time.Duration
	RowsAffected int64
	RowsSent     int64
	ErrorCode    uint16
}

// TableStats are the Delta counters of one table.
type TableStats struct {
	Table       TableID
	DeltaRows   uint64
	TouchedKeys uint64
	Bytes       uint64
}

// DeltaStats are the Delta counters of the environment.
type DeltaStats struct {
	Bytes  uint64
	Tables []TableStats
	Seq    uint64
}

// LockStats are the lock manager's counters.
type LockStats struct {
	Entries   uint64
	Bytes     uint64
	Waits     uint64
	Deadlocks uint64
	Timeouts  uint64
}

// MemoryStats are the memory guard's readings.
type MemoryStats struct {
	Available uint64
	Ceiling   uint64
	Resident  uint64
	Reserve   float64
	Refusals  uint64
}

// ── Source keys ──

// EncodeSourceKey builds a SourceKey from column values: per column, a 4-byte
// little-endian length (0xFFFFFFFF for NULL) followed by the bytes.
func EncodeSourceKey(vals []Value) SourceKey {
	var b bytes.Buffer
	var n [4]byte
	for _, v := range vals {
		if v.Null {
			binary.LittleEndian.PutUint32(n[:], 0xFFFFFFFF)
			b.Write(n[:])
			continue
		}
		binary.LittleEndian.PutUint32(n[:], uint32(len(v.B)))
		b.Write(n[:])
		b.Write(v.B)
	}
	return b.Bytes()
}

// DecodeSourceKey splits a SourceKey back into values.
func DecodeSourceKey(k SourceKey) []Value {
	var out []Value
	for len(k) >= 4 {
		n := binary.LittleEndian.Uint32(k[:4])
		k = k[4:]
		if n == 0xFFFFFFFF {
			out = append(out, Value{Null: true})
			continue
		}
		if int(n) > len(k) {
			break
		}
		out = append(out, Value{B: k[:n]})
		k = k[n:]
	}
	return out
}

// RowSize is the accounted size of a row.
func RowSize(r Row) int {
	n := 24
	for _, v := range r {
		n += len(v.B) + 25
	}
	return n
}

// CloneRow deep-copies a row so callers may reuse their buffers.
func CloneRow(r Row) Row {
	out := make(Row, len(r))
	for i, v := range r {
		if v.Null {
			out[i] = Value{Null: true}
			continue
		}
		out[i] = Value{B: append([]byte(nil), v.B...)}
	}
	return out
}

// Project returns the columns cols of a full row.
func Project(r Row, cols ColumnSet) Row {
	if cols == nil {
		return r
	}
	out := make(Row, len(cols))
	for i, c := range cols {
		if int(c) < len(r) {
			out[i] = r[c]
		} else {
			out[i] = Value{Null: true}
		}
	}
	return out
}

// ── Change sets (doc 06 §2) ──

// ChangeKind classifies a change set.
type ChangeKind uint8

const (
	// ChangeCommit is a committed client transaction.
	ChangeCommit ChangeKind = iota
	// ChangeCaptured is a captured view, routine or trigger statement.
	ChangeCaptured
	// ChangeSource is a Source change applied to the catalog.
	ChangeSource
)

// RowOp is the operation of a RowChange.
type RowOp uint8

const (
	RowPut RowOp = iota
	RowDelete
)

// RowChange is one Delta row change.
type RowChange struct {
	Table     TableID
	Op        RowOp
	Image     KeyImage
	Origin    Origin
	SourceKey SourceKey
	Row       Row
	Images    []KeyImage
}

// TouchChange is one touched-key change.
type TouchChange struct {
	Table TableID
	Key   SourceKey
	Image KeyImage
	Delta int32
	Whole bool
}

// AutoIncMove records an auto-increment counter's new next value.
type AutoIncMove struct {
	Table TableID
	Next  uint64
}

// ChangeSet is what a commit, a captured statement or a Source change adds.
// Its order is the order of the writes. V is the JSON format version.
type ChangeSet struct {
	V        int
	Seq      uint64
	Kind     ChangeKind
	Rows     []RowChange
	Touched  []TouchChange
	Truncate []TableID // tables truncated (detached); earlier row changes of them are dropped
	Replaced []TableID // tables whose Delta rows were replaced by an ALTER's conversion
	Catalog  []CatalogChange
	Objects  []Object
	AutoInc  []AutoIncMove
}

// AsErrorOrNil is AsError that returns a nil error interface for nil.
func AsErrorOrNil(err error) error {
	if err == nil {
		return nil
	}
	return AsError(err)
}
