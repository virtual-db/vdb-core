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

// Key carries a key's sort image and its raw form in the engine's own key format.
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

// SessionInfo identifies one of the engine's client connections.
type SessionInfo struct {
	ConnID  uint64
	Account string
	Control bool
}

// SnapshotScope says how long a snapshot pair lives. The engine maps its
// isolation levels onto it (implementation plan doc 04 §2, doc 09 §3.3).
type SnapshotScope uint8

const (
	// PerTransaction keeps one snapshot pair for the whole transaction.
	PerTransaction SnapshotScope = iota
	// PerStatement takes a new snapshot pair for each statement.
	PerStatement
)

// ReadView says which Delta state a plain read sees (doc 04 §2.3).
type ReadView uint8

const (
	// ViewSnapshot is the transaction's own writes over its snapshot.
	ViewSnapshot ReadView = iota
	// ViewUncommitted is the current committed state overlaid with every open
	// transaction's writes.
	ViewUncommitted
)

// EvalContext holds the session settings the Source must mirror. Opaque to the core.
type EvalContext map[string]string

// StmtContext describes the statement that is starting.
type StmtContext struct {
	Database string
	Snapshot SnapshotScope
	View     ReadView
	LockWait time.Duration
	Eval     EvalContext
	ReadOnly bool
	Digest   string
}

// ReadLocks makes a read a locking read. The engine decides what to lock; the
// core calls Row for every row the read returns, before it re-reads that
// row's latest committed version (product doc 05 §3.2), and Missing for every
// key of a lookup that found nothing. Key is LockKey of the row.
type ReadLocks struct {
	Row     func(t TableID, key KeyImage) error
	Missing func(t TableID, idx IndexID, img KeyImage) error
}

// LockKey is the lock key of a row: its Source key bytes when it has them,
// else its identity image. The engine and the core compute it the same way,
// so both sides of a conflict name the same resource.
func LockKey(sk SourceKey, img KeyImage) KeyImage {
	if len(sk) > 0 {
		return append(KeyImage{'s'}, sk...)
	}
	return append(KeyImage{'i'}, img...)
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
	Locks   *ReadLocks // nil for a plain read
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
	// Reserve takes a Source connection for a snapshot without starting it.
	// It may wait for a free connection, so the core calls it outside the
	// commit lock; Start then opens the snapshot under that lock (D-35).
	Reserve(s SessionInfo, ctx StmtContext) (SourceSnapshot, error)
}

// SourceSnapshot is the Source half of a snapshot pair.
type SourceSnapshot interface {
	// Start opens the consistent snapshot on the reserved connection.
	Start() error
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

// ── Locks (doc 04 §3) ──

// LockMode is a lock mode the engine defines.
type LockMode uint8

// Resource is what a lock covers: one key image (Point), one range of images
// (Range), or a whole index (Whole), within (Table, Index).
type Resource struct {
	Table TableID
	Index IndexID
	Point KeyImage
	Range *ImageRange
	Whole bool
}

// LockModes is the engine's definition of its lock modes. Conflicts is asked
// only about overlapping resources held by other transactions. Covers reports
// whether a held lock on the same resource makes a wanted one unnecessary
// (nil: only the same mode). WaitOnly modes are waited for but not kept.
type LockModes struct {
	Names     []string
	Conflicts func(held, wanted LockMode, heldRes, wantedRes Resource) bool
	Covers    func(held, wanted LockMode) bool
	WaitOnly  func(m LockMode) bool
}

// LockMark is a position in a session's list of held locks.
type LockMark uint64

// WaitFunc is told the connection id of a lock holder before a call blocks.
type WaitFunc func(holder uint64)

// ── Errors (doc 03 §3.6) ──

// ErrorCode classifies a core error.
type ErrorCode uint16

const (
	ErrNone ErrorCode = iota
	ErrDuplicateKey
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
	ErrDuplicateKey:    "duplicate key",
	ErrLockWaitTimeout: "lock wait timeout", ErrDeadlock: "deadlock",
	ErrDeltaFull: "delta memory limit reached", ErrLockMemory: "lock memory limit reached",
	ErrRefused: "refused", ErrSource: "source error", ErrKilled: "killed", ErrInternal: "internal error",
}

// String names the code.
func (c ErrorCode) String() string { return codeNames[c] }

// Error is the one error type the core returns to the engine.
type Error struct {
	Code   ErrorCode
	Table  TableID
	Index  IndexID
	Holder uint64
	Msg    string
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
}

// AsErrorOrNil is AsError that returns a nil error interface for nil.
func AsErrorOrNil(err error) error {
	if err == nil {
		return nil
	}
	return AsError(err)
}
