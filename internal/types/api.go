// Package types — api.go
//
// The engine-facing interfaces: DriverAPI, Session, ScanHandle and Catalog
// (implementation plan doc 03 §4, doc 06 §1.2). The root package re-exports
// them; they are declared here so internal/driverapi can return them.
package types

// DriverAPI is the engine's only door into the core.
type DriverAPI interface {
	// Sessions: one per client connection that touches a VDB table.
	OpenSession(info SessionInfo) (Session, error)
	// Kill ends any lock wait of the connection and marks its session killed.
	Kill(connID uint64)

	// Catalog of record.
	Catalog() Catalog
	// Locks is the lock table; the engine decides what to lock and when.
	Locks() Locks

	// Client connections and statements, as the engine observes them.
	ConnectionOpened(connID uint64, account, addr string) error
	ConnectionClosed(connID uint64, account, addr string)
	QueryReceived(connID uint64, query, database string) (string, error)
	QueryCompleted(connID uint64, q QueryInfo)

	// Status for the vdb schema and metrics.
	DeltaStats() DeltaStats
	LockStats() LockStats
	MemoryStats() MemoryStats
	// Delta returns a stable read of the committed Delta (vdb.delta_sql).
	Delta() DeltaReader
	// Transforms lists columns plugins transform, as "database.table.column".
	Transforms() []string
	// Emit publishes an engine event (vdb.source.requested and the others).
	Emit(event string, payload any)
	// HasHandlers reports whether anything besides the built-in handlers is
	// attached to a point.
	HasHandlers(point string) bool
}

// Locks is the lock table (implementation plan doc 04 §3). Define is called
// once, before Run; the other calls take and drop locks for a session's open
// transaction.
type Locks interface {
	Define(m LockModes)
	// Acquire blocks until the lock is granted; wait is told the holder first.
	Acquire(s Session, r Resource, mode LockMode, wait WaitFunc) error
	// ReleaseSince drops the locks the session took after mark.
	ReleaseSince(s Session, mark LockMark)
	// ReleaseIf drops the session's locks for which drop returns true.
	ReleaseIf(s Session, drop func(r Resource, mode LockMode) bool)
	// Covered reports whether the session already holds a range or
	// whole-index lock containing r in a mode that covers mode (Covers), so a
	// lock on r would add nothing.
	Covered(s Session, r Resource, mode LockMode) bool
	// Release drops the locks the session took on exactly r after mark; a
	// lock on r held at or before mark stays (READ COMMITTED unlock_row).
	Release(s Session, r Resource, mark LockMark)
}

// Session is one client connection's view of the core. Used by one goroutine
// at a time, except that Kill may come from any goroutine.
type Session interface {
	StmtBegin(ctx StmtContext) error
	StmtEnd() error
	StmtRollback() error
	Snapshot() error
	Commit() error
	Rollback() error
	Savepoint(name string) error
	RollbackTo(name string) error
	Release(name string) error
	// LockMark is the position in the session's lock list, for ReleaseSince.
	LockMark() LockMark

	Scan(req ScanRequest) (ScanHandle, error)
	// Lookup answers keys on index idx as a cursor (see LookupCursor).
	Lookup(t TableID, idx IndexID, keys []Key, cols ColumnSet, locks *ReadLocks) (LookupCursor, error)
	// ByColumns reads every merged row whose columns cols equal values: the
	// Delta through index idx at image img (a key prefix), the Source by value.
	// The rows come as a cursor, so the caller can stop early (RESTRICT).
	ByColumns(t TableID, idx IndexID, img KeyImage, cols ColumnSet, values Row, want ColumnSet, locks *ReadLocks) (Cursor, error)
	Count(t TableID) (count uint64, declined bool, err error)

	Insert(t TableID, rows []Row, images [][]KeyImage) ([]WriteResult, error)
	Update(t TableID, old RowRef, row Row, images []KeyImage) error
	Delete(t TableID, old RowRef) error
	Truncate(t TableID) error
	// DeleteAll is a DELETE without WHERE (SD-1): the table is detached like
	// TRUNCATE, but as a Delta change that statement and transaction rollback
	// undo, with no catalog change, so the engine keeps AUTO_INCREMENT. It
	// returns the rows deleted, the table's exact row count. declined means
	// the count was declined (too many shadowed keys): nothing was changed
	// and the engine deletes row by row.
	DeleteAll(t TableID) (deleted uint64, declined bool, err error)

	// DDL, staged in the transaction so it commits with the engine's own DDL.
	ApplyDDL(c CatalogChange) (TableID, error)
	ApplySourceDDL(c CatalogChange) (TableID, error)
	AlterRows(t TableID) Cursor
	AlterPut(t TableID, rows []Row, images [][]KeyImage) error

	// Info returns the session's identity; InTransaction whether one is open.
	Info() SessionInfo
	InTransaction() bool

	End() error
}

// ScanHandle is one ScanOpen: one cursor for unordered scans, two for ordered ones.
type ScanHandle interface {
	Source() Cursor
	Delta() Cursor
	Merged() Cursor
	ShadowImages() []KeyImage
	Close() error
}

// Catalog is the catalog of record as the engine sees it.
type Catalog interface {
	Tables() []TableDef
	Table(id TableID) (TableDef, bool)
	TableByName(namespace []string, name string) (TableDef, bool)
	Objects() []Object

	Load(defs []TableDef, objs []Object) error
	ApplySource(change SourceChange) (TableID, error)
	Provide(id TableID, p RowProvider)
	RecordObject(obj Object) error
}

// DeltaRow is one committed Delta row as DeltaReader yields it.
type DeltaRowInfo struct {
	Table     TableID
	Image     KeyImage
	Row       Row
	Origin    Origin
	SourceKey SourceKey
}

// TouchedInfo is one committed touched key.
type TouchedInfo struct {
	Table TableID
	Key   SourceKey
	Count uint32
}

// DeltaReader reads one stable snapshot of the committed Delta.
type DeltaReader interface {
	Tables() []TableID
	Rows(t TableID, fn func(DeltaRowInfo) bool)
	Touched(t TableID, fn func(TouchedInfo) bool)
}
