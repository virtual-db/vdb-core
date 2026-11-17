// Package types — api.go
//
// The engine-facing interfaces: DriverAPI, Session, ScanHandle and Catalog
// (implementation plan doc 03 §4, doc 06 §1.2). The root package re-exports
// them; they are declared here so internal/driverapi can return them.
package types

// DriverAPI is the engine's only door into the core.
type DriverAPI interface {
	// Sessions: one per mysqld connection that touches a VDB table.
	OpenSession(info SessionInfo) (Session, error)
	// Kill ends any lock wait of the connection and marks its session killed.
	Kill(connID uint64)

	// Catalog of record.
	Catalog() Catalog

	// Client connections and statements, fed by the statements plug-in.
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
}

// Session is one mysqld connection's view of the core. Used by one goroutine
// at a time, except that Kill may come from any goroutine.
type Session interface {
	// Callbacks for the call that follows: told the holder before a lock
	// wait, and asked for the sort images of rows a cascade writes (D-86).
	SetCallbacks(wait WaitFunc, images ImageFunc)

	StmtBegin(ctx StmtContext) error
	StmtEnd() error
	StmtRollback() error
	Snapshot() error
	Commit() error
	Rollback() error
	Savepoint(name string) error
	RollbackTo(name string) error
	Release(name string) error

	Scan(req ScanRequest) (ScanHandle, error)
	Lookup(t TableID, idx IndexID, keys []Key, cols ColumnSet, lock LockMode) (Batch, []KeyImage, error)
	Count(t TableID) (count uint64, declined bool, err error)

	Insert(t TableID, rows []Row, images [][]KeyImage) ([]WriteResult, error)
	Update(t TableID, old RowRef, row Row, images []KeyImage) error
	Delete(t TableID, old RowRef) error
	Truncate(t TableID) error
	AutoIncrement(t TableID, n uint64) (first, granted uint64, err error)

	// DDL, staged in the transaction so it commits with mysqld's DDL.
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
	TableByName(db, name string) (TableDef, bool)
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
