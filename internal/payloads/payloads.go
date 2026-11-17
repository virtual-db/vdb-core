// Package payloads holds the typed payload structs that cross pipelines and
// events (implementation plan doc 06 §5.3). Handlers read them with
// Decode[T], never with a bare type assertion, so they still work after an
// out-of-process plugin round-tripped the payload through JSON.
package payloads

import "github.com/virtual-db/vdb-core/internal/types"

// ConnectionOpenedPayload is the payload of vdb.connection.opened.
type ConnectionOpenedPayload struct {
	ConnectionID uint64
	User         string
	Address      string
}

// ConnectionClosedPayload is the payload of vdb.connection.closed.
type ConnectionClosedPayload struct {
	ConnectionID uint64
	User         string
	Address      string
}

// QueryReceivedPayload is the payload of vdb.query.received.
type QueryReceivedPayload struct {
	ConnectionID uint64
	Query        string
	Database     string
}

// QueryCompletedPayload is the payload of the vdb.query.completed event.
type QueryCompletedPayload struct {
	ConnectionID uint64
	Query        string
	Digest       string
	Database     string
	Account      string
	DurationMs   float64
	RowsAffected int64
	RowsSent     int64
	ErrorCode    uint16
}

// RecordsPayload is the payload of vdb.records.source and vdb.records.merged.
type RecordsPayload struct {
	ConnectionID uint64
	Table        string
	Columns      []string
	Rows         []types.Row
}

// WriteInsertPayload is the payload of vdb.write.insert. Results is filled by
// the apply handler. Images are the engine's sort images of every index.
type WriteInsertPayload struct {
	ConnectionID uint64
	Table        string
	TableID      types.TableID
	Columns      []string
	Rows         []types.Row
	Images       [][]types.KeyImage
	Results      []types.WriteResult
}

// WriteUpdatePayload is the payload of vdb.write.update.
type WriteUpdatePayload struct {
	ConnectionID uint64
	Table        string
	TableID      types.TableID
	Columns      []string
	Old          types.RowRef
	New          types.Row
	Images       []types.KeyImage
}

// WriteDeletePayload is the payload of vdb.write.delete.
type WriteDeletePayload struct {
	ConnectionID uint64
	Table        string
	TableID      types.TableID
	Old          types.RowRef
}

// WriteTruncatePayload is the payload of vdb.write.truncate.
type WriteTruncatePayload struct {
	ConnectionID uint64
	Table        string
	TableID      types.TableID
}

// TransactionBeginPayload is the payload of vdb.transaction.begin.
type TransactionBeginPayload struct {
	ConnectionID uint64
	ReadOnly     bool
	Isolation    types.Isolation
}

// TransactionCommitPayload is the payload of vdb.transaction.commit and of the
// vdb.transaction.committed event; Change carries the change set.
type TransactionCommitPayload struct {
	ConnectionID uint64
	Change       *types.ChangeSet
}

// TransactionRollbackPayload is the payload of vdb.transaction.rollback.
type TransactionRollbackPayload struct {
	ConnectionID uint64
	Savepoint    string
}

// ServerStartPayload is the payload of vdb.server.start. A persistence
// plugin attached before priority 10 at configure returns change sets to
// restore in Restore.
type ServerStartPayload struct {
	Restore []types.ChangeSet
}

// ServerStopPayload is the payload of vdb.server.stop.
type ServerStopPayload struct {
	Reason string
}

// SchemaPayload is the payload of vdb.schema.loaded and vdb.schema.invalidated.
type SchemaPayload struct {
	Database string
	Table    string
	TableID  types.TableID
	Kind     string
	Version  uint64
	Columns  []string
}

// SourceRequestedPayload is the payload of vdb.source.requested.
type SourceRequestedPayload struct {
	ConnectionID uint64
	Digest       string
	Shape        string
	Table        string
	DurationUs   int64
	Rows         int64
	Bytes        int64
	Error        string
}

// LimitReachedPayload is the payload of vdb.limit.reached.
type LimitReachedPayload struct {
	Limit        string
	Value        uint64
	ConnectionID uint64
}

// SnapshotExpiredPayload is the payload of vdb.snapshot.expired.
type SnapshotExpiredPayload struct {
	ConnectionID uint64
	AgeMs        int64
}

// KeyConflictPayload is the payload of vdb.key.conflict.
type KeyConflictPayload struct {
	Table    string
	TableID  types.TableID
	KeyImage []byte
}
