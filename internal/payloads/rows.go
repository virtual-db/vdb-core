package payloads

import "github.com/virtual-db/vdb-core/types"

// RecordIter is an alias for types.RecordIter, re-exported for convenience
// within the payloads package. See types.RecordIter for documentation.
type RecordIter = types.RecordIter

// RecordsSourcePayload is the payload for the vdb.records.source pipeline.
// Records holds the streaming source records before the delta overlay is applied.
type RecordsSourcePayload struct {
	ConnectionID uint32
	Table        string
	Records      RecordIter
}

// RecordsMergedPayload is the payload for the vdb.records.merged pipeline.
type RecordsMergedPayload struct {
	ConnectionID uint32
	Table        string
	Records      []map[string]any
}
