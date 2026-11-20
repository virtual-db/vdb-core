// Package merge — handlers.go
//
// The merge itself is code, not a handler; this registers the records
// pipelines' build_context points.
package merge

import (
	"fmt"

	"github.com/virtual-db/vdb-core/internal/framework"
	"github.com/virtual-db/vdb-core/internal/points"
)

// Handlers attaches the records pipelines' built-in handlers.
type Handlers struct{}

// Register attaches build_context at priority 10.
func (Handlers) Register(r framework.Registrar) error {
	for _, p := range []string{points.PointRecordsSourceBuildContext, points.PointRecordsMergedBuildContext} {
		if err := r.Attach(p, 10, framework.BuildContext); err != nil {
			return fmt.Errorf("merge: attach %q: %w", p, err)
		}
	}
	return nil
}
