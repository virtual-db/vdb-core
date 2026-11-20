// Package txn — handlers.go
//
// The built-in handlers of the vdb.transaction.* pipelines. The commit
// pipeline's apply point at priority 10 publishes; a sync persistence handler
// attached below 10 runs first and can fail the commit (doc 06 §2.3).
package txn

import (
	"fmt"

	"github.com/virtual-db/vdb-core/internal/framework"
	"github.com/virtual-db/vdb-core/internal/payloads"
	"github.com/virtual-db/vdb-core/internal/points"
	"github.com/virtual-db/vdb-core/internal/types"
)

// Handlers attaches the transaction pipelines' built-in handlers.
type Handlers struct {
	M *Manager
}

// Register attaches every handler at priority 10.
func (h *Handlers) Register(r framework.Registrar) error {
	for _, reg := range []struct {
		point string
		fn    framework.PointFunc
	}{
		{points.PointTransactionBeginBuildContext, framework.BuildContext},
		{points.PointTransactionCommitBuildContext, framework.BuildContext},
		{points.PointTransactionRollbackBuildContext, framework.BuildContext},
		{points.PointTransactionCommitApply, h.CommitApply},
	} {
		if err := r.Attach(reg.point, 10, reg.fn); err != nil {
			return fmt.Errorf("txn: attach %q: %w", reg.point, err)
		}
	}
	return nil
}

// CommitApply publishes the committing transaction's changes. A change set
// without a pending transaction (captured statements) is replayed directly.
func (h *Handlers) CommitApply(ctx any, p any) (any, any, error) {
	pl, err := payloads.Decode[payloads.TransactionCommitPayload](p)
	if err != nil {
		return ctx, nil, fmt.Errorf("transaction.commit.apply: %w", err)
	}
	if pl.Change == nil {
		return ctx, pl, nil
	}
	if tx, ok := h.M.Pending(pl.ConnectionID); ok && pl.Change.Kind == types.ChangeCommit {
		if err := h.M.Publish(tx, pl.Change); err != nil {
			return ctx, nil, fmt.Errorf("transaction.commit.apply: %w", err)
		}
	}
	return ctx, pl, nil
}
