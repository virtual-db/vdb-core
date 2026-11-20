// Package write — handlers.go
//
// The built-in handlers of the vdb.write.* pipelines: the apply points at
// priority 10 do the work; build_context stamps correlation ids.
package write

import (
	"fmt"

	"github.com/virtual-db/vdb-core/internal/framework"
	"github.com/virtual-db/vdb-core/internal/payloads"
	"github.com/virtual-db/vdb-core/internal/points"
	"github.com/virtual-db/vdb-core/internal/txn"
	"github.com/virtual-db/vdb-core/internal/types"
)

// TxFor returns the open transaction of a connection.
type TxFor func(connID uint64) (*txn.Tx, bool)

// Handlers attaches the write pipelines' built-in handlers.
type Handlers struct {
	W     *Writer
	TxFor TxFor
}

// Register attaches every handler at priority 10.
func (h *Handlers) Register(r framework.Registrar) error {
	for _, reg := range []struct {
		point string
		fn    framework.PointFunc
	}{
		{points.PointWriteInsertBuildContext, framework.BuildContext},
		{points.PointWriteUpdateBuildContext, framework.BuildContext},
		{points.PointWriteDeleteBuildContext, framework.BuildContext},
		{points.PointWriteTruncateBuildContext, framework.BuildContext},
		{points.PointWriteInsertApply, h.InsertApply},
		{points.PointWriteUpdateApply, h.UpdateApply},
		{points.PointWriteDeleteApply, h.DeleteApply},
		{points.PointWriteTruncateApply, h.TruncateApply},
	} {
		if err := r.Attach(reg.point, 10, reg.fn); err != nil {
			return fmt.Errorf("write: attach %q: %w", reg.point, err)
		}
	}
	return nil
}

func (h *Handlers) tx(conn uint64) (*txn.Tx, error) {
	tx, ok := h.TxFor(conn)
	if !ok {
		return nil, &types.Error{Code: types.ErrInternal, Msg: "no open transaction for the connection"}
	}
	return tx, nil
}

// InsertApply inserts the payload's rows and fills Results.
func (h *Handlers) InsertApply(ctx any, p any) (any, any, error) {
	pl, err := payloads.Decode[payloads.WriteInsertPayload](p)
	if err != nil {
		return ctx, nil, fmt.Errorf("write.insert.apply: %w", err)
	}
	tx, err := h.tx(pl.ConnectionID)
	if err != nil {
		return ctx, nil, err
	}
	res, err := h.W.Insert(tx, pl.TableID, pl.Rows, pl.Images)
	if err != nil {
		return ctx, nil, err
	}
	pl.Results = res
	return ctx, pl, nil
}

// UpdateApply replaces the payload's row.
func (h *Handlers) UpdateApply(ctx any, p any) (any, any, error) {
	pl, err := payloads.Decode[payloads.WriteUpdatePayload](p)
	if err != nil {
		return ctx, nil, fmt.Errorf("write.update.apply: %w", err)
	}
	tx, err := h.tx(pl.ConnectionID)
	if err != nil {
		return ctx, nil, err
	}
	if err := h.W.Update(tx, pl.TableID, pl.Old, pl.New, pl.Images); err != nil {
		return ctx, nil, err
	}
	return ctx, pl, nil
}

// DeleteApply removes the payload's row.
func (h *Handlers) DeleteApply(ctx any, p any) (any, any, error) {
	pl, err := payloads.Decode[payloads.WriteDeletePayload](p)
	if err != nil {
		return ctx, nil, fmt.Errorf("write.delete.apply: %w", err)
	}
	tx, err := h.tx(pl.ConnectionID)
	if err != nil {
		return ctx, nil, err
	}
	if err := h.W.Delete(tx, pl.TableID, pl.Old); err != nil {
		return ctx, nil, err
	}
	return ctx, pl, nil
}

// TruncateApply detaches the payload's table.
func (h *Handlers) TruncateApply(ctx any, p any) (any, any, error) {
	pl, err := payloads.Decode[payloads.WriteTruncatePayload](p)
	if err != nil {
		return ctx, nil, fmt.Errorf("write.truncate.apply: %w", err)
	}
	tx, err := h.tx(pl.ConnectionID)
	if err != nil {
		return ctx, nil, err
	}
	if err := h.W.Truncate(tx, pl.TableID); err != nil {
		return ctx, nil, err
	}
	return ctx, pl, nil
}
