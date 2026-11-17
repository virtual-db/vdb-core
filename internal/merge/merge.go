// Package merge implements reads over the merged table (implementation plan
// doc 05 §1, product doc 05 §3): unordered scans, the Delta and Source
// cursors of ordered scans, lookups, and the exact count. The merge rule:
// visible(T) = Source rows whose key is neither touched nor held by the Delta,
// plus the Delta rows.
package merge

import (
	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/lock"
	"github.com/virtual-db/vdb-core/internal/txn"
	"github.com/virtual-db/vdb-core/internal/types"
)

// RecordsFunc runs vdb.records.source or vdb.records.merged on one batch. It
// must return as many rows as it was given.
type RecordsFunc func(tx *txn.Tx, def *types.TableDef, cols types.ColumnSet, rows []types.Row) ([]types.Row, error)

// Hooks are the pipeline runs the merge makes per batch, and the event it emits.
type Hooks struct {
	Source      RecordsFunc
	Merged      RecordsFunc
	KeyConflict func(t types.TableID, img types.KeyImage)
}

// Config holds the merge's settings.
type Config struct {
	CountDeclineKeys int // vdb_count_max_shadowed
	KeyBatch         int // vdb_lookup_batch
}

// Merge performs reads. One per environment; safe for concurrent use.
type Merge struct {
	cfg   Config
	hooks Hooks
}

// New returns a Merge.
func New(cfg Config, hooks Hooks) *Merge {
	if cfg.CountDeclineKeys <= 0 {
		cfg.CountDeclineKeys = 100000
	}
	if cfg.KeyBatch <= 0 {
		cfg.KeyBatch = 1000
	}
	return &Merge{cfg: cfg, hooks: hooks}
}

// SetHooks installs the hooks.
func (m *Merge) SetHooks(h Hooks) { m.hooks = h }

// LockKey is the record-lock key of a row: its Source key bytes when it has
// them, else its identity image. Both sides of a conflict compute it the same way.
func LockKey(sk types.SourceKey, img types.KeyImage) types.KeyImage {
	if len(sk) > 0 {
		return append(types.KeyImage{'s'}, sk...)
	}
	return append(types.KeyImage{'i'}, img...)
}

// LockRow takes a record lock on a row.
func LockRow(tx *txn.Tx, t types.TableID, sk types.SourceKey, img types.KeyImage, mode types.LockMode) error {
	if mode == types.LockNone {
		return nil
	}
	return tx.Lock(lock.Request{Table: t, Kind: lock.Record, Mode: mode, Key: LockKey(sk, img)})
}

func def(tx *txn.Tx, t types.TableID) (*types.TableDef, error) {
	d, ok := tx.Def(t)
	if !ok {
		return nil, &types.Error{Code: types.ErrRefused, Table: t, Msg: "table is not in the catalog of record"}
	}
	return &d, nil
}

// hasSource reports whether a table reads from the Source.
func hasSource(d *types.TableDef, view *delta.Table) bool {
	if d.Kind != types.Mirrored || d.Source == nil {
		return false
	}
	if view != nil && view.State.Detached {
		return false
	}
	return true
}

// identityImage returns a Delta row's image for index idx.
func indexImage(r *delta.DeltaRow, idx types.IndexID) types.KeyImage {
	if int(idx) < len(r.Images) && r.Images[idx] != nil {
		return r.Images[idx]
	}
	if idx == 0 {
		return r.Image
	}
	return nil
}

func deltaRef(r *delta.DeltaRow, whole bool) types.RowRef {
	ref := types.RowRef{Origin: types.OriginDelta, PK: types.Key{Image: r.Image}, SourceKey: r.SourceKey}
	if whole {
		ref.Whole = r.Row
	}
	return ref
}

// interval locks every scanned range of a locking scan (next-key locks over
// the merged index, stored as one interval per range, D-69). An index whose
// order is not proven gets a whole-index lock (D-64).
func intervalLocks(tx *txn.Tx, d *types.TableDef, req types.ScanRequest) error {
	if req.Lock == types.LockNone || !tx.Gaps() {
		return nil
	}
	ix := d.Index(req.Index)
	ranges := req.Ranges
	if len(ranges) == 0 || (ix != nil && !ix.OrderProven) {
		ranges = []types.KeyRange{{}}
	}
	for _, r := range ranges {
		if err := tx.Lock(lock.Request{Table: d.ID, Index: req.Index, Kind: lock.Interval, Mode: req.Lock, Range: r.Images}); err != nil {
			return err
		}
	}
	return nil
}

func project(rows []types.Row, cols types.ColumnSet) []types.Row {
	if cols == nil {
		return rows
	}
	out := make([]types.Row, len(rows))
	for i, r := range rows {
		out[i] = types.Project(r, cols)
	}
	return out
}

func batchBytes(rows []types.Row) int {
	n := 0
	for _, r := range rows {
		n += types.RowSize(r)
	}
	return n
}
