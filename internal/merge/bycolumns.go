// Package merge — bycolumns.go
//
// Reads by column values (implementation plan doc 05 §1.4): every merged row
// whose columns equal given values. The engine uses it to find a parent row
// or the children of a parent (foreign keys) and for uniqueness on indexes it
// added. The Delta is read through an index at a key image, the Source by
// value, so equality on both sides follows the engine's collation.
package merge

import (
	"io"

	"github.com/virtual-db/vdb-core/internal/txn"
	"github.com/virtual-db/vdb-core/internal/types"
)

// ByColumns returns the merged rows of table t whose columns cols equal values,
// with the columns want (nil: every column). The Delta is read through index
// idx at image img, compared as a prefix; a nil img skips the Delta side.
// With locks, each row is locked through the engine's hook and replaced by its
// latest committed version, and rows deleted since the snapshot are dropped.
func (m *Merge) ByColumns(tx *txn.Tx, t types.TableID, idx types.IndexID, img types.KeyImage, cols types.ColumnSet,
	values types.Row, want types.ColumnSet, lk *types.ReadLocks) (types.Batch, error) {
	d, err := def(tx, t)
	if err != nil {
		return types.Batch{}, err
	}
	if d.Kind == types.Provided {
		return types.Batch{}, &types.Error{Code: types.ErrRefused, Table: t, Msg: "provided tables are read by scans only"}
	}
	view, err := tx.View(t)
	if err != nil {
		return types.Batch{}, err
	}
	whole := d.Identity == types.WholeRow
	var out types.Batch
	seen := map[string]bool{}
	if view != nil && img != nil {
		r := types.ImageRange{Low: img, High: img, LowIncl: true, HighIncl: true}
		it := view.IndexInRange(idx, r, types.Forward)
		if idx == d.Identity && !whole {
			it = view.RowsInRange(r, types.Forward)
		}
		for dr := it.Next(); dr != nil; dr = it.Next() {
			row := dr
			if lk != nil && lk.Row != nil {
				if err := lk.Row(t, types.LockKey(dr.SourceKey, dr.Image)); err != nil {
					it.Close()
					return out, err
				}
				latest, _ := tx.Latest(t, dr.Image, nil)
				if latest == nil {
					continue
				}
				row = latest
				if err := probe(lk, t, row.Row, nil); err != nil {
					it.Close()
					return out, err
				}
			}
			out.Rows = append(out.Rows, types.Project(row.Row, want))
			out.Refs = append(out.Refs, deltaRef(row, whole))
			if row.SourceKey != nil {
				seen[string(row.SourceKey)] = true
			}
		}
		it.Close()
	}
	if !hasSource(d, view) {
		return out, nil
	}
	snap, err := tx.Source()
	if err != nil {
		return out, err
	}
	cur, err := snap.ByColumns(t, cols, []types.Row{values}, want)
	if err != nil {
		return out, &types.Error{Code: types.ErrSource, Table: t, Msg: err.Error()}
	}
	defer cur.Close()
	for {
		sr, err := cur.Next(m.cfg.KeyBatch)
		if len(sr.Rows) > 0 {
			b, _, ferr := m.filterSourceIdx(tx, d, view, sr, want, lk, whole, nil)
			if ferr != nil {
				return out, ferr
			}
			for i := range b.Rows {
				if k := b.Refs[i].SourceKey; k != nil && seen[string(k)] {
					continue
				}
				out.Rows = append(out.Rows, b.Rows[i])
				out.Refs = append(out.Refs, b.Refs[i])
			}
		}
		if err == io.EOF || len(sr.Rows) == 0 {
			return out, nil
		}
		if err != nil {
			return out, &types.Error{Code: types.ErrSource, Table: t, Msg: err.Error()}
		}
	}
}
