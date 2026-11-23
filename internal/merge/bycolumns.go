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

	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/txn"
	"github.com/virtual-db/vdb-core/internal/types"
)

// ByColumns returns a cursor over the merged rows of table t whose columns
// cols equal values, with the columns want (nil: every column). The Delta is
// read through index idx at image img, compared as a prefix; a nil img skips
// the Delta side. Then the Source is read through its cursor, one batch per
// Next (Source scaling plan K3), so a parent with many children is never held
// whole; a RESTRICT check can stop after the first row. With locks, each row
// is locked through the engine's hook and replaced by its latest committed
// version, and rows deleted since the snapshot are dropped.
func (m *Merge) ByColumns(tx *txn.Tx, t types.TableID, idx types.IndexID, img types.KeyImage, cols types.ColumnSet,
	values types.Row, want types.ColumnSet, lk *types.ReadLocks) (types.Cursor, error) {
	d, err := def(tx, t)
	if err != nil {
		return nil, err
	}
	if d.Kind == types.Provided {
		return nil, &types.Error{Code: types.ErrRefused, Table: t, Msg: "provided tables are read by scans only"}
	}
	view, err := tx.View(t)
	if err != nil {
		return nil, err
	}
	c := &byColumnsCursor{m: m, tx: tx, d: d, view: view, cols: cols, values: values, want: want, lk: lk,
		whole: d.Identity == types.WholeRow, seen: map[string]bool{}}
	if view != nil && img != nil {
		r := types.ImageRange{Low: img, High: img, LowIncl: true, HighIncl: true}
		if idx == d.Identity && !c.whole {
			c.it = view.RowsInRange(r, types.Forward)
		} else {
			c.it = view.IndexInRange(idx, r, types.Forward)
		}
	}
	c.source = hasSource(d, view)
	return c, nil
}

type byColumnsCursor struct {
	m      *Merge
	tx     *txn.Tx
	d      *types.TableDef
	view   *delta.Table
	cols   types.ColumnSet
	values types.Row
	want   types.ColumnSet
	lk     *types.ReadLocks
	whole  bool
	seen   map[string]bool // Source keys the Delta already answered
	it     *delta.Iter
	source bool
	sc     types.SourceCursor
	done   bool
}

func (c *byColumnsCursor) Close() error {
	if c.it != nil {
		c.it.Close()
		c.it = nil
	}
	if c.sc != nil {
		_ = c.sc.Close()
		c.sc = nil
	}
	c.done = true
	return nil
}

func (c *byColumnsCursor) Next(maxRows, maxBytes int) (types.Batch, error) {
	if maxRows <= 0 {
		maxRows = c.m.cfg.KeyBatch
	}
	var out types.Batch
	for c.it != nil && len(out.Rows) < maxRows {
		dr := c.it.Next()
		if dr == nil {
			c.it.Close()
			c.it = nil
			break
		}
		row := dr
		if c.lk != nil && c.lk.Row != nil {
			if err := c.lk.Row(c.d.ID, types.LockKey(dr.SourceKey, dr.Image)); err != nil {
				c.Close()
				return types.Batch{}, err
			}
			latest, _ := c.tx.Latest(c.d.ID, dr.Image, nil)
			if latest == nil {
				continue
			}
			row = latest
			if err := probe(c.lk, c.d.ID, row.Row, nil); err != nil {
				c.Close()
				return types.Batch{}, err
			}
		}
		out.Rows = append(out.Rows, types.Project(row.Row, c.want))
		out.Refs = append(out.Refs, deltaRef(row, c.whole))
		if row.SourceKey != nil {
			c.seen[string(row.SourceKey)] = true
		}
	}
	if len(out.Rows) > 0 {
		return out, nil
	}
	for !c.done && c.source {
		if c.sc == nil {
			snap, err := c.tx.Source()
			if err != nil {
				c.Close()
				return types.Batch{}, err
			}
			sc, err := snap.ByColumns(c.d.ID, c.cols, []types.Row{c.values}, c.want)
			if err != nil {
				c.Close()
				return types.Batch{}, &types.Error{Code: types.ErrSource, Table: c.d.ID, Msg: err.Error()}
			}
			c.sc = sc
		}
		sr, err := c.sc.Next(maxRows)
		if err != nil && err != io.EOF {
			c.Close()
			return types.Batch{}, &types.Error{Code: types.ErrSource, Table: c.d.ID, Msg: err.Error()}
		}
		if err == io.EOF || len(sr.Rows) == 0 {
			c.done = true
		}
		if len(sr.Rows) == 0 {
			break
		}
		b, _, ferr := c.m.filterSourceIdx(c.tx, c.d, c.view, sr, c.want, c.lk, c.whole, nil)
		if ferr != nil {
			c.Close()
			return types.Batch{}, ferr
		}
		for i := range b.Rows {
			if k := b.Refs[i].SourceKey; k != nil && c.seen[string(k)] {
				continue
			}
			out.Rows = append(out.Rows, b.Rows[i])
			out.Refs = append(out.Refs, b.Refs[i])
		}
		if len(out.Rows) > 0 {
			return out, nil
		}
	}
	c.Close()
	return types.Batch{}, io.EOF
}
