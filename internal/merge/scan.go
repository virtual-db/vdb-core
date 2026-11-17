// Package merge — scan.go
//
// Unordered scans (doc 05 §1.1) and the two cursors of ordered scans (§1.2).
package merge

import (
	"io"

	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/txn"
	"github.com/virtual-db/vdb-core/internal/types"
)

// Handle is one ScanOpen.
type Handle struct {
	merged  types.Cursor
	source  types.Cursor
	delta   types.Cursor
	shadows []types.KeyImage
}

// Source returns the Source cursor of an ordered scan.
func (h *Handle) Source() types.Cursor { return h.source }

// Delta returns the Delta cursor of an ordered scan.
func (h *Handle) Delta() types.Cursor { return h.delta }

// Merged returns the merged cursor of an unordered scan.
func (h *Handle) Merged() types.Cursor { return h.merged }

// ShadowImages returns the images of Delta keys that can hide a Source row.
func (h *Handle) ShadowImages() []types.KeyImage { return h.shadows }

// Close closes every cursor.
func (h *Handle) Close() error {
	for _, c := range []types.Cursor{h.merged, h.source, h.delta} {
		if c != nil {
			_ = c.Close()
		}
	}
	return nil
}

// Scan opens a scan: unordered or ordered, over a mirrored, local, detached
// or provided table.
func (m *Merge) Scan(tx *txn.Tx, req types.ScanRequest) (*Handle, error) {
	d, err := def(tx, req.Table)
	if err != nil {
		return nil, err
	}
	if d.Kind == types.Provided {
		p, ok := tx.Manager().Catalog().Provider(d.ID)
		if !ok {
			return nil, &types.Error{Code: types.ErrInternal, Table: d.ID, Msg: "provided table has no provider"}
		}
		c, err := p.Rows(tx.Info, req.Columns)
		if err != nil {
			return nil, types.AsError(err)
		}
		if req.Ordered {
			return &Handle{source: &emptyCursor{}, delta: c}, nil
		}
		return &Handle{merged: c}, nil
	}
	view, err := tx.View(d.ID)
	if err != nil {
		return nil, err
	}
	if err := intervalLocks(tx, d, req); err != nil {
		return nil, err
	}
	h := &Handle{}
	var sc types.Cursor = &emptyCursor{}
	if hasSource(d, view) {
		snap, err := tx.Source()
		if err != nil {
			return nil, err
		}
		cur, err := snap.Scan(types.SourceScan{Table: d.ID, Index: req.Index, Ranges: req.Ranges, Ordered: req.Ordered,
			Dir: req.Dir, Columns: req.Columns, Pushed: req.Pushed})
		if err != nil {
			return nil, &types.Error{Code: types.ErrSource, Table: d.ID, Msg: err.Error()}
		}
		sc = &sourceCursor{m: m, tx: tx, d: d, view: view, sc: cur, cols: req.Columns, lock: req.Lock,
			merged: req.Ordered, dropped: map[string]uint32{}}
		if view != nil && view.Len()+int(view.State.TouchedN) > 0 {
			h.shadows = view.ShadowImages()
		}
	}
	dc := &deltaCursor{m: m, tx: tx, d: d, view: view, req: req, merged: req.Ordered}
	if req.Ordered {
		h.source, h.delta = sc, dc
	} else {
		h.merged = &chain{m: m, tx: tx, d: d, cols: req.Columns, parts: []types.Cursor{sc, dc}}
	}
	return h, nil
}

type emptyCursor struct{}

func (emptyCursor) Next(int, int) (types.Batch, error) { return types.Batch{}, io.EOF }
func (emptyCursor) Close() error                       { return nil }

// chain yields the Source cursor's rows, then the Delta cursor's, running
// vdb.records.merged on each batch.
type chain struct {
	m     *Merge
	tx    *txn.Tx
	d     *types.TableDef
	cols  types.ColumnSet
	parts []types.Cursor
	i     int
}

func (c *chain) Next(maxRows, maxBytes int) (types.Batch, error) {
	for c.i < len(c.parts) {
		b, err := c.parts[c.i].Next(maxRows, maxBytes)
		if err == io.EOF {
			c.i++
			if len(b.Rows) == 0 {
				continue
			}
		} else if err != nil {
			return types.Batch{}, err
		}
		if len(b.Rows) == 0 {
			continue
		}
		return c.m.mergedHook(c.tx, c.d, c.cols, b)
	}
	return types.Batch{}, io.EOF
}

func (c *chain) Close() error {
	for _, p := range c.parts {
		_ = p.Close()
	}
	return nil
}

func (m *Merge) mergedHook(tx *txn.Tx, d *types.TableDef, cols types.ColumnSet, b types.Batch) (types.Batch, error) {
	if m.hooks.Merged == nil || len(b.Rows) == 0 {
		return b, nil
	}
	rows, err := m.hooks.Merged(tx, d, cols, b.Rows)
	if err != nil {
		return types.Batch{}, types.AsError(err)
	}
	if len(rows) != len(b.Rows) {
		return types.Batch{}, &types.Error{Code: types.ErrInternal, Table: d.ID, Msg: "a vdb.records.merged handler changed the number of rows"}
	}
	b.Rows = rows
	return b, nil
}

// sourceCursor yields Source rows with touched and held keys removed.
type sourceCursor struct {
	m       *Merge
	tx      *txn.Tx
	d       *types.TableDef
	view    *delta.Table
	sc      types.SourceCursor
	cols    types.ColumnSet
	lock    types.LockMode
	merged  bool // run vdb.records.merged here (ordered scans)
	dropped map[string]uint32
	done    bool
}

func (c *sourceCursor) Next(maxRows, maxBytes int) (types.Batch, error) {
	whole := c.d.Identity == types.WholeRow
	for !c.done {
		sr, err := c.sc.Next(maxRows)
		if err == io.EOF {
			c.done = true
		} else if err != nil {
			return types.Batch{}, &types.Error{Code: types.ErrSource, Table: c.d.ID, Msg: err.Error()}
		}
		if len(sr.Rows) == 0 {
			continue
		}
		b, err := c.m.filterSource(c.tx, c.d, c.view, sr, c.cols, c.lock, whole, c.dropped)
		if err != nil {
			return types.Batch{}, err
		}
		if len(b.Rows) == 0 {
			continue
		}
		if c.merged {
			return c.m.mergedHook(c.tx, c.d, c.cols, b)
		}
		return b, nil
	}
	return types.Batch{}, io.EOF
}

func (c *sourceCursor) Close() error { return c.sc.Close() }

// filterSource runs vdb.records.source on a Source batch, then drops rows the
// Delta hides. With a lock mode, each kept row is locked and replaced by its
// latest committed version (product doc 05 §3.2).
func (m *Merge) filterSource(tx *txn.Tx, d *types.TableDef, view *delta.Table, sr types.SourceRows, cols types.ColumnSet,
	mode types.LockMode, whole bool, dropped map[string]uint32) (types.Batch, error) {
	b, _, err := m.filterSourceIdx(tx, d, view, sr, cols, mode, whole, dropped)
	return b, err
}

// filterSourceIdx is filterSource that also returns, for each kept row, its
// position in the Source batch.
func (m *Merge) filterSourceIdx(tx *txn.Tx, d *types.TableDef, view *delta.Table, sr types.SourceRows, cols types.ColumnSet,
	mode types.LockMode, whole bool, dropped map[string]uint32) (types.Batch, []int, error) {
	var orig []int
	rows := sr.Rows
	if m.hooks.Source != nil {
		var err error
		rows, err = m.hooks.Source(tx, d, cols, rows)
		if err != nil {
			return types.Batch{}, nil, types.AsError(err)
		}
		if len(rows) != len(sr.Rows) {
			return types.Batch{}, nil, &types.Error{Code: types.ErrInternal, Table: d.ID, Msg: "a vdb.records.source handler changed the number of rows"}
		}
	}
	var b types.Batch
	for i, row := range rows {
		var key types.SourceKey
		if i < len(sr.Keys) {
			key = sr.Keys[i]
		}
		var img types.KeyImage
		if i < len(sr.Images) {
			img = sr.Images[i]
		}
		if view != nil {
			if n, ok := view.IsTouched(key); ok {
				if !whole {
					continue
				}
				if dropped != nil && dropped[string(key)] < n {
					dropped[string(key)]++
					continue
				}
			}
			if img != nil && view.Holds(img) {
				if m.hooks.KeyConflict != nil {
					m.hooks.KeyConflict(d.ID, img)
				}
				continue
			}
		}
		ref := types.RowRef{Origin: types.OriginSource, SourceKey: key, PK: types.Key{Image: img}}
		if whole {
			ref.Whole = row
		}
		if mode != types.LockNone {
			if err := LockRow(tx, d.ID, key, img, mode); err != nil {
				return types.Batch{}, nil, err
			}
			if _, touched := tx.Latest(d.ID, nil, key); touched && !whole {
				// Another transaction committed a change to this row after our snapshot.
				latest := tx.Manager().Store().Snapshot().Table(d.ID)
				if latest == nil {
					continue
				}
				dr, ok := latest.FindBySourceKey(key)
				if !ok {
					continue // deleted
				}
				b.Rows = append(b.Rows, types.Project(dr.Row, cols))
				b.Refs = append(b.Refs, deltaRef(dr, false))
				orig = append(orig, i)
				continue
			}
		}
		b.Rows = append(b.Rows, row)
		b.Refs = append(b.Refs, ref)
		orig = append(orig, i)
	}
	return b, orig, nil
}

// deltaCursor yields Delta rows: for unordered scans every row of the table
// within the ranges; for ordered scans each range in image order.
type deltaCursor struct {
	m      *Merge
	tx     *txn.Tx
	d      *types.TableDef
	view   *delta.Table
	req    types.ScanRequest
	merged bool
	its    []*delta.Iter
	cur    int
	opened bool
}

func (c *deltaCursor) open() {
	c.opened = true
	if c.view == nil {
		return
	}
	identity := c.req.Index == c.d.Identity && c.d.Identity != types.WholeRow
	if !c.req.Ordered {
		c.its = []*delta.Iter{c.view.RowsInRange(types.ImageRange{}, types.Forward)}
		return
	}
	ranges := c.req.Ranges
	if len(ranges) == 0 {
		ranges = []types.KeyRange{{}}
	}
	order := make([]types.KeyRange, len(ranges))
	copy(order, ranges)
	if c.req.Dir == types.Backward {
		for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
			order[i], order[j] = order[j], order[i]
		}
	}
	for _, r := range order {
		if identity {
			c.its = append(c.its, c.view.RowsInRange(r.Images, c.req.Dir))
		} else {
			c.its = append(c.its, c.view.IndexInRange(c.req.Index, r.Images, c.req.Dir))
		}
	}
}

func (c *deltaCursor) inRanges(r *delta.DeltaRow) bool {
	if c.req.Ordered || len(c.req.Ranges) == 0 {
		return true
	}
	img := indexImage(r, c.req.Index)
	if img == nil {
		return true
	}
	for _, rg := range c.req.Ranges {
		if delta.InRange(img, rg.Images) {
			return true
		}
	}
	return false
}

func (c *deltaCursor) Next(maxRows, maxBytes int) (types.Batch, error) {
	if !c.opened {
		c.open()
	}
	whole := c.d.Identity == types.WholeRow
	var b types.Batch
	size := 0
	for c.cur < len(c.its) {
		r := c.its[c.cur].Next()
		if r == nil {
			c.cur++
			continue
		}
		if !c.inRanges(r) {
			continue
		}
		if c.req.Lock != types.LockNone {
			if err := LockRow(c.tx, c.d.ID, r.SourceKey, r.Image, c.req.Lock); err != nil {
				return types.Batch{}, err
			}
			latest, _ := c.tx.Latest(c.d.ID, r.Image, nil)
			if latest == nil {
				continue
			}
			r = latest
		}
		row := types.Project(r.Row, c.req.Columns)
		b.Rows = append(b.Rows, row)
		b.Refs = append(b.Refs, deltaRef(r, whole))
		size += types.RowSize(row)
		if (maxRows > 0 && len(b.Rows) >= maxRows) || (maxBytes > 0 && size >= maxBytes) {
			break
		}
	}
	if len(b.Rows) == 0 {
		return b, io.EOF
	}
	if c.merged {
		return c.m.mergedHook(c.tx, c.d, c.req.Columns, b)
	}
	return b, nil
}

func (c *deltaCursor) Close() error {
	for _, it := range c.its {
		it.Close()
	}
	return nil
}
