// Package write implements writes into a transaction's draft (implementation
// plan doc 05 §2, product doc 10): inserts with uniqueness checks against the
// Delta and the Source, updates, deletes, truncate, refusals, foreign-key
// checks through package fk, and the memory check on every growing change.
package write

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync/atomic"
	"time"

	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/fk"
	"github.com/virtual-db/vdb-core/internal/lock"
	"github.com/virtual-db/vdb-core/internal/merge"
	"github.com/virtual-db/vdb-core/internal/txn"
	"github.com/virtual-db/vdb-core/internal/types"
)

// Reserver is the memory guard.
type Reserver interface {
	Reserve(n int, kind string) error
}

// Writer performs writes. One per environment; safe for concurrent use.
type Writer struct {
	mem Reserver
	fk  *fk.Checker
}

// rowIDs hands out synthetic identities for Delta rows of tables without a
// usable key. Seeded from the clock so restored rows never collide.
var rowIDs atomic.Uint64

func init() { rowIDs.Store(uint64(time.Now().UnixNano())) }

func syntheticImage() types.KeyImage {
	b := make([]byte, 9)
	b[0] = 'r'
	binary.BigEndian.PutUint64(b[1:], rowIDs.Add(1))
	return b
}

// New returns a Writer.
func New(mem Reserver, m *merge.Merge) *Writer {
	w := &Writer{mem: mem}
	w.fk = &fk.Checker{W: w, Merge: m}
	return w
}

// FK returns the foreign-key checker.
func (w *Writer) FK() *fk.Checker { return w.fk }

func def(tx *txn.Tx, t types.TableID) (*types.TableDef, error) {
	d, ok := tx.Def(t)
	if !ok {
		return nil, &types.Error{Code: types.ErrRefused, Table: t, Msg: "table is not in the catalog of record"}
	}
	if d.Kind == types.Provided {
		return nil, &types.Error{Code: types.ErrRefused, Table: t, Msg: "tables of the vdb schema are read-only"}
	}
	return &d, nil
}

func hasSource(d *types.TableDef, view *delta.Table) bool {
	return d.Kind == types.Mirrored && d.Source != nil && (view == nil || !view.State.Detached)
}

func imageAt(images []types.KeyImage, id types.IndexID) types.KeyImage {
	if int(id) < len(images) {
		return images[id]
	}
	return nil
}

func uniqueLockKey(idx types.IndexID, img types.KeyImage) types.KeyImage {
	k := types.KeyImage{'u', byte(idx >> 8), byte(idx)}
	return append(k, img...)
}

func nullIn(row types.Row, cols []uint16) bool {
	for _, c := range cols {
		if int(c) >= len(row) || row[c].Null {
			return true
		}
	}
	return false
}

func project(row types.Row, cols []uint16) []types.Value {
	out := make([]types.Value, len(cols))
	for i, c := range cols {
		if int(c) < len(row) {
			out[i] = row[c]
		}
	}
	return out
}

func (w *Writer) reserve(n int) error {
	if w.mem == nil {
		return nil
	}
	return w.mem.Reserve(n, "delta")
}

// Insert inserts rows. images[i] holds the sort image of every index of row
// i, by IndexID. Results are per row, in order, so mysqld sees each duplicate
// at the row where it occurs.
func (w *Writer) Insert(tx *txn.Tx, t types.TableID, rows []types.Row, images [][]types.KeyImage) ([]types.WriteResult, error) {
	d, err := def(tx, t)
	if err != nil {
		return nil, err
	}
	view, err := tx.View(t)
	if err != nil {
		return nil, err
	}
	whole := d.Identity == types.WholeRow
	results := make([]types.WriteResult, len(rows))

	// Source check of the identity key, batched over every row of the call.
	srcDup := make([]bool, len(rows))
	if hasSource(d, view) && !whole {
		cols := d.IdentityColumns()
		var keys []types.SourceKey
		var at []int
		for i, r := range rows {
			if nullIn(r, cols) {
				continue
			}
			keys = append(keys, types.EncodeSourceKey(project(r, cols)))
			at = append(at, i)
		}
		if len(keys) > 0 {
			snap, err := tx.Source()
			if err != nil {
				return nil, err
			}
			ex, err := snap.Exists(t, keys)
			if err != nil {
				return nil, &types.Error{Code: types.ErrSource, Table: t, Msg: err.Error()}
			}
			for j, e := range ex {
				if !e {
					continue
				}
				_, touchedView := false, false
				if view != nil {
					_, touchedView = view.IsTouched(keys[j])
				}
				_, touchedLatest := tx.Latest(t, nil, keys[j])
				if !touchedView && !touchedLatest {
					srcDup[at[j]] = true
				}
			}
		}
	}

	full := false
	for i, row := range rows {
		if full {
			results[i] = types.WriteResult{Err: &types.Error{Code: types.ErrDeltaFull, Table: t, Msg: "the delta memory limit is reached"}}
			continue
		}
		var ims []types.KeyImage
		if i < len(images) {
			ims = images[i]
		}
		if e := w.insertOne(tx, d, row, ims, srcDup[i], 0); e != nil {
			ce := types.AsError(e)
			if ce.Code == types.ErrLockWaitTimeout || ce.Code == types.ErrDeadlock || ce.Code == types.ErrKilled ||
				ce.Code == types.ErrSource || ce.Code == types.ErrInternal || ce.Code == types.ErrLockMemory {
				return results, ce
			}
			if ce.Code == types.ErrDeltaFull {
				full = true
			}
			results[i] = types.WriteResult{Err: ce, Index: ce.Index}
		}
	}
	return results, nil
}

func (w *Writer) insertOne(tx *txn.Tx, d *types.TableDef, row types.Row, ims []types.KeyImage, srcDup bool, depth int) error {
	t := d.ID
	whole := d.Identity == types.WholeRow
	var pk types.KeyImage
	if whole {
		pk = syntheticImage()
	} else {
		pk = imageAt(ims, d.Identity)
		if pk == nil {
			return &types.Error{Code: types.ErrInternal, Table: t, Msg: "insert without an identity key image"}
		}
	}
	// Locks: insert intention and a record lock on every unique key (D-64).
	for _, ix := range d.Indexes {
		if !(ix.Unique || ix.Primary) {
			continue
		}
		img := imageAt(ims, ix.ID)
		if img == nil || nullIn(row, ix.Columns) {
			continue
		}
		if err := tx.Lock(lock.Request{Table: t, Index: ix.ID, Kind: lock.InsertIntention, Mode: types.LockExclusive, Key: img}); err != nil {
			return err
		}
		key := uniqueLockKey(ix.ID, img)
		if ix.ID == d.Identity && !whole {
			key = merge.LockKey(nil, img)
		}
		if err := tx.Lock(lock.Request{Table: t, Kind: lock.Record, Mode: types.LockExclusive, Key: key}); err != nil {
			return err
		}
	}
	// Non-unique indexes still wait for gap locks covering the new entry.
	for _, ix := range d.Indexes {
		if ix.Unique || ix.Primary {
			continue
		}
		if img := imageAt(ims, ix.ID); img != nil {
			if err := tx.Lock(lock.Request{Table: t, Index: ix.ID, Kind: lock.InsertIntention, Mode: types.LockExclusive, Key: img}); err != nil {
				return err
			}
		}
	}
	if err := w.checkUnique(tx, d, row, ims, nil, nil, srcDup); err != nil {
		return err
	}
	if tx.Ctx.ForeignKeyChecks {
		for _, f := range d.ForeignKeys {
			if err := w.fk.CheckParent(tx, d, f, row, ims); err != nil {
				return err
			}
		}
	}
	dr := &delta.DeltaRow{Image: pk, Row: types.CloneRow(row), Origin: types.OriginDelta, Images: cloneImages(ims, pk, d)}
	if err := w.reserve(types.RowSize(row) + 64*len(ims)); err != nil {
		return err
	}
	return tx.Apply(delta.Op{Kind: delta.OpPutRow, Table: t, Row: dr, TKind: d.Kind, Version: d.Version})
}

func cloneImages(ims []types.KeyImage, pk types.KeyImage, d *types.TableDef) []types.KeyImage {
	out := make([]types.KeyImage, len(ims))
	for i, im := range ims {
		if im != nil {
			out[i] = append(types.KeyImage(nil), im...)
		}
	}
	if d.Identity != types.WholeRow && int(d.Identity) < len(out) {
		out[d.Identity] = pk
	}
	return out
}

// checkUnique checks every unique key of a new row version against the
// Delta (the snapshot view and the latest committed state) and the Source.
// selfImg and selfKey exclude the row being updated.
func (w *Writer) checkUnique(tx *txn.Tx, d *types.TableDef, row types.Row, ims []types.KeyImage, selfImg types.KeyImage, selfKey types.SourceKey, srcDupPK bool) error {
	t := d.ID
	view, err := tx.View(t)
	if err != nil {
		return err
	}
	latest := tx.Manager().Store().Snapshot().Table(t)
	whole := d.Identity == types.WholeRow
	for _, ix := range d.Indexes {
		if !(ix.Unique || ix.Primary) || nullIn(row, ix.Columns) {
			continue
		}
		img := imageAt(ims, ix.ID)
		dup := &types.Error{Code: types.ErrDuplicateKey, Table: t, Index: ix.ID, Msg: "Duplicate entry for key '" + ix.Name + "'"}
		if img != nil {
			if ix.ID == d.Identity && !whole {
				if !bytes.Equal(img, selfImg) {
					if view != nil && view.Holds(img) {
						return dup
					}
					if r, _ := tx.Latest(t, img, nil); r != nil {
						return dup
					}
				}
			} else {
				if view != nil {
					if _, ok := view.IndexHas(ix.ID, img, selfImg); ok {
						return dup
					}
				}
				if latest != nil {
					if pk, ok := latest.IndexHas(ix.ID, img, selfImg); ok {
						if r, _ := tx.Latest(t, pk, nil); r != nil {
							return dup
						}
					}
				}
			}
		}
		if !hasSource(d, view) {
			continue
		}
		if ix.ID == d.Identity && !whole {
			if selfKey == nil && srcDupPK {
				return dup
			}
			if selfKey != nil {
				// An update that changes the identity key: ask the Source.
				k := types.EncodeSourceKey(project(row, ix.Columns))
				if bytes.Equal(k, selfKey) {
					continue
				}
				if taken, err := w.sourceHas(tx, d, view, ix, row, selfKey); err != nil {
					return err
				} else if taken {
					return dup
				}
			}
			continue
		}
		taken, err := w.sourceHas(tx, d, view, ix, row, selfKey)
		if err != nil {
			return err
		}
		if taken {
			return dup
		}
	}
	return nil
}

// sourceHas reports whether a Source row other than selfKey, not touched,
// holds the values of index ix.
func (w *Writer) sourceHas(tx *txn.Tx, d *types.TableDef, view *delta.Table, ix types.IndexDef, row types.Row, selfKey types.SourceKey) (bool, error) {
	snap, err := tx.Source()
	if err != nil {
		return false, err
	}
	cur, err := snap.ByColumns(d.ID, ix.Columns, []types.Row{project(row, ix.Columns)}, nil)
	if err != nil {
		return false, &types.Error{Code: types.ErrSource, Table: d.ID, Msg: err.Error()}
	}
	defer cur.Close()
	for {
		sr, err := cur.Next(100)
		for i := range sr.Rows {
			var key types.SourceKey
			if i < len(sr.Keys) {
				key = sr.Keys[i]
			}
			if selfKey != nil && bytes.Equal(key, selfKey) {
				continue
			}
			if view != nil {
				if _, t := view.IsTouched(key); t {
					continue
				}
			}
			if _, t := tx.Latest(d.ID, nil, key); t {
				continue
			}
			return true, nil
		}
		if err == io.EOF || len(sr.Rows) == 0 {
			return false, nil
		}
		if err != nil {
			return false, &types.Error{Code: types.ErrSource, Table: d.ID, Msg: err.Error()}
		}
	}
}

func equalityProven(d *types.TableDef) bool {
	if d.Identity == types.WholeRow {
		return true
	}
	ix := d.Index(d.Identity)
	return ix == nil || ix.EqualityProven
}

func refusedCollation(d *types.TableDef) error {
	return &types.Error{Code: types.ErrRefused, Table: d.ID,
		Msg: "table " + d.Database + "." + d.Name + " has a primary key in a collation not proven for equality; updates and deletes of Source rows are refused (D-62)"}
}

// oldRowOf returns the full old row: the Delta row's, or the one the engine
// sent in RowRef.Whole.
func oldRowOf(tx *txn.Tx, t types.TableID, old types.RowRef) (*delta.DeltaRow, types.Row) {
	if old.Origin == types.OriginDelta {
		if r, _ := tx.Latest(t, old.PK.Image, nil); r != nil {
			return r, r.Row
		}
		if v, err := tx.View(t); err == nil && v != nil {
			if r, ok := v.Get(old.PK.Image); ok {
				return r, r.Row
			}
		}
	}
	return nil, old.Whole
}

// Update replaces a row (doc 05 §2.2).
func (w *Writer) Update(tx *txn.Tx, t types.TableID, old types.RowRef, row types.Row, images []types.KeyImage) error {
	return w.UpdateRow(tx, t, old, nil, row, images, 0)
}

// UpdateRow is Update with the old row known (cascades) and a cascade depth.
func (w *Writer) UpdateRow(tx *txn.Tx, t types.TableID, old types.RowRef, oldRow, row types.Row, images []types.KeyImage, depth int) error {
	d, err := def(tx, t)
	if err != nil {
		return err
	}
	whole := d.Identity == types.WholeRow
	if old.Origin == types.OriginSource && !equalityProven(d) {
		return refusedCollation(d)
	}
	if err := merge.LockRow(tx, t, old.SourceKey, old.PK.Image, types.LockExclusive); err != nil {
		return err
	}
	prev, prevRow := oldRowOf(tx, t, old)
	if oldRow == nil {
		oldRow = prevRow
	}
	var newImg types.KeyImage
	if whole {
		if prev != nil {
			newImg = prev.Image
		} else {
			newImg = syntheticImage()
		}
	} else {
		newImg = imageAt(images, d.Identity)
		if newImg == nil {
			return &types.Error{Code: types.ErrInternal, Table: t, Msg: "update without an identity key image"}
		}
	}
	selfImg := old.PK.Image
	if prev != nil {
		selfImg = prev.Image
	}
	if !whole && !bytes.Equal(newImg, selfImg) {
		if err := merge.LockRow(tx, t, nil, newImg, types.LockExclusive); err != nil {
			return err
		}
	}
	if err := w.checkUnique(tx, d, row, images, selfImg, old.SourceKey, false); err != nil {
		return err
	}
	if tx.Ctx.ForeignKeyChecks || depth > 0 {
		for _, f := range d.ForeignKeys {
			if oldRow != nil {
				ov, _ := valuesOf(oldRow, f.Columns)
				nv, _ := valuesOf(row, f.Columns)
				if sameValues(ov, nv) {
					continue
				}
			}
			if err := w.fk.CheckParent(tx, d, f, row, images); err != nil {
				return err
			}
		}
		if oldRow != nil {
			if err := w.fk.OnParentChange(tx, d, oldRow, row, depth); err != nil {
				return err
			}
		}
	}
	if err := w.reserve(types.RowSize(row) + 64*len(images)); err != nil {
		return err
	}
	nr := &delta.DeltaRow{Image: newImg, Row: types.CloneRow(row), Origin: types.OriginDelta, Images: cloneImages(images, newImg, d)}
	if old.Origin == types.OriginSource {
		touch := delta.Op{Kind: delta.OpTouch, Table: t, Key: old.SourceKey, Image: old.PK.Image, Whole: whole, Delta: 1, TKind: d.Kind, Version: d.Version}
		if whole {
			touch.Image = nil
			touch.Key = types.EncodeSourceKey(old.Whole)
		}
		if err := tx.Apply(touch); err != nil {
			return err
		}
		nr.Origin, nr.SourceKey = types.OriginSource, touch.Key
	} else if prev != nil {
		nr.Origin, nr.SourceKey = prev.Origin, prev.SourceKey
		if !bytes.Equal(prev.Image, newImg) {
			if err := tx.Apply(delta.Op{Kind: delta.OpDelRow, Table: t, Image: prev.Image}); err != nil {
				return err
			}
		}
	}
	return tx.Apply(delta.Op{Kind: delta.OpPutRow, Table: t, Row: nr, TKind: d.Kind, Version: d.Version})
}

func valuesOf(row types.Row, cols []uint16) ([]types.Value, bool) {
	if row == nil {
		return nil, false
	}
	return project(row, cols), true
}

func sameValues(a, b []types.Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Null != b[i].Null || !bytes.Equal(a[i].B, b[i].B) {
			return false
		}
	}
	return true
}

// Delete removes a row (doc 05 §2.2).
func (w *Writer) Delete(tx *txn.Tx, t types.TableID, old types.RowRef) error {
	return w.DeleteRow(tx, t, old, nil, 0)
}

// DeleteRow is Delete with the old row known (cascades) and a cascade depth.
func (w *Writer) DeleteRow(tx *txn.Tx, t types.TableID, old types.RowRef, oldRow types.Row, depth int) error {
	d, err := def(tx, t)
	if err != nil {
		return err
	}
	whole := d.Identity == types.WholeRow
	if old.Origin == types.OriginSource && !equalityProven(d) {
		return refusedCollation(d)
	}
	if err := merge.LockRow(tx, t, old.SourceKey, old.PK.Image, types.LockExclusive); err != nil {
		return err
	}
	prev, prevRow := oldRowOf(tx, t, old)
	if oldRow == nil {
		oldRow = prevRow
	}
	if (tx.Ctx.ForeignKeyChecks || depth > 0) && oldRow != nil {
		if err := w.fk.OnParentChange(tx, d, oldRow, nil, depth); err != nil {
			return err
		}
	}
	if old.Origin == types.OriginSource {
		touch := delta.Op{Kind: delta.OpTouch, Table: t, Key: old.SourceKey, Image: old.PK.Image, Whole: whole, Delta: 1, TKind: d.Kind, Version: d.Version}
		if whole {
			touch.Image = nil
			touch.Key = types.EncodeSourceKey(old.Whole)
		}
		if err := w.reserve(len(touch.Key) + 64); err != nil {
			return err
		}
		return tx.Apply(touch)
	}
	img := old.PK.Image
	if prev != nil {
		img = prev.Image
	}
	return tx.Apply(delta.Op{Kind: delta.OpDelRow, Table: t, Image: img, TKind: d.Kind, Version: d.Version})
}

// Truncate detaches a mirrored table and clears its Delta rows and touched
// keys; the change is recorded in the change set (doc 05 §2.3).
func (w *Writer) Truncate(tx *txn.Tx, t types.TableID) error {
	d, err := def(tx, t)
	if err != nil {
		return err
	}
	if err := tx.Apply(delta.Op{Kind: delta.OpTruncate, Table: t, TKind: d.Kind, Version: d.Version}); err != nil {
		return err
	}
	if d.Kind == types.Mirrored {
		tx.Stage(types.CatalogChange{Kind: types.DetachTable, ID: t})
	}
	return nil
}
