// Package fk enforces foreign keys over merged rows as InnoDB does
// (implementation plan doc 05 §3, product doc 10 §6, D-61): parent checks
// with a shared lock on the parent, RESTRICT, NO ACTION, CASCADE and SET NULL
// row by row inside the write that triggered them, a depth limit of 15, and
// validation of existing rows for ADD FOREIGN KEY. Cascaded rows get their
// sort images from the engine through the transaction's ImageFunc (D-86).
package fk

import (
	"bytes"
	"io"

	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/merge"
	"github.com/virtual-db/vdb-core/internal/txn"
	"github.com/virtual-db/vdb-core/internal/types"
)

// MaxDepth is InnoDB's cascade depth limit.
const MaxDepth = 15

// Writer applies cascaded changes through the write path.
type Writer interface {
	DeleteRow(tx *txn.Tx, t types.TableID, old types.RowRef, oldRow types.Row, depth int) error
	UpdateRow(tx *txn.Tx, t types.TableID, old types.RowRef, oldRow, row types.Row, images []types.KeyImage, depth int) error
}

// Checker runs foreign-key checks.
type Checker struct {
	W     Writer
	Merge *merge.Merge
}

func values(row types.Row, cols []uint16) ([]types.Value, bool) {
	out := make([]types.Value, len(cols))
	for i, c := range cols {
		if int(c) >= len(row) || row[c].Null {
			return nil, false
		}
		out[i] = row[c]
	}
	return out, true
}

func prefixLen(ix *types.IndexDef, parts int) int {
	if ix == nil || len(ix.PartLens) < parts {
		return -1
	}
	n := 0
	for i := 0; i < parts; i++ {
		n += int(ix.PartLens[i])
	}
	return n
}

func cut(img types.KeyImage, n int) types.KeyImage {
	if n < 0 || n > len(img) {
		return img
	}
	return img[:n]
}

func refused(t types.TableID, name string) error {
	return &types.Error{Code: types.ErrRefused, Table: t, Constraint: name,
		Msg: "foreign key " + name + " uses a collation not proven for equality; set foreign_key_checks=0 to write without the check (D-62)"}
}

// CheckParent checks that a child row's parent exists, taking a shared lock
// on it. A NULL in any foreign-key column skips the check.
func (c *Checker) CheckParent(tx *txn.Tx, child *types.TableDef, fk types.ForeignKeyDef, row types.Row, images []types.KeyImage) error {
	vals, ok := values(row, fk.Columns)
	if !ok {
		return nil
	}
	cat := tx.Manager().Catalog()
	parent, ok := cat.TableByName(fk.ParentDB, fk.ParentTable)
	if !ok {
		return &types.Error{Code: types.ErrNoReferencedRow, Table: child.ID, Constraint: fk.Name, Msg: "parent table is missing"}
	}
	pix := parent.Index(fk.ParentIndex)
	if pix == nil {
		return &types.Error{Code: types.ErrNoReferencedRow, Table: child.ID, Constraint: fk.Name, Msg: "parent key is missing"}
	}
	if !pix.EqualityProven {
		return refused(child.ID, fk.Name)
	}
	var img types.KeyImage
	if int(fk.ChildIndex) < len(images) {
		img = cut(images[fk.ChildIndex], prefixLen(pix, len(pix.Columns)))
	}
	found, err := c.findParent(tx, &parent, pix, vals, img)
	if err != nil {
		return err
	}
	if !found {
		return &types.Error{Code: types.ErrNoReferencedRow, Table: child.ID, Constraint: fk.Name,
			Msg: "Cannot add or update a child row: a foreign key constraint fails"}
	}
	return nil
}

// findParent looks the parent key up in the merged parent table: the Delta by
// image, then the Source with a read that ignores touched keys.
func (c *Checker) findParent(tx *txn.Tx, parent *types.TableDef, pix *types.IndexDef, vals []types.Value, img types.KeyImage) (bool, error) {
	view, err := tx.View(parent.ID)
	if err != nil {
		return false, err
	}
	identity := pix.ID == parent.Identity && parent.Identity != types.WholeRow
	if img != nil {
		var dr *delta.DeltaRow
		if identity {
			if r, _ := tx.Latest(parent.ID, img, nil); r != nil {
				dr = r
			} else if view != nil {
				dr, _ = view.Get(img)
			}
		} else if view != nil {
			if pk, ok := view.IndexHas(pix.ID, img, nil); ok {
				dr, _ = view.Get(pk)
			}
		}
		if dr != nil {
			if err := merge.LockRow(tx, parent.ID, dr.SourceKey, dr.Image, types.LockShared); err != nil {
				return false, err
			}
			if latest, _ := tx.Latest(parent.ID, dr.Image, nil); latest != nil {
				return true, nil
			}
		}
	}
	if parent.Kind != types.Mirrored || parent.Source == nil || (view != nil && view.State.Detached) {
		return false, nil
	}
	snap, err := tx.Source()
	if err != nil {
		return false, err
	}
	cur, err := snap.ByColumns(parent.ID, pix.Columns, []types.Row{vals}, nil)
	if err != nil {
		return false, &types.Error{Code: types.ErrSource, Table: parent.ID, Msg: err.Error()}
	}
	defer cur.Close()
	for {
		sr, err := cur.Next(100)
		for i := range sr.Rows {
			var key types.SourceKey
			if i < len(sr.Keys) {
				key = sr.Keys[i]
			}
			if view != nil {
				if _, t := view.IsTouched(key); t {
					continue
				}
			}
			if err := merge.LockRow(tx, parent.ID, key, nil, types.LockShared); err != nil {
				return false, err
			}
			if _, touched := tx.Latest(parent.ID, nil, key); touched {
				continue
			}
			return true, nil
		}
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, &types.Error{Code: types.ErrSource, Table: parent.ID, Msg: err.Error()}
		}
		if len(sr.Rows) == 0 {
			return false, nil
		}
	}
}

// child is one row referencing a changed parent.
type child struct {
	ref types.RowRef
	row types.Row
}

// OnParentChange applies every foreign key that references a changed parent
// row: newRow is nil for a delete. Children are found through the child's
// supporting index, in the Delta and on the Source.
func (c *Checker) OnParentChange(tx *txn.Tx, parent *types.TableDef, oldRow, newRow types.Row, depth int) error {
	cat := tx.Manager().Catalog()
	children := cat.Children(parent.ID)
	if len(children) == 0 {
		return nil
	}
	var oldImages []types.KeyImage
	for _, cf := range children {
		fk := cf.FK
		pix := parent.Index(fk.ParentIndex)
		if pix == nil {
			continue
		}
		oldVals, ok := values(oldRow, pix.Columns)
		if !ok {
			continue
		}
		if newRow != nil {
			newVals, _ := values(newRow, pix.Columns)
			if newVals != nil && equalVals(oldVals, newVals) {
				continue // the referenced key did not change
			}
		}
		if !pix.EqualityProven {
			return refused(parent.ID, fk.Name)
		}
		action := fk.OnDelete
		if newRow != nil {
			action = fk.OnUpdate
		}
		if oldImages == nil && tx.Images != nil {
			ims, err := tx.Images(parent.ID, []types.Row{oldRow})
			if err != nil {
				return types.AsError(err)
			}
			if len(ims) > 0 {
				oldImages = ims[0]
			}
		}
		cdef, ok := cat.Table(cf.Child)
		if !ok {
			continue
		}
		kids, err := c.children(tx, &cdef, fk, pix, oldVals, oldImages)
		if err != nil {
			return err
		}
		if len(kids) == 0 {
			continue
		}
		switch action {
		case types.FKRestrict, types.FKNoAction:
			return &types.Error{Code: types.ErrRowIsReferenced, Table: parent.ID, Constraint: fk.Name,
				Msg: "Cannot delete or update a parent row: a foreign key constraint fails"}
		}
		if depth+1 > MaxDepth {
			return &types.Error{Code: types.ErrFKDepth, Table: cdef.ID, Constraint: fk.Name, Msg: "Foreign key cascade delete/update exceeds max depth of 15."}
		}
		for _, k := range kids {
			if err := merge.LockRow(tx, cdef.ID, k.ref.SourceKey, k.ref.PK.Image, types.LockExclusive); err != nil {
				return err
			}
			if action == types.FKCascade && newRow == nil {
				if err := c.W.DeleteRow(tx, cdef.ID, k.ref, k.row, depth+1); err != nil {
					return err
				}
				continue
			}
			nr := types.CloneRow(k.row)
			for i, col := range fk.Columns {
				if action == types.FKSetNull {
					nr[col] = types.Value{Null: true}
				} else {
					nv, _ := values(newRow, pix.Columns)
					nr[col] = nv[i]
				}
			}
			if tx.Images == nil {
				return &types.Error{Code: types.ErrInternal, Table: cdef.ID, Msg: "the engine supplied no image callback for a cascaded update"}
			}
			ims, err := tx.Images(cdef.ID, []types.Row{nr})
			if err != nil {
				return types.AsError(err)
			}
			if err := c.W.UpdateRow(tx, cdef.ID, k.ref, k.row, nr, ims[0], depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func equalVals(a, b []types.Value) bool {
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

// children finds the merged child rows referencing a parent key.
func (c *Checker) children(tx *txn.Tx, cdef *types.TableDef, fk types.ForeignKeyDef, pix *types.IndexDef, vals []types.Value, parentImages []types.KeyImage) ([]child, error) {
	view, err := tx.View(cdef.ID)
	if err != nil {
		return nil, err
	}
	whole := cdef.Identity == types.WholeRow
	var out []child
	seen := map[string]bool{}
	if view != nil && int(pix.ID) < len(parentImages) && parentImages[pix.ID] != nil {
		pimg := cut(parentImages[pix.ID], prefixLen(pix, len(pix.Columns)))
		it := view.IndexInRange(fk.ChildIndex, types.ImageRange{Low: pimg, High: pimg, LowIncl: true, HighIncl: true}, types.Forward)
		for r := it.Next(); r != nil; r = it.Next() {
			ref := types.RowRef{Origin: types.OriginDelta, PK: types.Key{Image: r.Image}, SourceKey: r.SourceKey}
			if whole {
				ref.Whole = r.Row
			}
			out = append(out, child{ref: ref, row: r.Row})
			if r.SourceKey != nil {
				seen[string(r.SourceKey)] = true
			}
		}
		it.Close()
	}
	if cdef.Kind != types.Mirrored || cdef.Source == nil || (view != nil && view.State.Detached) {
		return out, nil
	}
	snap, err := tx.Source()
	if err != nil {
		return nil, err
	}
	all := make(types.ColumnSet, len(cdef.Columns))
	for i := range all {
		all[i] = uint16(i)
	}
	cur, err := snap.ByColumns(cdef.ID, fk.Columns, []types.Row{vals}, all)
	if err != nil {
		return nil, &types.Error{Code: types.ErrSource, Table: cdef.ID, Msg: err.Error()}
	}
	defer cur.Close()
	for {
		sr, err := cur.Next(1000)
		for i, row := range sr.Rows {
			var key types.SourceKey
			if i < len(sr.Keys) {
				key = sr.Keys[i]
			}
			if seen[string(key)] {
				continue
			}
			if view != nil {
				if _, t := view.IsTouched(key); t && !whole {
					continue
				}
			}
			if _, t := tx.Latest(cdef.ID, nil, key); t && !whole {
				continue
			}
			ref := types.RowRef{Origin: types.OriginSource, SourceKey: key}
			if whole {
				ref.Whole = row
			}
			out = append(out, child{ref: ref, row: row})
		}
		if err == io.EOF || len(sr.Rows) == 0 {
			break
		}
		if err != nil {
			return nil, &types.Error{Code: types.ErrSource, Table: cdef.ID, Msg: err.Error()}
		}
	}
	return out, nil
}

// ValidateExisting checks every existing child row for ADD FOREIGN KEY with
// checks on: it streams the merged child table and fails on the first orphan.
// Nothing is stored (product doc 10 §6.4).
func (c *Checker) ValidateExisting(tx *txn.Tx, cdef *types.TableDef, fk types.ForeignKeyDef) error {
	all := make(types.ColumnSet, len(cdef.Columns))
	for i := range all {
		all[i] = uint16(i)
	}
	h, err := c.Merge.Scan(tx, types.ScanRequest{Table: cdef.ID, Index: cdef.Identity, Columns: all})
	if err != nil {
		return err
	}
	defer h.Close()
	for {
		b, err := h.Merged().Next(1000, 0)
		if len(b.Rows) > 0 {
			var images [][]types.KeyImage
			if tx.Images != nil {
				images, err = tx.Images(cdef.ID, b.Rows)
				if err != nil {
					return types.AsError(err)
				}
			}
			for i, row := range b.Rows {
				var ims []types.KeyImage
				if i < len(images) {
					ims = images[i]
				}
				if err := c.CheckParent(tx, cdef, fk, row, ims); err != nil {
					return err
				}
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
