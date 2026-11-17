// Package delta — iter.go
//
// Range reads over a table's rows and index entries (doc 04 §1.3). An Iter
// works on its own copy of the trees, so it is a stable cursor: writes made
// after it opened never appear in it (product doc 05 §3.1, D-51).
package delta

import (
	"bytes"

	"github.com/tidwall/btree"
	"github.com/virtual-db/vdb-core/internal/types"
)

// InLow reports whether img is at or after the range's low bound.
func InLow(img types.KeyImage, r types.ImageRange) bool {
	if len(r.Low) == 0 {
		return true
	}
	p := img
	if len(p) > len(r.Low) {
		p = p[:len(r.Low)]
	}
	c := bytes.Compare(p, r.Low)
	if r.LowIncl {
		return c >= 0
	}
	return c > 0
}

// InHigh reports whether img is at or before the range's high bound.
func InHigh(img types.KeyImage, r types.ImageRange) bool {
	if len(r.High) == 0 {
		return true
	}
	p := img
	if len(p) > len(r.High) {
		p = p[:len(r.High)]
	}
	c := bytes.Compare(p, r.High)
	if r.HighIncl {
		return c <= 0
	}
	return c < 0
}

// InRange reports whether img lies within r.
func InRange(img types.KeyImage, r types.ImageRange) bool { return InLow(img, r) && InHigh(img, r) }

// successor returns the smallest byte string greater than every string with prefix p.
func successor(p []byte) []byte {
	s := append([]byte(nil), p...)
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] != 0xFF {
			s[i]++
			return s[:i+1]
		}
	}
	return nil
}

// Iter yields Delta rows in image order within one range.
type Iter struct {
	rows  *btree.BTreeG[*DeltaRow]
	idx   *btree.BTreeG[IndexEntry]
	it    btree.IterG[*DeltaRow]
	ix    btree.IterG[IndexEntry]
	rng   types.ImageRange
	dir   types.Direction
	start bool
	done  bool
}

// RowsInRange iterates the table's Delta rows of the identity key in image order.
func (t *Table) RowsInRange(r types.ImageRange, dir types.Direction) *Iter {
	c := t.rows.Copy()
	return &Iter{rows: c, it: c.Iter(), rng: r, dir: dir}
}

// IndexInRange iterates index entries of index id in image order, resolved to rows.
func (t *Table) IndexInRange(id types.IndexID, r types.ImageRange, dir types.Direction) *Iter {
	tr := t.index[id]
	if tr == nil {
		tr = btree.NewBTreeG(entryLess)
	}
	c := tr.Copy()
	rows := t.rows.Copy()
	return &Iter{rows: rows, idx: c, ix: c.Iter(), rng: r, dir: dir}
}

// Next returns the next row, or nil at the end.
func (it *Iter) Next() *DeltaRow {
	if it.done {
		return nil
	}
	for {
		img, row, ok := it.step()
		if !ok {
			it.done = true
			it.release()
			return nil
		}
		if it.dir == types.Forward {
			if !InHigh(img, it.rng) {
				it.done = true
				it.release()
				return nil
			}
			if !InLow(img, it.rng) {
				continue
			}
		} else {
			if !InLow(img, it.rng) {
				it.done = true
				it.release()
				return nil
			}
			if !InHigh(img, it.rng) {
				continue
			}
		}
		if row != nil {
			return row
		}
	}
}

func (it *Iter) step() (types.KeyImage, *DeltaRow, bool) {
	if it.idx != nil {
		var ok bool
		if !it.start {
			it.start = true
			ok = it.seekIdx()
		} else if it.dir == types.Forward {
			ok = it.ix.Next()
		} else {
			ok = it.ix.Prev()
		}
		if !ok {
			return nil, nil, false
		}
		e := it.ix.Item()
		r, _ := it.rows.Get(&DeltaRow{Image: e.PK})
		return e.Img, r, true
	}
	var ok bool
	if !it.start {
		it.start = true
		ok = it.seekRows()
	} else if it.dir == types.Forward {
		ok = it.it.Next()
	} else {
		ok = it.it.Prev()
	}
	if !ok {
		return nil, nil, false
	}
	r := it.it.Item()
	return r.Image, r, true
}

func (it *Iter) seekRows() bool {
	if it.dir == types.Forward {
		if len(it.rng.Low) == 0 {
			return it.it.First()
		}
		return it.it.Seek(&DeltaRow{Image: it.rng.Low})
	}
	if len(it.rng.High) == 0 {
		return it.it.Last()
	}
	succ := successor(it.rng.High)
	if succ == nil || !it.it.Seek(&DeltaRow{Image: succ}) {
		return it.it.Last()
	}
	return it.it.Prev()
}

func (it *Iter) seekIdx() bool {
	if it.dir == types.Forward {
		if len(it.rng.Low) == 0 {
			return it.ix.First()
		}
		return it.ix.Seek(IndexEntry{Img: it.rng.Low})
	}
	if len(it.rng.High) == 0 {
		return it.ix.Last()
	}
	succ := successor(it.rng.High)
	if succ == nil || !it.ix.Seek(IndexEntry{Img: succ}) {
		return it.ix.Last()
	}
	return it.ix.Prev()
}

func (it *Iter) release() {
	if it.idx != nil {
		it.ix.Release()
	} else {
		it.it.Release()
	}
}

// Close releases the iterator.
func (it *Iter) Close() {
	if !it.done {
		it.done = true
		it.release()
	}
}
