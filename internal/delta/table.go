// Package delta provides the Delta: per table, copy-on-write B-trees of Delta
// rows, touched Source keys and Delta index entries, plus the committed root
// and the drafts transactions write into (implementation plan doc 04 §1).
//
// Every tree is a github.com/tidwall/btree generic B-tree. Copy() is
// copy-on-write and costs nothing until a node changes, which gives snapshots,
// write buffers and stable cursors. A tree that has been published in a Root
// is never mutated again; readers iterate over their own Copy().
package delta

// Package delta — table.go

import (
	"bytes"

	"github.com/tidwall/btree"
	"github.com/virtual-db/vdb-core/internal/types"
)

// DeltaRow is one row of the Delta.
type DeltaRow struct {
	Image     types.KeyImage   // identity image, or a synthetic row id for tables without a usable key
	Row       types.Row        // every column of the table, in definition order
	Origin    types.Origin     // OriginSource when it replaced a Source row
	SourceKey types.SourceKey  // the Source key it replaced, when Origin is OriginSource
	Images    []types.KeyImage // one per index, indexed by IndexID; may be nil for indexes without images
}

func (r *DeltaRow) size() uint64 {
	n := len(r.Image) + len(r.SourceKey) + types.RowSize(r.Row) + 48
	for _, im := range r.Images {
		n += len(im) + 24
	}
	return uint64(n)
}

// Touched is a Source key the environment updated or deleted. For tables
// without a usable key, Key is the whole row's bytes and Count the number of
// identical rows gone (D-10).
type Touched struct {
	Key   types.SourceKey
	Image types.KeyImage // when the engine supplied it
	Count uint32
}

// IndexEntry is one Delta index entry: the index image and the row's identity image.
type IndexEntry struct {
	Img types.KeyImage
	PK  types.KeyImage
}

// TableState is the per-table state of product doc 05 §2.
type TableState struct {
	Kind      types.TableKind
	Version   uint64
	Rows      uint64
	TouchedN  uint64
	Bytes     uint64
	Detached  bool
	NextRowID uint64
}

// Table holds the trees of one table.
type Table struct {
	rows       *btree.BTreeG[*DeltaRow]
	touched    *btree.BTreeG[*Touched]
	touchedImg *btree.BTreeG[*Touched]
	index      map[types.IndexID]*btree.BTreeG[IndexEntry]
	State      TableState
}

func rowLess(a, b *DeltaRow) bool     { return bytes.Compare(a.Image, b.Image) < 0 }
func touchLess(a, b *Touched) bool    { return bytes.Compare(a.Key, b.Key) < 0 }
func touchImgLess(a, b *Touched) bool { return bytes.Compare(a.Image, b.Image) < 0 }
func entryLess(a, b IndexEntry) bool {
	if c := bytes.Compare(a.Img, b.Img); c != 0 {
		return c < 0
	}
	return bytes.Compare(a.PK, b.PK) < 0
}

// NewTable returns an empty table.
func NewTable(kind types.TableKind, version uint64) *Table {
	return &Table{
		rows:       btree.NewBTreeG(rowLess),
		touched:    btree.NewBTreeG(touchLess),
		touchedImg: btree.NewBTreeG(touchImgLess),
		index:      map[types.IndexID]*btree.BTreeG[IndexEntry]{},
		State:      TableState{Kind: kind, Version: version},
	}
}

// copy returns a copy-on-write copy of every tree.
func (t *Table) copy() *Table {
	c := &Table{
		rows:       t.rows.Copy(),
		touched:    t.touched.Copy(),
		touchedImg: t.touchedImg.Copy(),
		index:      make(map[types.IndexID]*btree.BTreeG[IndexEntry], len(t.index)),
		State:      t.State,
	}
	for id, tr := range t.index {
		c.index[id] = tr.Copy()
	}
	return c
}

// Copy returns a stable copy of the table for reading.
func (t *Table) Copy() *Table { return t.copy() }

func (t *Table) indexTree(id types.IndexID) *btree.BTreeG[IndexEntry] {
	tr := t.index[id]
	if tr == nil {
		tr = btree.NewBTreeG(entryLess)
		t.index[id] = tr
	}
	return tr
}

// Get returns the Delta row with an identity image equal to img.
func (t *Table) Get(img types.KeyImage) (*DeltaRow, bool) {
	return t.rows.Get(&DeltaRow{Image: img})
}

// Holds reports whether a Delta row with an equal key exists (D-33).
func (t *Table) Holds(img types.KeyImage) bool {
	_, ok := t.Get(img)
	return ok
}

// IsTouched reports whether a Source row is hidden, and for tables without a
// usable key how many identical rows are gone.
func (t *Table) IsTouched(k types.SourceKey) (uint32, bool) {
	tc, ok := t.touched.Get(&Touched{Key: k})
	if !ok {
		return 0, false
	}
	return tc.Count, true
}

// TouchedByImage reports whether a touched key has the image img.
func (t *Table) TouchedByImage(img types.KeyImage) bool {
	if len(img) == 0 {
		return false
	}
	_, ok := t.touchedImg.Get(&Touched{Image: img})
	return ok
}

// IndexHas reports whether an index entry with index image img exists, other
// than for the row whose identity image is except.
func (t *Table) IndexHas(id types.IndexID, img types.KeyImage, except types.KeyImage) (types.KeyImage, bool) {
	tr := t.index[id]
	if tr == nil {
		return nil, false
	}
	var found types.KeyImage
	tr.Ascend(IndexEntry{Img: img}, func(e IndexEntry) bool {
		if !bytes.Equal(e.Img, img) {
			return false
		}
		if except != nil && bytes.Equal(e.PK, except) {
			return true
		}
		found = e.PK
		return false
	})
	return found, found != nil
}

// ShadowImages returns the images of every Delta key that can hide a Source
// row: touched keys with images and every Delta row (product doc 03 §2.4, D-33).
func (t *Table) ShadowImages() []types.KeyImage {
	var out []types.KeyImage
	t.touchedImg.Scan(func(tc *Touched) bool { out = append(out, tc.Image); return true })
	t.rows.Scan(func(r *DeltaRow) bool { out = append(out, r.Image); return true })
	return out
}

// TouchedKeys returns every touched key.
func (t *Table) TouchedKeys() []*Touched {
	var out []*Touched
	t.touched.Scan(func(tc *Touched) bool { out = append(out, tc); return true })
	return out
}

// Rows returns every Delta row in image order.
func (t *Table) Rows() []*DeltaRow {
	var out []*DeltaRow
	t.rows.Scan(func(r *DeltaRow) bool { out = append(out, r); return true })
	return out
}

// Len returns the number of Delta rows.
func (t *Table) Len() int { return t.rows.Len() }

// ── mutation (drafts only) ──

func (t *Table) putRow(r *DeltaRow) {
	if old, ok := t.rows.Get(r); ok {
		t.removeIndex(old)
		t.State.Bytes -= old.size()
		t.State.Rows--
	}
	t.rows.Set(r)
	for i, im := range r.Images {
		if im == nil {
			continue
		}
		t.indexTree(types.IndexID(i)).Set(IndexEntry{Img: im, PK: r.Image})
	}
	t.State.Bytes += r.size()
	t.State.Rows++
}

func (t *Table) delRow(img types.KeyImage) {
	old, ok := t.rows.Delete(&DeltaRow{Image: img})
	if !ok {
		return
	}
	t.removeIndex(old)
	t.State.Bytes -= old.size()
	t.State.Rows--
}

func (t *Table) removeIndex(r *DeltaRow) {
	for i, im := range r.Images {
		if im == nil {
			continue
		}
		if tr := t.index[types.IndexID(i)]; tr != nil {
			tr.Delete(IndexEntry{Img: im, PK: r.Image})
		}
	}
}

func (t *Table) touch(k types.SourceKey, img types.KeyImage, delta int32) {
	cur, ok := t.touched.Get(&Touched{Key: k})
	n := int64(delta)
	if ok {
		n += int64(cur.Count)
		t.touched.Delete(cur)
		if len(cur.Image) > 0 {
			t.touchedImg.Delete(cur)
		}
		if len(img) == 0 {
			img = cur.Image
		}
		t.State.TouchedN--
		t.State.Bytes -= uint64(len(cur.Key) + len(cur.Image) + 40)
	}
	if n <= 0 {
		return
	}
	tc := &Touched{Key: k, Image: img, Count: uint32(n)}
	t.touched.Set(tc)
	if len(img) > 0 {
		t.touchedImg.Set(tc)
	}
	t.State.TouchedN++
	t.State.Bytes += uint64(len(k) + len(img) + 40)
}

func (t *Table) clear() {
	fresh := NewTable(t.State.Kind, t.State.Version)
	t.rows, t.touched, t.touchedImg, t.index = fresh.rows, fresh.touched, fresh.touchedImg, fresh.index
	t.State.Rows, t.State.TouchedN, t.State.Bytes = 0, 0, 0
}

// FindBySourceKey returns the Delta row that replaced the Source row with key k.
func (t *Table) FindBySourceKey(k types.SourceKey) (*DeltaRow, bool) {
	var found *DeltaRow
	t.rows.Scan(func(r *DeltaRow) bool {
		if r.SourceKey != nil && bytes.Equal(r.SourceKey, k) {
			found = r
			return false
		}
		return true
	})
	return found, found != nil
}
