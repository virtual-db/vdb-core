// Package changelog turns a transaction's recorded Delta operations into a
// change set and back (implementation plan doc 06 §2). Change sets carry
// commit sequence numbers; persistence subscribes to them through ordinary
// pipeline handlers (sync) or event subscribers (async), and restores them at
// start through vdb.server.start.configure.
package changelog

import (
	"sync/atomic"

	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/types"
)

// FormatVersion is the change set's JSON format version (D-77).
const FormatVersion = 1

// Sequence hands out commit sequence numbers. Safe for concurrent use.
type Sequence struct{ n atomic.Uint64 }

// Next returns the next sequence number.
func (s *Sequence) Next() uint64 { return s.n.Add(1) }

// Current returns the last sequence number handed out.
func (s *Sequence) Current() uint64 { return s.n.Load() }

// Advance moves the counter to at least seq (restore).
func (s *Sequence) Advance(seq uint64) {
	for {
		cur := s.n.Load()
		if cur >= seq || s.n.CompareAndSwap(cur, seq) {
			return
		}
	}
}

// FromOps builds a change set from Delta ops, in write order.
func FromOps(seq uint64, kind types.ChangeKind, ops []delta.Op) *types.ChangeSet {
	cs := &types.ChangeSet{V: FormatVersion, Seq: seq, Kind: kind}
	for _, op := range ops {
		switch op.Kind {
		case delta.OpPutRow:
			cs.Rows = append(cs.Rows, types.RowChange{Table: op.Table, Op: types.RowPut, Image: op.Row.Image,
				Origin: op.Row.Origin, SourceKey: op.Row.SourceKey, Row: op.Row.Row, Images: op.Row.Images})
		case delta.OpDelRow:
			cs.Rows = append(cs.Rows, types.RowChange{Table: op.Table, Op: types.RowDelete, Image: op.Image})
		case delta.OpTouch:
			d := op.Delta
			if !op.Whole {
				d = 1
			}
			cs.Touched = append(cs.Touched, types.TouchChange{Table: op.Table, Key: op.Key, Image: op.Image, Delta: d, Whole: op.Whole})
		case delta.OpTruncate:
			cs.Rows = dropTable(cs.Rows, op.Table)
			cs.Touched = dropTouches(cs.Touched, op.Table)
			cs.Truncate = append(cs.Truncate, op.Table)
		case delta.OpReplaceRows:
			cs.Rows = dropTable(cs.Rows, op.Table)
			cs.Replaced = append(cs.Replaced, op.Table)
			for _, r := range op.Rows {
				cs.Rows = append(cs.Rows, types.RowChange{Table: op.Table, Op: types.RowPut, Image: r.Image,
					Origin: r.Origin, SourceKey: r.SourceKey, Row: r.Row, Images: r.Images})
			}
		}
	}
	return cs
}

func dropTable(rs []types.RowChange, t types.TableID) []types.RowChange {
	out := rs[:0]
	for _, r := range rs {
		if r.Table != t {
			out = append(out, r)
		}
	}
	return out
}

func dropTouches(ts []types.TouchChange, t types.TableID) []types.TouchChange {
	out := ts[:0]
	for _, x := range ts {
		if x.Table != t {
			out = append(out, x)
		}
	}
	return out
}

// ToOps turns a change set's row, touch and truncate changes back into Delta
// ops for replay. Catalog changes are replayed by the catalog.
func ToOps(cs *types.ChangeSet) []delta.Op {
	var ops []delta.Op
	for _, t := range cs.Truncate {
		ops = append(ops, delta.Op{Kind: delta.OpTruncate, Table: t})
	}
	for _, t := range cs.Replaced {
		ops = append(ops, delta.Op{Kind: delta.OpReplaceRows, Table: t})
	}
	for _, tc := range cs.Touched {
		ops = append(ops, delta.Op{Kind: delta.OpTouch, Table: tc.Table, Key: tc.Key, Image: tc.Image, Delta: tc.Delta, Whole: tc.Whole})
	}
	for _, rc := range cs.Rows {
		if rc.Op == types.RowDelete {
			ops = append(ops, delta.Op{Kind: delta.OpDelRow, Table: rc.Table, Image: rc.Image})
			continue
		}
		ops = append(ops, delta.Op{Kind: delta.OpPutRow, Table: rc.Table, Row: &delta.DeltaRow{
			Image: rc.Image, Row: rc.Row, Origin: rc.Origin, SourceKey: rc.SourceKey, Images: rc.Images}})
	}
	return ops
}
