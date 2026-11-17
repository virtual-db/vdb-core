// Package delta — store.go
//
// The committed state (Store, Root), drafts (copy-on-write write buffers),
// the operation log a draft records, savepoints, and publication (doc 04 §1.2).
package delta

import (
	"fmt"
	"sync"

	"github.com/virtual-db/vdb-core/internal/types"
)

// OpKind classifies one recorded change.
type OpKind uint8

const (
	OpPutRow OpKind = iota
	OpDelRow
	OpTouch
	OpTruncate
	OpCreateTable
	OpDropTable
	OpSetVersion
	OpReplaceRows
)

// Op is one change recorded by a draft, in write order. Publication re-applies
// a draft's ops to the current committed trees.
type Op struct {
	Kind    OpKind
	Table   types.TableID
	Row     *DeltaRow
	Image   types.KeyImage
	Key     types.SourceKey
	Delta   int32 // OpTouch: count change for tables without a usable key
	Whole   bool  // OpTouch: the table has no usable key
	TKind   types.TableKind
	Version uint64
	Rows    []*DeltaRow // OpReplaceRows
}

// Root is an immutable set of per-table trees and the commit sequence number
// that produced it.
type Root struct {
	tables map[types.TableID]*Table
	Seq    uint64
}

// Table returns the committed table, or nil.
func (r *Root) Table(id types.TableID) *Table {
	if r == nil {
		return nil
	}
	return r.tables[id]
}

// Tables returns the ids of every table in the root.
func (r *Root) Tables() []types.TableID {
	out := make([]types.TableID, 0, len(r.tables))
	for id := range r.tables {
		out = append(out, id)
	}
	return out
}

// Store holds the committed state. Safe for concurrent use.
type Store struct {
	mu   sync.RWMutex
	root *Root
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{root: &Root{tables: map[types.TableID]*Table{}}}
}

// Snapshot returns the committed root.
func (s *Store) Snapshot() *Root {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.root
}

// Publish re-applies a draft's ops to the current committed trees and swaps
// the root. The caller holds the transaction manager's commit lock.
func (s *Store) Publish(ops []Op, seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next, err := replay(s.root, ops)
	if err != nil {
		return err
	}
	next.Seq = seq
	s.root = next
	return nil
}

// Bytes sums the Delta's accounted size.
func (s *Store) Bytes() uint64 {
	r := s.Snapshot()
	var n uint64
	for _, t := range r.tables {
		n += t.State.Bytes
	}
	return n
}

// Stats returns per-table counters.
func (s *Store) Stats() types.DeltaStats {
	r := s.Snapshot()
	st := types.DeltaStats{Seq: r.Seq}
	for id, t := range r.tables {
		st.Bytes += t.State.Bytes
		st.Tables = append(st.Tables, types.TableStats{Table: id, DeltaRows: t.State.Rows, TouchedKeys: t.State.TouchedN, Bytes: t.State.Bytes})
	}
	return st
}

// replay applies ops to a copy of base and returns the new root.
func replay(base *Root, ops []Op) (*Root, error) {
	next := &Root{tables: make(map[types.TableID]*Table, len(base.tables)), Seq: base.Seq}
	for id, t := range base.tables {
		next.tables[id] = t
	}
	copied := map[types.TableID]bool{}
	for _, op := range ops {
		if err := applyOp(next.tables, copied, op); err != nil {
			return nil, err
		}
	}
	return next, nil
}

func applyOp(tables map[types.TableID]*Table, copied map[types.TableID]bool, op Op) error {
	switch op.Kind {
	case OpCreateTable:
		tables[op.Table] = NewTable(op.TKind, op.Version)
		copied[op.Table] = true
		return nil
	case OpDropTable:
		delete(tables, op.Table)
		delete(copied, op.Table)
		return nil
	}
	t := tables[op.Table]
	if t == nil {
		// A mirrored table gets its Delta trees on its first write.
		t = NewTable(op.TKind, op.Version)
		tables[op.Table] = t
		copied[op.Table] = true
	} else if !copied[op.Table] {
		t = t.copy()
		tables[op.Table] = t
		copied[op.Table] = true
	}
	switch op.Kind {
	case OpPutRow:
		t.putRow(op.Row)
	case OpDelRow:
		t.delRow(op.Image)
	case OpTouch:
		if op.Whole {
			t.touch(op.Key, op.Image, op.Delta)
		} else if _, ok := t.IsTouched(op.Key); !ok {
			t.touch(op.Key, op.Image, 1)
		}
	case OpTruncate:
		t.clear()
		t.State.Detached = true
		if t.State.Kind == types.Mirrored {
			t.State.Kind = types.Detached
		}
	case OpSetVersion:
		t.State.Version = op.Version
		if op.TKind != t.State.Kind {
			t.State.Kind = op.TKind
		}
	case OpReplaceRows:
		touched := t.TouchedKeys()
		kind, ver, det, next := t.State.Kind, t.State.Version, t.State.Detached, t.State.NextRowID
		t.clear()
		t.State.Kind, t.State.Version, t.State.Detached, t.State.NextRowID = kind, ver, det, next
		for _, tc := range touched {
			t.touch(tc.Key, tc.Image, int32(tc.Count))
		}
		for _, r := range op.Rows {
			t.putRow(r)
		}
	default:
		return fmt.Errorf("delta.apply: unknown op %d", op.Kind)
	}
	return nil
}

// Draft is a transaction's copy-on-write view of the Delta: its own writes
// over the root it forked from. Used by one goroutine at a time; the owning
// transaction guards it for READ UNCOMMITTED readers.
type Draft struct {
	base   *Root
	tables map[types.TableID]*Table
	copied map[types.TableID]bool
	ops    []Op
}

// Fork starts a draft over r.
func (r *Root) Fork() *Draft {
	return &Draft{base: r, tables: map[types.TableID]*Table{}, copied: map[types.TableID]bool{}}
}

// Rebase returns a draft over r holding the same ops (used at READ COMMITTED
// to see the latest committed state under the transaction's own writes).
func (d *Draft) Rebase(r *Root) (*Draft, error) {
	nd := r.Fork()
	for _, op := range d.ops {
		if err := nd.Apply(op); err != nil {
			return nil, err
		}
	}
	return nd, nil
}

// Base returns the root the draft forked from.
func (d *Draft) Base() *Root { return d.base }

// Table returns the draft's view of a table: its own copy when written, else
// the base's committed table (nil when the table has no Delta trees).
func (d *Draft) Table(id types.TableID) *Table {
	if t, ok := d.tables[id]; ok {
		return t
	}
	if d.dropped(id) {
		return nil
	}
	return d.base.Table(id)
}

func (d *Draft) dropped(id types.TableID) bool {
	for i := len(d.ops) - 1; i >= 0; i-- {
		if d.ops[i].Table != id {
			continue
		}
		return d.ops[i].Kind == OpDropTable
	}
	return false
}

// Apply records op and applies it to the draft's trees.
func (d *Draft) Apply(op Op) error {
	if _, ok := d.tables[op.Table]; !ok && op.Kind != OpCreateTable && op.Kind != OpDropTable {
		if bt := d.base.Table(op.Table); bt != nil && !d.dropped(op.Table) {
			d.tables[op.Table] = bt
		}
	}
	if err := applyOp(d.tables, d.copied, op); err != nil {
		return err
	}
	if op.Kind == OpDropTable {
		delete(d.tables, op.Table)
	}
	d.ops = append(d.ops, op)
	return nil
}

// Ops returns the recorded ops in write order.
func (d *Draft) Ops() []Op { return d.ops }

// Empty reports whether the draft recorded nothing.
func (d *Draft) Empty() bool { return len(d.ops) == 0 }

// Mark is a savepoint of a draft: the number of ops recorded so far.
type Mark int

// Mark returns a savepoint.
func (d *Draft) Mark() Mark { return Mark(len(d.ops)) }

// Restore undoes every op recorded after m by replaying the earlier ops over
// the draft's base: always exact, whatever happened since.
func (d *Draft) Restore(m Mark) error {
	if int(m) >= len(d.ops) {
		return nil
	}
	keep := append([]Op(nil), d.ops[:m]...)
	nd := d.base.Fork()
	for _, op := range keep {
		if err := nd.Apply(op); err != nil {
			return err
		}
	}
	*d = *nd
	return nil
}

// Since returns the ops recorded after m.
func (d *Draft) Since(m Mark) []Op { return d.ops[m:] }
