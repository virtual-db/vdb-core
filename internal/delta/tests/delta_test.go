// Package delta_test — tests/delta_test.go
//
// Copy-on-write isolation of snapshots, publication re-applying over
// intervening commits, stable iterators, ranges with prefix bounds, touched
// keys with counts, savepoint restore, and byte accounting (doc 04 §5).
package delta_test

import (
	"testing"

	. "github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/types"
)

func row(img string) *DeltaRow {
	return &DeltaRow{Image: types.KeyImage(img), Row: types.Row{{B: []byte(img)}}, Images: []types.KeyImage{types.KeyImage(img)}}
}

func put(t types.TableID, img string) Op { return Op{Kind: OpPutRow, Table: t, Row: row(img)} }

func images(it *Iter) []string {
	var out []string
	for r := it.Next(); r != nil; r = it.Next() {
		out = append(out, string(r.Image))
	}
	return out
}

func TestSnapshotIsolationAndPublish(t *testing.T) {
	s := NewStore()
	if err := s.Publish([]Op{put(1, "b")}, 1); err != nil {
		t.Fatal(err)
	}
	snap := s.Snapshot()
	d1 := snap.Fork()
	d1.Apply(put(1, "a"))
	d2 := snap.Fork()
	d2.Apply(put(1, "c"))
	if err := s.Publish(d1.Ops(), 2); err != nil {
		t.Fatal(err)
	}
	if err := s.Publish(d2.Ops(), 3); err != nil {
		t.Fatal(err)
	}
	if got := images(s.Snapshot().Table(1).RowsInRange(types.ImageRange{}, types.Forward)); len(got) != 3 {
		t.Fatalf("publish must re-apply over intervening commits, got %v", got)
	}
	if snap.Table(1).Len() != 1 {
		t.Fatal("an old snapshot must not change")
	}
}

func TestStableIteratorAndRanges(t *testing.T) {
	s := NewStore()
	s.Publish([]Op{put(1, "a1"), put(1, "a2"), put(1, "b1"), put(1, "c1")}, 1)
	d := s.Snapshot().Fork()
	d.Apply(put(1, "a3"))
	it := d.Table(1).RowsInRange(types.ImageRange{Low: types.KeyImage("a"), High: types.KeyImage("a"), LowIncl: true, HighIncl: true}, types.Forward)
	d.Apply(put(1, "a4")) // after the iterator opened
	if got := images(it); len(got) != 3 || got[2] != "a3" {
		t.Fatalf("prefix range must see a1 a2 a3 only, got %v", got)
	}
	back := images(d.Table(1).RowsInRange(types.ImageRange{High: types.KeyImage("b"), HighIncl: false}, types.Backward))
	if len(back) != 4 || back[0] != "a4" {
		t.Fatalf("backward below b: %v", back)
	}
	ex := images(d.Table(1).RowsInRange(types.ImageRange{Low: types.KeyImage("a"), LowIncl: false}, types.Forward))
	if len(ex) != 2 || ex[0] != "b1" {
		t.Fatalf("exclusive low prefix skips every a*: %v", ex)
	}
}

func TestTouchedCountsAndRestore(t *testing.T) {
	s := NewStore()
	d := s.Snapshot().Fork()
	d.Apply(Op{Kind: OpTouch, Table: 2, Key: types.SourceKey("row"), Whole: true, Delta: 1})
	m := d.Mark()
	d.Apply(Op{Kind: OpTouch, Table: 2, Key: types.SourceKey("row"), Whole: true, Delta: 1})
	if n, _ := d.Table(2).IsTouched(types.SourceKey("row")); n != 2 {
		t.Fatalf("count = %d", n)
	}
	d.Restore(m)
	if n, _ := d.Table(2).IsTouched(types.SourceKey("row")); n != 1 {
		t.Fatalf("after restore count = %d", n)
	}
	if d.Table(2).State.Bytes == 0 {
		t.Fatal("touched keys are accounted")
	}
}

func TestTruncateDetaches(t *testing.T) {
	s := NewStore()
	s.Publish([]Op{put(3, "x"), {Kind: OpTouch, Table: 3, Key: types.SourceKey("k")}}, 1)
	s.Publish([]Op{{Kind: OpTruncate, Table: 3}}, 2)
	tb := s.Snapshot().Table(3)
	if tb.Len() != 0 || len(tb.TouchedKeys()) != 0 || !tb.State.Detached {
		t.Fatal("truncate clears rows and touched keys and detaches")
	}
}
