// Package changelog_test — tests/changelog_test.go
//
// Change-set order equals write order, and the JSON round trip of every
// value shape (doc 06 §6).
package changelog_test

import (
	"encoding/json"
	"reflect"
	"testing"

	. "github.com/virtual-db/vdb-core/internal/changelog"
	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/types"
)

func TestOrderAndRoundTrip(t *testing.T) {
	ops := []delta.Op{
		{Kind: delta.OpPutRow, Table: 1, Row: &delta.DeltaRow{Image: types.KeyImage{1}, Row: types.Row{{B: []byte("a")}, {Null: true}, {B: []byte{0xff, 0x00}}}}},
		{Kind: delta.OpTouch, Table: 1, Key: types.SourceKey("k")},
		{Kind: delta.OpDelRow, Table: 1, Image: types.KeyImage{2}},
	}
	cs := FromOps(7, types.ChangeCommit, ops)
	if len(cs.Rows) != 2 || cs.Rows[0].Op != types.RowPut || cs.Rows[1].Op != types.RowDelete || len(cs.Touched) != 1 {
		t.Fatalf("change set: %+v", cs)
	}
	b, err := json.Marshal(cs)
	if err != nil {
		t.Fatal(err)
	}
	var back types.ChangeSet
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cs.Rows[0].Row, back.Rows[0].Row) {
		t.Fatalf("values: %+v vs %+v", cs.Rows[0].Row, back.Rows[0].Row)
	}
	if len(ToOps(&back)) != 3 {
		t.Fatal("ops from a change set")
	}
}
