// Package catalog_test — tests/catalog_test.go
//
// Load keeps ids, kinds and local changes; Source changes; object order;
// engine parts kept with local changes; provided tables (doc 06 §6).
package catalog_test

import (
	"testing"

	. "github.com/virtual-db/vdb-core/internal/catalog"
	"github.com/virtual-db/vdb-core/internal/types"
)

func def(name string) types.TableDef {
	return types.TableDef{Namespace: []string{"d"}, Name: name, Kind: types.Mirrored, Columns: []types.ColumnDef{{Name: "id"}},
		Indexes: []types.IndexDef{{ID: 0, Name: "PRIMARY", Primary: true, Unique: true, Columns: []uint16{0}}}}
}

func TestLoadKeepsLocalState(t *testing.T) {
	c := New()
	c.Load([]types.TableDef{def("t")}, nil)
	t1, _ := c.TableByName([]string{"d"}, "T")
	c.Apply(types.CatalogChange{Kind: types.AlterTable, ID: t1.ID, Table: def("t"), Local: &types.LocalChange{Kind: types.AddColumn, Object: "x"}})
	c.Load([]types.TableDef{def("t")}, nil)
	t2, _ := c.TableByName([]string{"d"}, "t")
	if t2.ID != t1.ID || len(t2.LocalChanges) != 1 {
		t.Fatalf("id and local changes kept: %+v", t2)
	}
}

func TestEnginePartKeptWithLocalChanges(t *testing.T) {
	c := New()
	src := def("t")
	src.Engine = []byte("source definition")
	c.Load([]types.TableDef{src}, nil)
	t1, _ := c.TableByName([]string{"d"}, "t")
	local := def("t")
	local.Engine = []byte("local definition")
	c.Apply(types.CatalogChange{Kind: types.AlterTable, ID: t1.ID, Table: local, Local: &types.LocalChange{Kind: types.AddColumn, Object: "x"}})
	c.Load([]types.TableDef{src}, nil)
	t2, _ := c.TableByName([]string{"d"}, "t")
	if string(t2.Engine) != "local definition" {
		t.Fatalf("engine part = %q, want the local definition", t2.Engine)
	}
}

func TestObjectsInOrder(t *testing.T) {
	c := New()
	c.RecordObject(types.Object{Kind: "view", Namespace: []string{"d"}, Name: "v1", Engine: []byte("1")})
	c.RecordObject(types.Object{Kind: "view", Namespace: []string{"d"}, Name: "v2", Engine: []byte("2")})
	c.RecordObject(types.Object{Kind: "view", Namespace: []string{"d"}, Name: "v1", Engine: []byte("3")})
	objs := c.Objects()
	if len(objs) != 2 || objs[0].Name != "v2" || string(objs[1].Engine) != "3" {
		t.Fatalf("objects: %+v", objs)
	}
	c.RecordObject(types.Object{Kind: "view", Namespace: []string{"d"}, Name: "v2", Dropped: true})
	if len(c.Objects()) != 1 {
		t.Fatal("a captured DROP removes the object")
	}
}
