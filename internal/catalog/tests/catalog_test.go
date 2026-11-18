// Package catalog_test — tests/catalog_test.go
//
// Load keeps ids, kinds and local changes; Source changes; object order;
// foreign-key parent resolution; provided tables (doc 06 §6).
package catalog_test

import (
	"testing"

	. "github.com/virtual-db/vdb-core/internal/catalog"
	"github.com/virtual-db/vdb-core/internal/types"
)

func def(name string) types.TableDef {
	return types.TableDef{Database: "d", Name: name, Kind: types.Mirrored, Columns: []types.ColumnDef{{Name: "id"}},
		Indexes: []types.IndexDef{{ID: 0, Name: "PRIMARY", Primary: true, Unique: true, Columns: []uint16{0}}}}
}

func TestLoadKeepsLocalState(t *testing.T) {
	c := New()
	c.Load([]types.TableDef{def("t")}, nil)
	t1, _ := c.TableByName("d", "T")
	c.Apply(types.CatalogChange{Kind: types.AlterTable, ID: t1.ID, Table: def("t"), Local: &types.LocalChange{Kind: types.AddColumn, Object: "x"}})
	c.Load([]types.TableDef{def("t")}, nil)
	t2, _ := c.TableByName("d", "t")
	if t2.ID != t1.ID || len(t2.LocalChanges) != 1 {
		t.Fatalf("id and local changes kept: %+v", t2)
	}
}

func TestForeignKeyResolution(t *testing.T) {
	c := New()
	child := def("child")
	child.Columns = append(child.Columns, types.ColumnDef{Name: "p"})
	child.ForeignKeys = []types.ForeignKeyDef{{Name: "fk", Columns: []uint16{1}, ParentDB: "d", ParentTable: "parent", ParentColumns: []string{"id"}}}
	c.Load([]types.TableDef{def("parent"), child}, nil)
	p, _ := c.TableByName("d", "parent")
	if kids := c.Children(p.ID); len(kids) != 1 || kids[0].FK.Name != "fk" {
		t.Fatalf("children: %+v", kids)
	}
}

func TestObjectsInOrder(t *testing.T) {
	c := New()
	c.RecordObject(types.Object{Kind: types.View, Database: "d", Name: "v1", Statement: "CREATE VIEW v1 AS SELECT 1"})
	c.RecordObject(types.Object{Kind: types.View, Database: "d", Name: "v2", Statement: "CREATE VIEW v2 AS SELECT 2"})
	c.RecordObject(types.Object{Kind: types.View, Database: "d", Name: "v1", Statement: "CREATE VIEW v1 AS SELECT 3"})
	objs := c.Objects()
	if len(objs) != 2 || objs[0].Name != "v2" || objs[1].Statement != "CREATE VIEW v1 AS SELECT 3" {
		t.Fatalf("objects: %+v", objs)
	}
	c.RecordObject(types.Object{Kind: types.View, Database: "d", Name: "v2", Dropped: true})
	if len(c.Objects()) != 1 {
		t.Fatal("a captured DROP removes the object")
	}
}
