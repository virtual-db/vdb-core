// Package types — catalog.go
//
// The catalog of record's types (implementation plan doc 06 §1.1). They hold
// what the core itself needs, in a form any engine can fill; each entry has an
// opaque engine part (Engine) that the core stores, versions, records in change
// sets and restores, but never reads (D-88).
package types

import "strings"

// TableKind says where a table's rows come from.
type TableKind uint8

const (
	// Mirrored tables merge a Source table with the Delta.
	Mirrored TableKind = iota
	// Local tables were created by a client; every row is a Delta row.
	Local
	// Detached tables were mirrored until a TRUNCATE; every row is a Delta row.
	Detached
	// Provided tables are produced by a RowProvider (the vdb schema).
	Provided
)

func (k TableKind) String() string {
	return [...]string{"mirrored", "local", "detached", "provided"}[k]
}

// FetchMode says how a column's value is obtained for Source rows.
type FetchMode uint8

const (
	FromSource FetchMode = iota
	Default
	Virtual
	Transformed
)

// WholeRow marks a table without a usable key (D-10) in TableDef.Identity.
const WholeRow IndexID = 0xFFFF

// ColumnDef is one column.
type ColumnDef struct {
	Name     string
	Pos      uint16
	Nullable bool
	Fetch    FetchMode
	Default  Value
	Engine   []byte
}

// IndexDef is one index. Index 0 is the identity key when the table has one.
// The engine sets OrderProven and EqualityProven from its own rules.
type IndexDef struct {
	ID             IndexID
	Name           string
	Primary        bool
	Unique         bool
	Columns        []uint16
	OrderProven    bool
	EqualityProven bool
	AddedLocally   bool
	Engine         []byte
}

// SourceMapping ties a mirrored table to its Source table.
type SourceMapping struct {
	Namespace   []string
	Table       string
	Fingerprint string
	Engine      []byte
}

// LocalChangeKind classifies a client DDL change on a mirrored table.
type LocalChangeKind uint8

const (
	AddColumn LocalChangeKind = iota
	DropColumn
	AddIndex
	DropIndex
	ChangeDefault
	AddForeignKey
	DropForeignKey
	OtherChange
)

// LocalChange is one client DDL change kept on a mirrored table.
type LocalChange struct {
	Kind   LocalChangeKind
	Object string
	Engine []byte
}

// TableDef is one table of the catalog of record.
//
// Namespace is the table's place in the engine's naming: for MySQL the
// database ([db]); for PostgreSQL [db, schema]. KeyNeedsShadows, set by the
// engine, says a Source key can equal a Delta key without being byte-equal
// (product doc 03 §2.4).
type TableDef struct {
	ID              TableID
	Namespace       []string
	Name            string
	Kind            TableKind
	Version         uint64
	Columns         []ColumnDef
	Indexes         []IndexDef
	Identity        IndexID
	KeyNeedsShadows bool
	Source          *SourceMapping
	LocalChanges    []LocalChange
	Engine          []byte
}

// QualifiedName is the namespace and name joined with dots.
func (t *TableDef) QualifiedName() string {
	return strings.Join(append(append([]string(nil), t.Namespace...), t.Name), ".")
}

// Index returns the index with id, or nil.
func (t *TableDef) Index(id IndexID) *IndexDef {
	for i := range t.Indexes {
		if t.Indexes[i].ID == id {
			return &t.Indexes[i]
		}
	}
	return nil
}

// IdentityColumns returns the identity key's columns, or nil for WholeRow tables.
func (t *TableDef) IdentityColumns() []uint16 {
	if t.Identity == WholeRow {
		return nil
	}
	if ix := t.Index(t.Identity); ix != nil {
		return ix.Columns
	}
	return nil
}

// ShadowsNeeded reports whether a Source row's key can match a Delta key
// without being byte-equal; shadow sets are skipped otherwise.
func (t *TableDef) ShadowsNeeded() bool { return t.KeyNeedsShadows }

// ColumnByName returns the column's position, or -1.
func (t *TableDef) ColumnByName(name string) int {
	for i, c := range t.Columns {
		if equalFold(c.Name, name) {
			return i
		}
	}
	return -1
}

func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		x, y := a[i], b[i]
		if 'A' <= x && x <= 'Z' {
			x += 'a' - 'A'
		}
		if 'A' <= y && y <= 'Z' {
			y += 'a' - 'A'
		}
		if x != y {
			return false
		}
	}
	return true
}

// ObjectOrigin says who created an object.
type ObjectOrigin uint8

const (
	ObjectFromSource ObjectOrigin = iota
	ObjectClient
)

// Object is a catalog object other than a table; its kind is the engine's
// (for MySQL "view", "procedure", "function", "trigger").
type Object struct {
	Kind      string
	Namespace []string
	Name      string
	Origin    ObjectOrigin
	Seq       uint64
	Dropped   bool // a DROP captured from a client
	Engine    []byte
}

// CatalogChangeKind classifies a staged DDL change.
type CatalogChangeKind uint8

const (
	CreateTable CatalogChangeKind = iota
	DropTable
	RenameTable
	AlterTable
	// DetachTable marks a mirrored table detached after TRUNCATE.
	DetachTable
)

// CatalogChange is a DDL change staged in a transaction (doc 06 §1.2).
type CatalogChange struct {
	Kind         CatalogChangeKind
	Table        TableDef // the new definition (Create, Alter, Rename)
	ID           TableID  // the table changed (Drop, Rename, Alter)
	NewNamespace []string // Rename
	NewName      string   // Rename
	Local        *LocalChange
	Source       bool // applied from the Source through a control session
}

// SourceChangeKind classifies a change discovered on the Source.
type SourceChangeKind uint8

const (
	SourceTableAdded SourceChangeKind = iota
	SourceTableChanged
	SourceTableDropped
	SourceTableRenamed
	SourceObjectChanged
	SourceObjectDropped
	SourceStatsRefreshed
)

// SourceChange is one change applied outside a client transaction.
type SourceChange struct {
	Kind   SourceChangeKind
	Table  TableDef
	ID     TableID
	Object Object
}
