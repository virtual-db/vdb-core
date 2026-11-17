// Package types — catalog.go
//
// The catalog of record's types (implementation plan doc 06 §1.1).
package types

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
	Name          string
	Pos           uint16
	Type          string // the column's SQL type as the engine prints it, e.g. "varchar(20)"
	Charset       string
	Collation     string
	Nullable      bool
	Fetch         FetchMode
	Default       Value
	Timestamp     bool // TIMESTAMP columns travel as Unix time
	Float         bool // FLOAT and DOUBLE columns are never pushed down
	AutoIncrement bool
}

// IndexDef is one index. Index 0 is the identity key when the table has one.
type IndexDef struct {
	ID             IndexID
	Name           string
	Primary        bool
	Unique         bool
	Columns        []uint16
	PartLens       []uint32 // sort-image length of each key part
	OrderProven    bool
	EqualityProven bool
	AddedLocally   bool
}

// FKAction is a foreign-key referential action.
type FKAction uint8

const (
	FKRestrict FKAction = iota
	FKNoAction
	FKCascade
	FKSetNull
)

// ForeignKeyDef is one foreign key declared on a child table.
type ForeignKeyDef struct {
	Name          string
	Columns       []uint16 // child columns
	ChildIndex    IndexID  // the child's supporting index
	ParentDB      string
	ParentTable   string
	ParentColumns []string
	ParentIndex   IndexID // resolved by the catalog: the parent key referenced
	OnDelete      FKAction
	OnUpdate      FKAction
}

// SourceMapping ties a mirrored table to its Source table.
type SourceMapping struct {
	Database    string
	Table       string
	Definition  string // the Source's SHOW CREATE TABLE
	Fingerprint string
	AutoInc     uint64 // the Source's AUTO_INCREMENT at discovery (D-15)
	RowsEst     uint64
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
	Kind      LocalChangeKind
	Object    string
	Statement string
}

// TableDef is one table of the catalog of record.
type TableDef struct {
	ID           TableID
	Database     string
	Name         string
	Kind         TableKind
	Version      uint64
	Columns      []ColumnDef
	Indexes      []IndexDef
	Identity     IndexID
	ForeignKeys  []ForeignKeyDef
	Definition   string
	Source       *SourceMapping
	LocalChanges []LocalChange
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

// ObjectKind classifies a non-table catalog object.
type ObjectKind uint8

const (
	View ObjectKind = iota
	Procedure
	Function
	Trigger
)

func (k ObjectKind) String() string {
	return [...]string{"view", "procedure", "function", "trigger"}[k]
}

// ObjectOrigin says who created an object.
type ObjectOrigin uint8

const (
	ObjectFromSource ObjectOrigin = iota
	ObjectClient
)

// Object is a view, routine or trigger.
type Object struct {
	Kind      ObjectKind
	Database  string
	Name      string
	Statement string
	SQLMode   string
	Definer   string
	Origin    ObjectOrigin
	Seq       uint64
	Dropped   bool // a DROP captured from a client
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
	Kind    CatalogChangeKind
	Table   TableDef // the new definition (Create, Alter, Rename)
	ID      TableID  // the table changed (Drop, Rename, Alter)
	NewDB   string   // Rename
	NewName string   // Rename
	Local   *LocalChange
	Source  bool // applied from the Source through a control session
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
