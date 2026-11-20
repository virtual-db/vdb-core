// Package core — types.go
//
// The public types of the core (implementation plan doc 03 §3). Each is an
// alias of internal/types, where the internal packages share them; an engine
// imports only this package.
package core

import "github.com/virtual-db/vdb-core/internal/types"

// Types.
type (
	TableID           = types.TableID
	IndexID           = types.IndexID
	KeyImage          = types.KeyImage
	SourceKey         = types.SourceKey
	Value             = types.Value
	Row               = types.Row
	Key               = types.Key
	Origin            = types.Origin
	RowRef            = types.RowRef
	SessionInfo       = types.SessionInfo
	LockMode          = types.LockMode
	Resource          = types.Resource
	LockModes         = types.LockModes
	LockMark          = types.LockMark
	Locks             = types.Locks
	ReadLocks         = types.ReadLocks
	SnapshotScope     = types.SnapshotScope
	ReadView          = types.ReadView
	EvalContext       = types.EvalContext
	StmtContext       = types.StmtContext
	ImageRange        = types.ImageRange
	KeyRange          = types.KeyRange
	Direction         = types.Direction
	ColumnSet         = types.ColumnSet
	ScanRequest       = types.ScanRequest
	SourceScan        = types.SourceScan
	WriteResult       = types.WriteResult
	Cursor            = types.Cursor
	Batch             = types.Batch
	Source            = types.Source
	SourceSnapshot    = types.SourceSnapshot
	SourceCursor      = types.SourceCursor
	SourceRows        = types.SourceRows
	MemoryProbe       = types.MemoryProbe
	RowProvider       = types.RowProvider
	WaitFunc          = types.WaitFunc
	ErrorCode         = types.ErrorCode
	Error             = types.Error
	QueryInfo         = types.QueryInfo
	TableStats        = types.TableStats
	DeltaStats        = types.DeltaStats
	LockStats         = types.LockStats
	MemoryStats       = types.MemoryStats
	ChangeKind        = types.ChangeKind
	RowOp             = types.RowOp
	RowChange         = types.RowChange
	TouchChange       = types.TouchChange
	ChangeSet         = types.ChangeSet
	TableKind         = types.TableKind
	FetchMode         = types.FetchMode
	ColumnDef         = types.ColumnDef
	IndexDef          = types.IndexDef
	FKAction          = types.FKAction
	ForeignKeyDef     = types.ForeignKeyDef
	SourceMapping     = types.SourceMapping
	LocalChangeKind   = types.LocalChangeKind
	LocalChange       = types.LocalChange
	TableDef          = types.TableDef
	ObjectKind        = types.ObjectKind
	ObjectOrigin      = types.ObjectOrigin
	Object            = types.Object
	CatalogChangeKind = types.CatalogChangeKind
	CatalogChange     = types.CatalogChange
	SourceChangeKind  = types.SourceChangeKind
	SourceChange      = types.SourceChange
	DriverAPI         = types.DriverAPI
	Session           = types.Session
	ScanHandle        = types.ScanHandle
	Catalog           = types.Catalog
	DeltaRowInfo      = types.DeltaRowInfo
	TouchedInfo       = types.TouchedInfo
	DeltaReader       = types.DeltaReader
)

// Constants.
const (
	OriginSource         = types.OriginSource
	OriginDelta          = types.OriginDelta
	Forward              = types.Forward
	Backward             = types.Backward
	ErrNone              = types.ErrNone
	PerTransaction       = types.PerTransaction
	PerStatement         = types.PerStatement
	ViewSnapshot         = types.ViewSnapshot
	ViewUncommitted      = types.ViewUncommitted
	ErrDuplicateKey      = types.ErrDuplicateKey
	ErrLockWaitTimeout   = types.ErrLockWaitTimeout
	ErrDeadlock          = types.ErrDeadlock
	ErrDeltaFull         = types.ErrDeltaFull
	ErrLockMemory        = types.ErrLockMemory
	ErrRefused           = types.ErrRefused
	ErrSource            = types.ErrSource
	ErrKilled            = types.ErrKilled
	ErrInternal          = types.ErrInternal
	ChangeCommit         = types.ChangeCommit
	ChangeCaptured       = types.ChangeCaptured
	ChangeSource         = types.ChangeSource
	RowPut               = types.RowPut
	RowDelete            = types.RowDelete
	Mirrored             = types.Mirrored
	Local                = types.Local
	Detached             = types.Detached
	Provided             = types.Provided
	FromSource           = types.FromSource
	Default              = types.Default
	Virtual              = types.Virtual
	Transformed          = types.Transformed
	WholeRow             = types.WholeRow
	FKRestrict           = types.FKRestrict
	FKNoAction           = types.FKNoAction
	FKCascade            = types.FKCascade
	FKSetNull            = types.FKSetNull
	AddColumn            = types.AddColumn
	DropColumn           = types.DropColumn
	AddIndex             = types.AddIndex
	DropIndex            = types.DropIndex
	ChangeDefault        = types.ChangeDefault
	AddForeignKey        = types.AddForeignKey
	DropForeignKey       = types.DropForeignKey
	OtherChange          = types.OtherChange
	View                 = types.View
	Procedure            = types.Procedure
	Function             = types.Function
	Trigger              = types.Trigger
	ObjectFromSource     = types.ObjectFromSource
	ObjectClient         = types.ObjectClient
	CreateTable          = types.CreateTable
	DropTable            = types.DropTable
	RenameTable          = types.RenameTable
	AlterTable           = types.AlterTable
	DetachTable          = types.DetachTable
	SourceTableAdded     = types.SourceTableAdded
	SourceTableChanged   = types.SourceTableChanged
	SourceTableDropped   = types.SourceTableDropped
	SourceTableRenamed   = types.SourceTableRenamed
	SourceObjectChanged  = types.SourceObjectChanged
	SourceObjectDropped  = types.SourceObjectDropped
	SourceStatsRefreshed = types.SourceStatsRefreshed
)

// Functions.
var (
	Errorf          = types.Errorf
	AsError         = types.AsError
	AsErrorOrNil    = types.AsErrorOrNil
	EncodeSourceKey = types.EncodeSourceKey
	DecodeSourceKey = types.DecodeSourceKey
	RowSize         = types.RowSize
	CloneRow        = types.CloneRow
	Project         = types.Project
)

// LockKey is the lock key of a row: its Source key bytes when it has them,
// else its identity image (see ReadLocks).
func LockKey(sk SourceKey, img KeyImage) KeyImage { return types.LockKey(sk, img) }
