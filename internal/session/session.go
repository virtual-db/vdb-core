// Package session holds per-session state (implementation plan doc 04 §4):
// the session's identity, its open transaction, its open scans and its kill
// flag, and the registry of sessions by mysqld connection id. Session
// implements types.Session by calling txn, merge and write, and runs the
// transaction and write pipelines through a Runner.
package session

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/merge"
	"github.com/virtual-db/vdb-core/internal/payloads"
	"github.com/virtual-db/vdb-core/internal/points"
	"github.com/virtual-db/vdb-core/internal/txn"
	"github.com/virtual-db/vdb-core/internal/types"
	"github.com/virtual-db/vdb-core/internal/write"
)

// Runner runs a pipeline.
type Runner interface {
	Process(pipeline string, payload any) (any, error)
}

// Deps are what sessions need.
type Deps struct {
	Txn    *txn.Manager
	Merge  *merge.Merge
	Write  *write.Writer
	Runner Runner
	// DefaultLockWait applies when a statement sends no lock wait timeout.
	DefaultLockWait time.Duration
}

// Registry maps mysqld connection ids to sessions. Safe for concurrent use.
type Registry struct {
	mu       sync.Mutex
	sessions map[uint64]*Session
	deps     Deps
}

// NewRegistry returns an empty registry.
func NewRegistry(d Deps) *Registry {
	return &Registry{sessions: map[uint64]*Session{}, deps: d}
}

// SetDeps installs the dependencies (wiring order).
func (r *Registry) SetDeps(d Deps) { r.deps = d }

// Open returns a new session for a connection, ending any older one with the same id.
func (r *Registry) Open(info types.SessionInfo) *Session {
	r.mu.Lock()
	old := r.sessions[info.ConnID]
	s := &Session{reg: r, info: info, scans: map[*merge.Handle]bool{}, kill: make(chan struct{})}
	r.sessions[info.ConnID] = s
	r.mu.Unlock()
	if old != nil {
		_ = old.End()
	}
	return s
}

// Get returns the session of a connection.
func (r *Registry) Get(connID uint64) (*Session, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.sessions[connID]
	return s, ok
}

// Tx returns the open transaction of a connection.
func (r *Registry) Tx(connID uint64) (*txn.Tx, bool) {
	s, ok := r.Get(connID)
	if !ok || s.tx == nil || s.tx.Done() {
		return nil, false
	}
	return s.tx, true
}

// Kill marks a session killed and ends any lock wait at once.
func (r *Registry) Kill(connID uint64) {
	if s, ok := r.Get(connID); ok {
		s.Kill()
	}
}

// All returns every session.
func (r *Registry) All() []*Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Session, 0, len(r.sessions))
	for _, s := range r.sessions {
		out = append(out, s)
	}
	return out
}

func (r *Registry) remove(s *Session) {
	r.mu.Lock()
	if r.sessions[s.info.ConnID] == s {
		delete(r.sessions, s.info.ConnID)
	}
	r.mu.Unlock()
}

// Session is one mysqld connection's state.
type Session struct {
	reg    *Registry
	info   types.SessionInfo
	tx     *txn.Tx
	ctx    types.StmtContext
	scans  map[*merge.Handle]bool
	killMu sync.Mutex
	kill   chan struct{}
	killed atomic.Bool
	wait   types.WaitFunc
	images types.ImageFunc
	ended  bool

	alterRefs []types.RowRef
	alterRows []types.Row
	alterPut  int
}

// Info returns the session's identity.
func (s *Session) Info() types.SessionInfo { return s.info }

// InTransaction reports whether a transaction is open.
func (s *Session) InTransaction() bool { return s.tx != nil && !s.tx.Done() }

// Tx returns the open transaction, if any.
func (s *Session) Tx() *txn.Tx {
	if s.InTransaction() {
		return s.tx
	}
	return nil
}

// Kill sets the killed flag and closes the kill channel.
func (s *Session) Kill() {
	s.killed.Store(true)
	s.killMu.Lock()
	select {
	case <-s.kill:
	default:
		close(s.kill)
	}
	s.killMu.Unlock()
}

func (s *Session) checkKilled() error {
	if s.killed.Load() {
		return &types.Error{Code: types.ErrKilled, Msg: "Query execution was interrupted"}
	}
	return nil
}

// SetCallbacks installs the wait and image callbacks for the next call.
func (s *Session) SetCallbacks(wait types.WaitFunc, images types.ImageFunc) {
	s.wait, s.images = wait, images
	if s.tx != nil {
		s.tx.OnWait, s.tx.Images = wait, images
	}
}

func (s *Session) deps() Deps { return s.reg.deps }

// StmtBegin starts a statement, opening a transaction when none is open.
func (s *Session) StmtBegin(ctx types.StmtContext) error {
	s.killMu.Lock()
	if s.killed.Load() {
		s.killed.Store(false)
		s.kill = make(chan struct{})
	}
	kill := s.kill
	s.killMu.Unlock()
	if ctx.LockWait <= 0 {
		ctx.LockWait = s.deps().DefaultLockWait
	}
	s.ctx = ctx
	if s.tx == nil || s.tx.Done() {
		s.tx = s.deps().Txn.Begin(s.info, ctx)
		if _, err := s.deps().Runner.Process(points.PipelineTransactionBegin, payloads.TransactionBeginPayload{
			ConnectionID: s.info.ConnID, ReadOnly: ctx.ReadOnly, Isolation: ctx.Isolation}); err != nil {
			s.tx.Rollback()
			s.tx = nil
			return types.AsError(err)
		}
	}
	s.tx.Killed, s.tx.OnWait, s.tx.Images = kill, s.wait, s.images
	s.tx.Begin(ctx)
	return nil
}

func (s *Session) needTx() (*txn.Tx, error) {
	if err := s.checkKilled(); err != nil {
		return nil, err
	}
	if s.tx == nil || s.tx.Done() {
		if err := s.StmtBegin(s.ctx); err != nil {
			return nil, err
		}
	}
	return s.tx, nil
}

// StmtEnd ends the statement.
func (s *Session) StmtEnd() error {
	s.closeScans()
	if s.tx != nil && !s.tx.Done() {
		s.tx.End()
	}
	return nil
}

// StmtRollback undoes the statement.
func (s *Session) StmtRollback() error {
	s.closeScans()
	if s.tx == nil || s.tx.Done() {
		return nil
	}
	return types.AsErrorOrNil(s.tx.StmtRollback())
}

// Snapshot takes the snapshot pair now (START TRANSACTION WITH CONSISTENT SNAPSHOT).
func (s *Session) Snapshot() error {
	tx, err := s.needTx()
	if err != nil {
		return err
	}
	return types.AsErrorOrNil(tx.TakePair(true))
}

// Commit commits the open transaction.
func (s *Session) Commit() error {
	s.closeScans()
	if s.tx == nil || s.tx.Done() {
		s.tx = nil
		return nil
	}
	tx := s.tx
	s.tx = nil
	if err := tx.Commit(); err != nil {
		return types.AsError(err)
	}
	return nil
}

// Rollback rolls back the open transaction.
func (s *Session) Rollback() error {
	s.closeScans()
	if s.tx == nil || s.tx.Done() {
		s.tx = nil
		return nil
	}
	tx := s.tx
	_, err := s.deps().Runner.Process(points.PipelineTransactionRollback, payloads.TransactionRollbackPayload{ConnectionID: s.info.ConnID})
	tx.Rollback()
	s.tx = nil
	return types.AsErrorOrNil(err)
}

// Savepoint pushes a named savepoint.
func (s *Session) Savepoint(name string) error {
	tx, err := s.needTx()
	if err != nil {
		return err
	}
	tx.Savepoint(name)
	return nil
}

// RollbackTo undoes to a named savepoint.
func (s *Session) RollbackTo(name string) error {
	if s.tx == nil || s.tx.Done() {
		return nil
	}
	if err := s.tx.RollbackTo(name); err != nil {
		return types.AsError(err)
	}
	_, err := s.deps().Runner.Process(points.PipelineTransactionRollback, payloads.TransactionRollbackPayload{ConnectionID: s.info.ConnID, Savepoint: name})
	return types.AsErrorOrNil(err)
}

// Release drops a named savepoint.
func (s *Session) Release(name string) error {
	if s.tx != nil {
		s.tx.Release(name)
	}
	return nil
}

// Scan opens a scan.
func (s *Session) Scan(req types.ScanRequest) (types.ScanHandle, error) {
	tx, err := s.needTx()
	if err != nil {
		return nil, err
	}
	h, err := s.deps().Merge.Scan(tx, req)
	if err != nil {
		return nil, types.AsError(err)
	}
	s.scans[h] = true
	return &handle{Handle: h, s: s}, nil
}

type handle struct {
	*merge.Handle
	s *Session
}

func (h *handle) Close() error {
	delete(h.s.scans, h.Handle)
	return h.Handle.Close()
}

func (s *Session) closeScans() {
	for h := range s.scans {
		_ = h.Close()
	}
	s.scans = map[*merge.Handle]bool{}
}

// Lookup answers keys; the second result is the shadow set for Source rows.
func (s *Session) Lookup(t types.TableID, idx types.IndexID, keys []types.Key, cols types.ColumnSet, mode types.LockMode) (types.Batch, []types.KeyImage, error) {
	tx, err := s.needTx()
	if err != nil {
		return types.Batch{}, nil, err
	}
	r, err := s.deps().Merge.Lookup(tx, t, idx, keys, cols, mode)
	if err != nil {
		return types.Batch{}, nil, types.AsError(err)
	}
	return r.Batch, r.Shadows, nil
}

// Count returns the exact row count.
func (s *Session) Count(t types.TableID) (uint64, bool, error) {
	tx, err := s.needTx()
	if err != nil {
		return 0, false, err
	}
	n, declined, err := s.deps().Merge.Count(tx, t)
	return n, declined, types.AsErrorOrNil(err)
}

func (s *Session) tableName(tx *txn.Tx, t types.TableID) (string, []string) {
	d, ok := tx.Def(t)
	if !ok {
		return "", nil
	}
	cols := make([]string, len(d.Columns))
	for i, c := range d.Columns {
		cols[i] = c.Name
	}
	return d.Database + "." + d.Name, cols
}

func (s *Session) writeCheck(tx *txn.Tx) error {
	if tx.ReadOnly {
		return &types.Error{Code: types.ErrRefused, Msg: "Cannot execute statement in a READ ONLY transaction."}
	}
	return nil
}

// Insert inserts rows through vdb.write.insert.
func (s *Session) Insert(t types.TableID, rows []types.Row, images [][]types.KeyImage) ([]types.WriteResult, error) {
	tx, err := s.needTx()
	if err != nil {
		return nil, err
	}
	if err := s.writeCheck(tx); err != nil {
		return nil, err
	}
	name, cols := s.tableName(tx, t)
	out, err := s.deps().Runner.Process(points.PipelineWriteInsert, payloads.WriteInsertPayload{
		ConnectionID: s.info.ConnID, Table: name, TableID: t, Columns: cols, Rows: rows, Images: images})
	if err != nil {
		return nil, types.AsError(err)
	}
	p, err := payloads.Decode[payloads.WriteInsertPayload](out)
	if err != nil {
		return nil, types.AsError(err)
	}
	return p.Results, nil
}

// Update replaces a row through vdb.write.update.
func (s *Session) Update(t types.TableID, old types.RowRef, row types.Row, images []types.KeyImage) error {
	tx, err := s.needTx()
	if err != nil {
		return err
	}
	if err := s.writeCheck(tx); err != nil {
		return err
	}
	name, cols := s.tableName(tx, t)
	_, err = s.deps().Runner.Process(points.PipelineWriteUpdate, payloads.WriteUpdatePayload{
		ConnectionID: s.info.ConnID, Table: name, TableID: t, Columns: cols, Old: old, New: row, Images: images})
	return types.AsErrorOrNil(err)
}

// Delete removes a row through vdb.write.delete.
func (s *Session) Delete(t types.TableID, old types.RowRef) error {
	tx, err := s.needTx()
	if err != nil {
		return err
	}
	if err := s.writeCheck(tx); err != nil {
		return err
	}
	name, _ := s.tableName(tx, t)
	_, err = s.deps().Runner.Process(points.PipelineWriteDelete, payloads.WriteDeletePayload{
		ConnectionID: s.info.ConnID, Table: name, TableID: t, Old: old})
	return types.AsErrorOrNil(err)
}

// Truncate detaches a table through vdb.write.truncate.
func (s *Session) Truncate(t types.TableID) error {
	tx, err := s.needTx()
	if err != nil {
		return err
	}
	name, _ := s.tableName(tx, t)
	_, err = s.deps().Runner.Process(points.PipelineWriteTruncate, payloads.WriteTruncatePayload{
		ConnectionID: s.info.ConnID, Table: name, TableID: t})
	return types.AsErrorOrNil(err)
}

// AutoIncrement hands out a block of values.
func (s *Session) AutoIncrement(t types.TableID, n uint64) (uint64, uint64, error) {
	tx, err := s.needTx()
	if err != nil {
		return 0, 0, err
	}
	first, granted, mv, err := s.deps().Txn.AutoIncrement(t, n, 1, 1)
	if err != nil {
		return 0, 0, types.AsError(err)
	}
	tx.AutoInc = append(tx.AutoInc, mv)
	return first, granted, nil
}

// ApplyDDL stages a client DDL change.
func (s *Session) ApplyDDL(c types.CatalogChange) (types.TableID, error) {
	c.Source = false
	return s.stage(c)
}

// ApplySourceDDL stages DDL from a control session, recorded as Source-derived.
func (s *Session) ApplySourceDDL(c types.CatalogChange) (types.TableID, error) {
	c.Source = true
	return s.stage(c)
}

func (s *Session) stage(c types.CatalogChange) (types.TableID, error) {
	tx, err := s.needTx()
	if err != nil {
		return 0, err
	}
	m := s.deps().Txn
	switch c.Kind {
	case types.CreateTable:
		if c.Table.ID == 0 {
			c.Table.ID = m.Catalog().NextID()
		}
		if c.Table.Version == 0 {
			c.Table.Version = 1
		}
		if c.Table.Kind == types.Local || c.Table.Kind == types.Provided {
			if err := tx.Apply(delta.Op{Kind: delta.OpCreateTable, Table: c.Table.ID, TKind: c.Table.Kind, Version: c.Table.Version}); err != nil {
				return 0, types.AsError(err)
			}
		}
		tx.Stage(c)
		return c.Table.ID, nil
	case types.DropTable:
		if err := tx.Apply(delta.Op{Kind: delta.OpDropTable, Table: c.ID}); err != nil {
			return 0, types.AsError(err)
		}
		tx.Stage(c)
		return c.ID, nil
	case types.RenameTable:
		tx.Stage(c)
		return c.ID, nil
	case types.AlterTable:
		cur, ok := tx.Def(c.ID)
		if !ok {
			return 0, &types.Error{Code: types.ErrRefused, Table: c.ID, Msg: "ALTER of a table not in the catalog of record"}
		}
		c.Table.ID = c.ID
		c.Table.Kind = cur.Kind
		if c.Table.Source == nil {
			c.Table.Source = cur.Source
		}
		c.Table.Version = cur.Version + 1
		if c.Local != nil && c.Local.Kind == types.AddForeignKey && tx.Ctx.ForeignKeyChecks {
			for _, f := range c.Table.ForeignKeys {
				if f.Name == c.Local.Object {
					tmp := c.Table
					if err := s.deps().Write.FK().ValidateExisting(tx, &tmp, f); err != nil {
						return 0, types.AsError(err)
					}
				}
			}
		}
		if err := tx.Apply(delta.Op{Kind: delta.OpSetVersion, Table: c.ID, TKind: cur.Kind, Version: c.Table.Version}); err != nil {
			return 0, types.AsError(err)
		}
		tx.Stage(c)
		return c.ID, nil
	}
	tx.Stage(c)
	return c.ID, nil
}

// AlterRows returns every Delta row of a table, in the draft's view, for an
// ALTER that converts them in vdb.so.
func (s *Session) AlterRows(t types.TableID) types.Cursor {
	tx, err := s.needTx()
	if err != nil {
		return errCursor{err}
	}
	view, err := tx.View(t)
	if err != nil {
		return errCursor{err}
	}
	s.alterRefs, s.alterRows, s.alterPut = nil, nil, 0
	var rows []*delta.DeltaRow
	if view != nil {
		rows = view.Rows()
	}
	return &alterCursor{s: s, rows: rows}
}

type alterCursor struct {
	s    *Session
	rows []*delta.DeltaRow
	i    int
}

func (c *alterCursor) Next(maxRows, maxBytes int) (types.Batch, error) {
	var b types.Batch
	size := 0
	for c.i < len(c.rows) {
		r := c.rows[c.i]
		c.i++
		ref := types.RowRef{Origin: r.Origin, PK: types.Key{Image: r.Image}, SourceKey: r.SourceKey}
		b.Rows = append(b.Rows, r.Row)
		b.Refs = append(b.Refs, ref)
		c.s.alterRefs = append(c.s.alterRefs, ref)
		size += types.RowSize(r.Row)
		if (maxRows > 0 && len(b.Rows) >= maxRows) || (maxBytes > 0 && size >= maxBytes) {
			break
		}
	}
	if len(b.Rows) == 0 {
		return b, io.EOF
	}
	return b, nil
}

func (c *alterCursor) Close() error { return nil }

type errCursor struct{ err error }

func (e errCursor) Next(int, int) (types.Batch, error) { return types.Batch{}, e.err }
func (e errCursor) Close() error                       { return nil }

// AlterPut replaces the table's Delta rows with the converted ones, in the
// order AlterRows yielded them, and rebuilds the index trees.
func (s *Session) AlterPut(t types.TableID, rows []types.Row, images [][]types.KeyImage) error {
	tx, err := s.needTx()
	if err != nil {
		return err
	}
	d, ok := tx.Def(t)
	if !ok {
		return &types.Error{Code: types.ErrRefused, Table: t, Msg: "unknown table"}
	}
	var drs []*delta.DeltaRow
	for i, row := range rows {
		k := s.alterPut + i
		var ref types.RowRef
		if k < len(s.alterRefs) {
			ref = s.alterRefs[k]
		}
		var ims []types.KeyImage
		if i < len(images) {
			ims = images[i]
		}
		img := ref.PK.Image
		if d.Identity != types.WholeRow && int(d.Identity) < len(ims) && ims[d.Identity] != nil {
			img = ims[d.Identity]
		}
		drs = append(drs, &delta.DeltaRow{Image: img, Row: types.CloneRow(row), Origin: ref.Origin, SourceKey: ref.SourceKey, Images: ims})
	}
	first := s.alterPut == 0
	s.alterPut += len(rows)
	if first {
		return types.AsErrorOrNil(tx.Apply(delta.Op{Kind: delta.OpReplaceRows, Table: t, Rows: drs, TKind: d.Kind, Version: d.Version}))
	}
	for _, r := range drs {
		if err := tx.Apply(delta.Op{Kind: delta.OpPutRow, Table: t, Row: r, TKind: d.Kind, Version: d.Version}); err != nil {
			return types.AsError(err)
		}
	}
	return nil
}

// End closes every scan, rolls back, and forgets the session.
func (s *Session) End() error {
	if s.ended {
		return nil
	}
	s.ended = true
	s.closeScans()
	if s.tx != nil && !s.tx.Done() {
		_ = s.Rollback()
	}
	s.reg.remove(s)
	return nil
}
