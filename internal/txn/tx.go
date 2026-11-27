// Package txn implements transactions (implementation plan doc 04 §2): the
// snapshot pair, the draft each transaction writes into, statement and named
// savepoints, DDL staged with the transaction, and commit.
package txn

// Package txn — tx.go

import (
	"bytes"
	"sync"

	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/lock"
	"github.com/virtual-db/vdb-core/internal/types"
)

type savepoint struct {
	name string // empty for the statement savepoint
	mark delta.Mark
	ddl  int
}

// Tx is one transaction. Used by one goroutine at a time; mu guards the draft
// for uncommitted-view readers in other sessions.
type Tx struct {
	m        *Manager
	ID       uint64
	Info     types.SessionInfo
	ReadOnly bool
	Ctx      types.StmtContext

	root    *delta.Root
	src     types.SourceSnapshot
	srcOpen bool

	mu    sync.Mutex
	draft *delta.Draft

	stmt       *savepoint
	savepoints []savepoint
	ddl        []types.CatalogChange
	own        map[types.TableID]map[string]bool // identity images this tx wrote
	ownTouch   map[types.TableID]map[string]bool // Source keys this tx touched

	Killed <-chan struct{}
	done   bool
}

// Manager returns the transaction's manager.
func (tx *Tx) Manager() *Manager { return tx.m }

// Begin pushes the statement savepoint and records the statement's context.
func (tx *Tx) Begin(ctx types.StmtContext) {
	tx.Ctx = ctx
	sp := savepoint{mark: tx.mark(), ddl: len(tx.ddl)}
	tx.stmt = &sp
}

func (tx *Tx) mark() delta.Mark {
	if tx.draft == nil {
		return 0
	}
	return tx.draft.Mark()
}

// End pops the statement savepoint. With a per-statement snapshot scope the
// snapshot pair is dropped so the next statement sees the latest committed state.
func (tx *Tx) End() {
	tx.stmt = nil
	if tx.Ctx.Snapshot == types.PerStatement {
		tx.dropPair()
	}
}

// StmtRollback undoes the statement. It releases no lock: whether the
// statement's locks go too is the engine's rule (Locks.ReleaseSince).
func (tx *Tx) StmtRollback() error {
	if tx.stmt == nil {
		return nil
	}
	sp := *tx.stmt
	if err := tx.restore(sp); err != nil {
		return err
	}
	tx.stmt = nil
	return nil
}

func (tx *Tx) restore(sp savepoint) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.draft != nil {
		if err := tx.draft.Restore(sp.mark); err != nil {
			return err
		}
		tx.rebuildOwn()
	}
	tx.ddl = tx.ddl[:sp.ddl]
	return nil
}

func (tx *Tx) rebuildOwn() {
	tx.own, tx.ownTouch = nil, nil
	for _, op := range tx.draft.Ops() {
		tx.noteOwn(op)
	}
}

func (tx *Tx) noteOwn(op delta.Op) {
	switch op.Kind {
	case delta.OpPutRow, delta.OpDelRow:
		img := op.Image
		if op.Row != nil {
			img = op.Row.Image
		}
		if tx.own == nil {
			tx.own = map[types.TableID]map[string]bool{}
		}
		if tx.own[op.Table] == nil {
			tx.own[op.Table] = map[string]bool{}
		}
		tx.own[op.Table][string(img)] = true
	case delta.OpTouch:
		if tx.ownTouch == nil {
			tx.ownTouch = map[types.TableID]map[string]bool{}
		}
		if tx.ownTouch[op.Table] == nil {
			tx.ownTouch[op.Table] = map[string]bool{}
		}
		tx.ownTouch[op.Table][string(op.Key)] = true
	}
}

// Savepoint pushes a named savepoint, replacing one of the same name.
func (tx *Tx) Savepoint(name string) {
	tx.Release(name)
	tx.savepoints = append(tx.savepoints, savepoint{name: name, mark: tx.mark(), ddl: len(tx.ddl)})
}

// RollbackTo undoes to a named savepoint and drops later ones.
func (tx *Tx) RollbackTo(name string) error {
	for i := len(tx.savepoints) - 1; i >= 0; i-- {
		if tx.savepoints[i].name == name {
			if err := tx.restore(tx.savepoints[i]); err != nil {
				return err
			}
			tx.savepoints = tx.savepoints[:i+1]
			return nil
		}
	}
	return types.Errorf(types.ErrRefused, "SAVEPOINT "+name+" does not exist")
}

// Release drops a named savepoint and later ones.
func (tx *Tx) Release(name string) {
	for i := len(tx.savepoints) - 1; i >= 0; i-- {
		if tx.savepoints[i].name == name {
			tx.savepoints = tx.savepoints[:i]
			return
		}
	}
}

// ── snapshot pair ──

// TakePair takes the snapshot pair now (START TRANSACTION WITH CONSISTENT
// SNAPSHOT, or the first read). Under the commit lock's read side, the Delta
// root is taken and the Source snapshot opened before it is released, so no
// commit lands between the two halves (D-35).
func (tx *Tx) TakePair(needSource bool) error {
	if tx.root != nil && (!needSource || tx.srcOpen) {
		return nil
	}
	// Reserve the Source connection before the commit lock: waiting for one
	// under the lock's read side would hold every commit behind it.
	var src types.SourceSnapshot
	if needSource && !tx.srcOpen && tx.m.source != nil {
		s, err := tx.m.source.Reserve(tx.Info, tx.Ctx)
		if err != nil {
			return &types.Error{Code: types.ErrSource, Msg: err.Error()}
		}
		src = s
	}
	tx.m.commitMu.RLock()
	defer tx.m.commitMu.RUnlock()
	if tx.root == nil {
		root := tx.m.store.Snapshot()
		tx.root = root
		if tx.draft != nil && tx.draft.Base() != root {
			tx.mu.Lock()
			nd, err := tx.draft.Rebase(root)
			if err != nil {
				tx.mu.Unlock()
				if src != nil {
					_ = src.Close()
				}
				return err
			}
			tx.draft = nd
			tx.mu.Unlock()
		}
	}
	if src != nil {
		if err := src.Start(); err != nil {
			_ = src.Close()
			return &types.Error{Code: types.ErrSource, Msg: err.Error()}
		}
		tx.src, tx.srcOpen = src, true
	}
	return nil
}

func (tx *Tx) dropPair() {
	tx.root = nil
	if tx.srcOpen {
		_ = tx.src.Close()
		tx.src, tx.srcOpen = nil, false
	}
}

// Source returns the Source half of the snapshot pair, opening it if needed.
func (tx *Tx) Source() (types.SourceSnapshot, error) {
	if err := tx.TakePair(true); err != nil {
		return nil, err
	}
	if tx.src == nil {
		return nil, types.Errorf(types.ErrSource, "no Source adapter")
	}
	return tx.src, nil
}

// View returns the transaction's view of a table's Delta: its own writes over
// its snapshot; with the uncommitted read view, the current committed state
// overlaid with every open transaction's writes. Nil means no Delta rows.
func (tx *Tx) View(t types.TableID) (*delta.Table, error) {
	if err := tx.TakePair(false); err != nil {
		return nil, err
	}
	if tx.Ctx.View == types.ViewUncommitted {
		return tx.m.uncommitted(tx, t)
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.draft != nil {
		if tb := tx.draft.Table(t); tb != nil {
			return tb.Copy(), nil
		}
		return nil, nil
	}
	if tb := tx.root.Table(t); tb != nil {
		return tb.Copy(), nil
	}
	return nil, nil
}

// Probe returns the same view of a table's Delta as View, for point lookups
// (Get, Holds, IsTouched, IndexHas) only, without View's Copy().
//
// A Copy() marks every tree of the draft's table as shared, so the draft's
// next write copies the whole root-to-leaf path in each tree. Taking one per
// written row (the uniqueness check) made every write copy B-tree nodes.
//
// The returned table is the draft's own (or the published root's) and must
// not be iterated or held across a write of this transaction: the caller
// uses it and drops it before its next Apply, Restore or Commit. Only the
// transaction's own goroutine writes its draft; other sessions read only the
// draft's ops (Manager.uncommitted), so the lookups need no copy.
func (tx *Tx) Probe(t types.TableID) (*delta.Table, error) {
	if err := tx.TakePair(false); err != nil {
		return nil, err
	}
	if tx.Ctx.View == types.ViewUncommitted {
		return tx.m.uncommitted(tx, t)
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.draft != nil {
		return tx.draft.Table(t), nil
	}
	return tx.root.Table(t), nil
}

// Latest returns the newest version of a Delta row for a locking read: the
// transaction's own write, else the current committed state, not the snapshot
// (product doc 05 §3.2). It also reports whether the Source key is touched.
func (tx *Tx) Latest(t types.TableID, img types.KeyImage, sk types.SourceKey) (row *delta.DeltaRow, touched bool) {
	tx.mu.Lock()
	ownRow := tx.own[t][string(img)]
	ownTouched := sk != nil && tx.ownTouch[t][string(sk)]
	var dt *delta.Table
	if tx.draft != nil {
		dt = tx.draft.Table(t)
	}
	tx.mu.Unlock()
	committed := tx.m.store.Snapshot().Table(t)
	if ownRow && dt != nil {
		row, _ = dt.Get(img)
	} else if committed != nil && img != nil {
		row, _ = committed.Get(img)
	}
	if sk != nil {
		if ownTouched && dt != nil {
			_, touched = dt.IsTouched(sk)
		} else if committed != nil {
			_, touched = committed.IsTouched(sk)
		}
	}
	return row, touched
}

// ── writes ──

// Apply records a Delta change in the transaction's draft.
func (tx *Tx) Apply(op delta.Op) error {
	if err := tx.TakePair(false); err != nil {
		return err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.draft == nil {
		tx.draft = tx.root.Fork()
	}
	if err := tx.draft.Apply(op); err != nil {
		return err
	}
	tx.noteOwn(op)
	return nil
}

// Stage stages a DDL change, published to the catalog at commit.
func (tx *Tx) Stage(c types.CatalogChange) { tx.ddl = append(tx.ddl, c) }

// Staged returns the staged DDL changes.
func (tx *Tx) Staged() []types.CatalogChange { return tx.ddl }

// Lock takes a lock for this transaction in the lock table.
func (tx *Tx) Lock(r types.Resource, mode types.LockMode, wait types.WaitFunc) error {
	w := lock.Waiter{Tx: tx.ID, Conn: tx.Info.ConnID, Timeout: tx.Ctx.LockWait, Killed: tx.Killed, OnWait: wait}
	return tx.m.locks.Acquire(w, r, mode)
}

// Writes reports whether the transaction changed anything.
func (tx *Tx) Writes() bool {
	return (tx.draft != nil && !tx.draft.Empty()) || len(tx.ddl) > 0
}

// Ops returns the draft's ops.
func (tx *Tx) Ops() []delta.Op {
	if tx.draft == nil {
		return nil
	}
	return tx.draft.Ops()
}

// Draft returns the draft (nil before the first write).
func (tx *Tx) Draft() *delta.Draft { return tx.draft }

// Done reports whether the transaction has ended.
func (tx *Tx) Done() bool { return tx.done }

// Rollback discards the draft, releases every lock and closes the Source snapshot.
func (tx *Tx) Rollback() {
	if tx.done {
		return
	}
	tx.done = true
	tx.mu.Lock()
	tx.draft = nil
	tx.mu.Unlock()
	tx.m.forget(tx)
	tx.m.locks.ReleaseAll(tx.ID)
	tx.dropPair()
}

// sameImage compares images.
func sameImage(a, b types.KeyImage) bool { return bytes.Equal(a, b) }

// Def returns a table's definition as this transaction sees it: a change it
// staged (CREATE TABLE ... SELECT inserts into a table not yet committed), else
// the catalog's.
func (tx *Tx) Def(id types.TableID) (types.TableDef, bool) {
	for i := len(tx.ddl) - 1; i >= 0; i-- {
		c := tx.ddl[i]
		switch c.Kind {
		case types.CreateTable, types.AlterTable:
			if c.Table.ID == id {
				return c.Table, true
			}
		case types.DropTable:
			if c.ID == id {
				return types.TableDef{}, false
			}
		}
	}
	return tx.m.catalog.Table(id)
}
