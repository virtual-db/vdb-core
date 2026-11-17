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
	name  string // empty for the statement savepoint
	mark  delta.Mark
	ddl   int
	locks int
}

// Tx is one transaction. Used by one goroutine at a time; mu guards the draft
// for READ UNCOMMITTED readers in other sessions.
type Tx struct {
	m        *Manager
	ID       uint64
	Info     types.SessionInfo
	Iso      types.Isolation
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
	// AutoInc records counter moves for the change set.
	AutoInc  []types.AutoIncMove
	lockLog  []lockKey
	own      map[types.TableID]map[string]bool // identity images this tx wrote
	ownTouch map[types.TableID]map[string]bool // Source keys this tx touched

	// LookupCache answers keys already asked inside one Source snapshot (D-68).
	// It holds Source rows only and is dropped with the snapshot.
	LookupCache map[string]CachedLookup

	Killed <-chan struct{}
	OnWait types.WaitFunc
	Images types.ImageFunc
	done   bool
}

type lockKey struct {
	table types.TableID
	key   string
}

// Manager returns the transaction's manager.
func (tx *Tx) Manager() *Manager { return tx.m }

// Begin pushes the statement savepoint and records the statement's context.
func (tx *Tx) Begin(ctx types.StmtContext) {
	tx.Ctx = ctx
	sp := savepoint{mark: tx.mark(), ddl: len(tx.ddl), locks: len(tx.lockLog)}
	tx.stmt = &sp
}

func (tx *Tx) mark() delta.Mark {
	if tx.draft == nil {
		return 0
	}
	return tx.draft.Mark()
}

// End pops the statement savepoint. At READ COMMITTED the snapshot pair is
// dropped so the next statement sees the latest committed state.
func (tx *Tx) End() {
	tx.stmt = nil
	if tx.Iso == types.ReadCommitted || tx.Iso == types.ReadUncommitted {
		tx.dropPair()
		tx.m.locks.ReleaseGaps(tx.ID)
	}
}

// StmtRollback undoes the statement. Locks taken by it are released only at
// READ COMMITTED, as InnoDB does.
func (tx *Tx) StmtRollback() error {
	if tx.stmt == nil {
		return nil
	}
	sp := *tx.stmt
	if err := tx.restore(sp); err != nil {
		return err
	}
	if tx.Iso == types.ReadCommitted || tx.Iso == types.ReadUncommitted {
		dropped := map[lockKey]bool{}
		for _, k := range tx.lockLog[sp.locks:] {
			dropped[k] = true
		}
		tx.m.locks.ReleaseIf(tx.ID, func(t types.TableID, key types.KeyImage) bool { return dropped[lockKey{t, string(key)}] })
		tx.lockLog = tx.lockLog[:sp.locks]
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
	tx.savepoints = append(tx.savepoints, savepoint{name: name, mark: tx.mark(), ddl: len(tx.ddl), locks: len(tx.lockLog)})
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
				return err
			}
			tx.draft = nd
			tx.mu.Unlock()
		}
	}
	if needSource && !tx.srcOpen && tx.m.source != nil {
		s, err := tx.m.source.Snapshot(tx.Info, tx.Ctx)
		if err != nil {
			return &types.Error{Code: types.ErrSource, Msg: err.Error()}
		}
		tx.src, tx.srcOpen = s, true
	}
	return nil
}

// CachedLookup is the Source's answer for one key.
type CachedLookup struct {
	Rows []types.Row
	Keys []types.SourceKey
}

func (tx *Tx) dropPair() {
	tx.root = nil
	tx.LookupCache = nil
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
// its snapshot. At READ UNCOMMITTED it is the current committed state overlaid
// with every open transaction's writes (D-63). Nil means no Delta rows.
func (tx *Tx) View(t types.TableID) (*delta.Table, error) {
	if err := tx.TakePair(false); err != nil {
		return nil, err
	}
	if tx.Iso == types.ReadUncommitted {
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

// Lock takes a lock for this transaction and logs record locks for
// statement rollback.
func (tx *Tx) Lock(r lock.Request) error {
	w := lock.Waiter{Tx: tx.ID, Conn: tx.Info.ConnID, Timeout: tx.Ctx.LockWait, Killed: tx.Killed, OnWait: tx.OnWait}
	if err := tx.m.locks.Acquire(w, r); err != nil {
		return err
	}
	if r.Kind == lock.Record {
		tx.lockLog = append(tx.lockLog, lockKey{r.Table, string(r.Key)})
	}
	return nil
}

// Gaps reports whether this transaction takes gap and interval locks.
func (tx *Tx) Gaps() bool {
	return tx.Iso == types.RepeatableRead || tx.Iso == types.Serializable
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
