// Package txn — manager.go
//
// The transaction manager: the commit lock, commit sequence numbers, open
// transactions (for READ UNCOMMITTED), auto-increment counters, and commit.
package txn

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/virtual-db/vdb-core/internal/catalog"
	"github.com/virtual-db/vdb-core/internal/changelog"
	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/lock"
	"github.com/virtual-db/vdb-core/internal/types"
)

// CommitFunc runs the vdb.transaction.commit pipeline for a change set. Its
// apply handler calls Manager.Publish. An error fails the commit.
type CommitFunc func(tx *Tx, cs *types.ChangeSet) error

// Manager owns the Delta store, the lock manager, the commit lock and the
// commit sequence. Safe for concurrent use.
type Manager struct {
	store    *delta.Store
	locks    *lock.Manager
	source   types.Source
	catalog  *catalog.Catalog
	Seq      *changelog.Sequence
	commitMu sync.RWMutex
	nextTx   atomic.Uint64
	commit   CommitFunc
	autoGap  uint64

	mu      sync.Mutex
	open    map[uint64]*Tx
	pending map[uint64]*Tx // by connection id, while its commit pipeline runs

	autoMu sync.Mutex
	auto   map[types.TableID]*counter
}

type counter struct {
	mu     sync.Mutex
	next   uint64
	seeded bool
}

// New returns a manager.
func New(store *delta.Store, locks *lock.Manager, cat *catalog.Catalog, seq *changelog.Sequence, autoGap uint64) *Manager {
	return &Manager{store: store, locks: locks, catalog: cat, Seq: seq, autoGap: autoGap,
		open: map[uint64]*Tx{}, pending: map[uint64]*Tx{}, auto: map[types.TableID]*counter{}}
}

// SetSource installs the engine's Source adapter.
func (m *Manager) SetSource(s types.Source) { m.source = s }

// SetCommit installs the function that runs the commit pipeline.
func (m *Manager) SetCommit(f CommitFunc) { m.commit = f }

// Store returns the Delta store.
func (m *Manager) Store() *delta.Store { return m.store }

// Locks returns the lock manager.
func (m *Manager) Locks() *lock.Manager { return m.locks }

// Catalog returns the catalog.
func (m *Manager) Catalog() *catalog.Catalog { return m.catalog }

// Begin starts a transaction. No snapshot is taken yet.
func (m *Manager) Begin(info types.SessionInfo, ctx types.StmtContext) *Tx {
	tx := &Tx{m: m, ID: m.nextTx.Add(1), Info: info, Iso: ctx.Isolation, ReadOnly: ctx.ReadOnly, Ctx: ctx}
	m.mu.Lock()
	m.open[tx.ID] = tx
	m.mu.Unlock()
	return tx
}

func (m *Manager) forget(tx *Tx) {
	m.mu.Lock()
	delete(m.open, tx.ID)
	m.mu.Unlock()
}

// uncommitted builds the READ UNCOMMITTED view of a table: the current
// committed state overlaid with every open transaction's writes to it. Row
// locks ensure at most one uncommitted version per row (D-63).
func (m *Manager) uncommitted(self *Tx, t types.TableID) (*delta.Table, error) {
	root := m.store.Snapshot()
	var ops []delta.Op
	m.mu.Lock()
	txs := make([]*Tx, 0, len(m.open))
	for _, o := range m.open {
		txs = append(txs, o)
	}
	m.mu.Unlock()
	for _, o := range txs {
		o.mu.Lock()
		if o.draft != nil {
			for _, op := range o.draft.Ops() {
				if op.Table == t {
					ops = append(ops, op)
				}
			}
		}
		o.mu.Unlock()
	}
	d := root.Fork()
	for _, op := range ops {
		if err := d.Apply(op); err != nil {
			return nil, err
		}
	}
	if tb := d.Table(t); tb != nil {
		return tb.Copy(), nil
	}
	return nil, nil
}

// Commit commits tx (doc 04 §2.4): take the commit lock, build the change set
// from the draft, run the commit pipeline (whose apply handler publishes),
// release the commit lock, then every lock, then the Source snapshot.
func (tx *Tx) Commit() error {
	if tx.done {
		return nil
	}
	m := tx.m
	if !tx.Writes() && len(tx.AutoInc) == 0 {
		tx.Rollback()
		return nil
	}
	m.commitMu.Lock()
	seq := m.Seq.Next()
	cs := changelog.FromOps(seq, types.ChangeCommit, tx.Ops())
	cs.Catalog = append(cs.Catalog, tx.ddl...)
	cs.AutoInc = append(cs.AutoInc, tx.AutoInc...)
	m.mu.Lock()
	m.pending[tx.Info.ConnID] = tx
	m.mu.Unlock()
	var err error
	if m.commit != nil {
		err = m.commit(tx, cs)
	} else {
		err = m.Publish(tx, cs)
	}
	m.mu.Lock()
	delete(m.pending, tx.Info.ConnID)
	m.mu.Unlock()
	m.commitMu.Unlock()
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("txn.commit: %w", err)
	}
	tx.done = true
	m.forget(tx)
	m.locks.ReleaseAll(tx.ID)
	tx.dropPair()
	return nil
}

// Pending returns the transaction committing on a connection, for the commit
// pipeline's apply handler.
func (m *Manager) Pending(connID uint64) (*Tx, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, ok := m.pending[connID]
	return tx, ok
}

// Publish makes a committing transaction's changes visible: the Delta first,
// then the catalog. Called with the commit lock held.
func (m *Manager) Publish(tx *Tx, cs *types.ChangeSet) error {
	if err := m.store.Publish(tx.Ops(), cs.Seq); err != nil {
		return err
	}
	for _, c := range tx.ddl {
		if err := m.catalog.Apply(c); err != nil {
			return err
		}
	}
	return nil
}

// Replay applies a change set outside any transaction (restore at start, and
// captured or Source change sets). Catalog changes are applied to the catalog.
func (m *Manager) Replay(cs *types.ChangeSet) error {
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	for _, c := range cs.Catalog {
		if c.Kind == types.CreateTable {
			if err := m.catalog.Apply(c); err != nil {
				return err
			}
		}
	}
	ops := []delta.Op{}
	for _, c := range cs.Catalog {
		if c.Kind == types.CreateTable && c.Table.Kind == types.Local {
			ops = append(ops, delta.Op{Kind: delta.OpCreateTable, Table: c.Table.ID, TKind: types.Local, Version: c.Table.Version})
		}
	}
	ops = append(ops, changelog.ToOps(cs)...)
	for _, c := range cs.Catalog {
		if c.Kind == types.DropTable {
			ops = append(ops, delta.Op{Kind: delta.OpDropTable, Table: c.ID})
		}
	}
	if err := m.store.Publish(ops, cs.Seq); err != nil {
		return err
	}
	for _, c := range cs.Catalog {
		if c.Kind != types.CreateTable {
			if err := m.catalog.Apply(c); err != nil {
				return err
			}
		}
	}
	for _, o := range cs.Objects {
		m.catalog.RestoreObject(o)
	}
	for _, a := range cs.AutoInc {
		m.autoMu.Lock()
		m.auto[a.Table] = &counter{next: a.Next, seeded: true}
		m.autoMu.Unlock()
	}
	m.Seq.Advance(cs.Seq)
	return nil
}

// AutoIncrement hands out n values for a table (doc 05 §2.3): seeded once
// from max(Source AUTO_INCREMENT, highest Delta value) plus the gap (D-15) for
// mirrored tables, then counted up. Values are never handed out twice, and are
// not returned on rollback, as in InnoDB.
func (m *Manager) AutoIncrement(t types.TableID, n uint64, offset, increment uint64) (first, granted uint64, move types.AutoIncMove, err error) {
	if n == 0 {
		n = 1
	}
	if increment == 0 {
		increment = 1
	}
	m.autoMu.Lock()
	c := m.auto[t]
	if c == nil {
		c = &counter{}
		m.auto[t] = c
	}
	m.autoMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.seeded {
		def, ok := m.catalog.Table(t)
		if !ok {
			return 0, 0, move, types.Errorf(types.ErrInternal, "auto-increment on unknown table")
		}
		var high uint64
		if col := autoColumn(def); col >= 0 {
			if tb := m.store.Snapshot().Table(t); tb != nil {
				for _, r := range tb.Rows() {
					if col < len(r.Row) && !r.Row[col].Null {
						if v, err := strconv.ParseUint(string(r.Row[col].B), 10, 64); err == nil && v > high {
							high = v
						}
					}
				}
			}
		}
		next := high + 1
		if def.Kind == types.Mirrored && def.Source != nil {
			src := def.Source.AutoInc
			if src < 1 {
				src = 1
			}
			if src > next {
				next = src
			}
			next += m.autoGap
		}
		c.next, c.seeded = next, true
	}
	first = c.next
	if offset > 1 || increment > 1 {
		// Align to auto_increment_offset + k * auto_increment_increment.
		if offset == 0 {
			offset = 1
		}
		if first < offset {
			first = offset
		} else if r := (first - offset) % increment; r != 0 {
			first += increment - r
		}
	}
	c.next = first + n*increment
	return first, n, types.AutoIncMove{Table: t, Next: c.next}, nil
}

// SetAutoIncrement moves a table's counter (ALTER TABLE ... AUTO_INCREMENT, restore).
func (m *Manager) SetAutoIncrement(t types.TableID, next uint64) {
	m.autoMu.Lock()
	defer m.autoMu.Unlock()
	m.auto[t] = &counter{next: next, seeded: true}
}

func autoColumn(def types.TableDef) int {
	for i, c := range def.Columns {
		if c.AutoIncrement {
			return i
		}
	}
	return -1
}
