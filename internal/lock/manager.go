// Package lock is the lock manager (implementation plan doc 04 §3, product
// doc 07 §5): record locks, gap and interval locks, insert intention, waits,
// the wait-for graph with deadlock victims, and lock memory.
//
// Record locks are keyed by (table, identity-key image). Gap, interval and
// whole-index insert locks are keyed by (table, index) and an image range.
// Compatibility follows InnoDB's matrix: gap locks never conflict with each
// other; an insert-intention lock waits for any gap or interval lock covering
// its image held by another transaction; record locks follow shared and
// exclusive rules; interval locks behave as the record locks of every row in
// their range plus the gaps between them.
package lock

import (
	"sort"
	"sync"
	"time"

	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/types"
)

// Kind is a lock kind.
type Kind uint8

const (
	Record Kind = iota
	Gap
	Interval
	InsertIntention
)

// EntrySize is the accounted size of one lock entry.
const EntrySize = 160

// Request describes one lock to take.
type Request struct {
	Table types.TableID
	Index types.IndexID
	Kind  Kind
	Mode  types.LockMode // LockShared or LockExclusive
	Key   types.KeyImage // Record, InsertIntention
	Range types.ImageRange
}

// Waiter describes the transaction asking for a lock.
type Waiter struct {
	Tx      uint64 // transaction id; a larger id is a younger transaction
	Conn    uint64 // the mysqld connection id, reported to lock waiters
	Timeout time.Duration
	Killed  <-chan struct{}
	OnWait  types.WaitFunc
}

type entry struct {
	req  Request
	tx   uint64
	conn uint64
}

type slot struct {
	table types.TableID
	index types.IndexID
}

// Reserver is the memory guard's interface.
type Reserver interface {
	Reserve(n int, kind string) error
}

// Manager is the environment's lock manager. Safe for concurrent use.
type Manager struct {
	mu      sync.Mutex
	records map[slot]map[string][]*entry
	ranges  map[slot][]*entry
	byTx    map[uint64][]*entry
	waitFor map[uint64]map[uint64]bool
	victims map[uint64]chan struct{}
	changed chan struct{}
	mem     Reserver

	entries, waits, deadlocks, timeouts uint64
}

// New returns an empty manager.
func New(mem Reserver) *Manager {
	return &Manager{
		records: map[slot]map[string][]*entry{},
		ranges:  map[slot][]*entry{},
		byTx:    map[uint64][]*entry{},
		waitFor: map[uint64]map[uint64]bool{},
		victims: map[uint64]chan struct{}{},
		changed: make(chan struct{}),
		mem:     mem,
	}
}

func modesConflict(a, b types.LockMode) bool {
	return a == types.LockExclusive || b == types.LockExclusive
}

func rangesOverlap(a, b types.ImageRange) bool {
	return (len(a.Low) == 0 || delta.InHigh(a.Low, b)) && (len(b.Low) == 0 || delta.InHigh(b.Low, a))
}

// conflicts returns the holders of locks incompatible with r, other than tx.
func (m *Manager) conflicts(tx uint64, r Request) []*entry {
	var out []*entry
	s := slot{r.Table, r.Index}
	switch r.Kind {
	case Record:
		for _, e := range m.records[s][string(r.Key)] {
			if e.tx != tx && modesConflict(e.req.Mode, r.Mode) {
				out = append(out, e)
			}
		}
		for _, e := range m.ranges[s] {
			if e.tx != tx && e.req.Kind == Interval && delta.InRange(r.Key, e.req.Range) && modesConflict(e.req.Mode, r.Mode) {
				out = append(out, e)
			}
		}
	case InsertIntention:
		for _, e := range m.ranges[s] {
			if e.tx != tx && (e.req.Kind == Gap || e.req.Kind == Interval) && delta.InRange(r.Key, e.req.Range) {
				out = append(out, e)
			}
		}
	case Interval:
		for _, e := range m.ranges[s] {
			if e.tx != tx && e.req.Kind == Interval && rangesOverlap(e.req.Range, r.Range) && modesConflict(e.req.Mode, r.Mode) {
				out = append(out, e)
			}
		}
		for k, es := range m.records[s] {
			if !delta.InRange(types.KeyImage(k), r.Range) {
				continue
			}
			for _, e := range es {
				if e.tx != tx && modesConflict(e.req.Mode, r.Mode) {
					out = append(out, e)
				}
			}
		}
	case Gap:
		// Gap locks conflict with nothing; only insert intention waits for them.
	}
	return out
}

func (m *Manager) holds(tx uint64, r Request) bool {
	s := slot{r.Table, r.Index}
	switch r.Kind {
	case Record:
		for _, e := range m.records[s][string(r.Key)] {
			if e.tx == tx && (e.req.Mode == types.LockExclusive || r.Mode == types.LockShared) {
				return true
			}
		}
	case Gap, Interval:
		for _, e := range m.byTx[tx] {
			if e.req.Kind == r.Kind && e.req.Table == r.Table && e.req.Index == r.Index &&
				string(e.req.Range.Low) == string(r.Range.Low) && string(e.req.Range.High) == string(r.Range.High) &&
				e.req.Range.LowIncl == r.Range.LowIncl && e.req.Range.HighIncl == r.Range.HighIncl &&
				(e.req.Mode == types.LockExclusive || r.Mode != types.LockExclusive) {
				return true
			}
		}
	}
	return false
}

// Acquire takes the lock for w, waiting while another transaction holds an
// incompatible one. It returns ErrLockWaitTimeout, ErrDeadlock, ErrKilled or
// ErrLockMemory, or nil when granted. Insert-intention locks are not kept
// once granted: they only wait.
func (m *Manager) Acquire(w Waiter, r Request) error {
	if r.Mode == types.LockNone {
		r.Mode = types.LockShared
	}
	var deadline <-chan time.Time
	reported := false
	for {
		m.mu.Lock()
		if r.Kind != InsertIntention && m.holds(w.Tx, r) {
			m.mu.Unlock()
			return nil
		}
		cs := m.conflicts(w.Tx, r)
		if len(cs) == 0 {
			delete(m.waitFor, w.Tx)
			if r.Kind == InsertIntention {
				m.mu.Unlock()
				return nil
			}
			if m.mem != nil {
				if err := m.mem.Reserve(EntrySize, "lock"); err != nil {
					m.mu.Unlock()
					return err
				}
			}
			m.grant(w, r)
			m.mu.Unlock()
			return nil
		}
		// Must wait: record the edges and look for a cycle.
		edges := map[uint64]bool{}
		for _, e := range cs {
			edges[e.tx] = true
		}
		m.waitFor[w.Tx] = edges
		if cycle := m.cycleFrom(w.Tx); cycle != nil {
			victim := cycle[0]
			for _, t := range cycle {
				if t > victim {
					victim = t
				}
			}
			m.deadlocks++
			if victim == w.Tx {
				delete(m.waitFor, w.Tx)
				m.mu.Unlock()
				return &types.Error{Code: types.ErrDeadlock, Table: r.Table, Msg: "Deadlock found when trying to get lock; try restarting transaction"}
			}
			if ch, ok := m.victims[victim]; ok {
				select {
				case <-ch:
				default:
					close(ch)
				}
			}
		}
		vch, ok := m.victims[w.Tx]
		if !ok {
			vch = make(chan struct{})
			m.victims[w.Tx] = vch
		}
		changed := m.changed
		holder := cs[0].conn
		m.waits++
		m.mu.Unlock()

		if !reported && w.OnWait != nil {
			w.OnWait(holder)
			reported = true
		}
		if deadline == nil {
			t := w.Timeout
			if t <= 0 {
				t = 50 * time.Second
			}
			deadline = time.After(t)
		}
		select {
		case <-changed:
		case <-vch:
			m.mu.Lock()
			delete(m.victims, w.Tx)
			delete(m.waitFor, w.Tx)
			m.mu.Unlock()
			return &types.Error{Code: types.ErrDeadlock, Table: r.Table, Msg: "Deadlock found when trying to get lock; try restarting transaction"}
		case <-deadline:
			m.mu.Lock()
			delete(m.waitFor, w.Tx)
			m.timeouts++
			m.mu.Unlock()
			return &types.Error{Code: types.ErrLockWaitTimeout, Table: r.Table, Holder: holder, Msg: "Lock wait timeout exceeded; try restarting transaction"}
		case <-w.Killed:
			m.mu.Lock()
			delete(m.waitFor, w.Tx)
			m.mu.Unlock()
			return &types.Error{Code: types.ErrKilled, Msg: "Query execution was interrupted"}
		}
	}
}

func (m *Manager) grant(w Waiter, r Request) {
	e := &entry{req: r, tx: w.Tx, conn: w.Conn}
	s := slot{r.Table, r.Index}
	if r.Kind == Record {
		mm := m.records[s]
		if mm == nil {
			mm = map[string][]*entry{}
			m.records[s] = mm
		}
		mm[string(r.Key)] = append(mm[string(r.Key)], e)
	} else {
		m.ranges[s] = append(m.ranges[s], e)
	}
	m.byTx[w.Tx] = append(m.byTx[w.Tx], e)
	m.entries++
}

// cycleFrom returns a wait-for cycle through start, or nil.
func (m *Manager) cycleFrom(start uint64) []uint64 {
	var path []uint64
	seen := map[uint64]bool{}
	var dfs func(t uint64) bool
	dfs = func(t uint64) bool {
		path = append(path, t)
		seen[t] = true
		next := make([]uint64, 0, len(m.waitFor[t]))
		for n := range m.waitFor[t] {
			next = append(next, n)
		}
		sort.Slice(next, func(i, j int) bool { return next[i] < next[j] })
		for _, n := range next {
			if n == start {
				return true
			}
			if !seen[n] && dfs(n) {
				return true
			}
		}
		path = path[:len(path)-1]
		return false
	}
	if dfs(start) {
		return path
	}
	return nil
}

// ReleaseAll drops every lock tx holds and wakes waiters.
func (m *Manager) ReleaseAll(tx uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseLocked(tx, func(*entry) bool { return true })
}

// ReleaseIf drops the record locks of tx for which drop returns true
// (statement rollback at READ COMMITTED releases the locks it took).
func (m *Manager) ReleaseIf(tx uint64, drop func(table types.TableID, key types.KeyImage) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseLocked(tx, func(e *entry) bool { return e.req.Kind == Record && drop(e.req.Table, e.req.Key) })
}

// ReleaseGaps drops the gap and interval locks of tx (READ COMMITTED keeps none).
func (m *Manager) ReleaseGaps(tx uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseLocked(tx, func(e *entry) bool { return e.req.Kind != Record })
}

func (m *Manager) releaseLocked(tx uint64, drop func(*entry) bool) {
	var keep []*entry
	n := 0
	for _, e := range m.byTx[tx] {
		if !drop(e) {
			keep = append(keep, e)
			continue
		}
		n++
		s := slot{e.req.Table, e.req.Index}
		if e.req.Kind == Record {
			es := m.records[s][string(e.req.Key)]
			for i, x := range es {
				if x == e {
					es = append(es[:i], es[i+1:]...)
					break
				}
			}
			if len(es) == 0 {
				delete(m.records[s], string(e.req.Key))
			} else {
				m.records[s][string(e.req.Key)] = es
			}
		} else {
			es := m.ranges[s]
			for i, x := range es {
				if x == e {
					es = append(es[:i], es[i+1:]...)
					break
				}
			}
			m.ranges[s] = es
		}
	}
	if len(keep) == 0 {
		delete(m.byTx, tx)
	} else {
		m.byTx[tx] = keep
	}
	m.entries -= uint64(n)
	if len(keep) == 0 {
		delete(m.waitFor, tx)
		delete(m.victims, tx)
	}
	if n > 0 {
		close(m.changed)
		m.changed = make(chan struct{})
	}
}

// Stats reports the lock manager's counters.
func (m *Manager) Stats() types.LockStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return types.LockStats{Entries: m.entries, Bytes: m.entries * EntrySize, Waits: m.waits, Deadlocks: m.deadlocks, Timeouts: m.timeouts}
}

// Held returns how many locks tx holds.
func (m *Manager) Held(tx uint64) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.byTx[tx])
}
