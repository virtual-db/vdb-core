// Package lock is the lock table (implementation plan doc 04 §3, D-89): it
// queues, grants, times out and breaks deadlocks, and accounts lock memory.
// It does not know which locks a statement needs or how modes conflict: the
// engine defines its modes and a conflict function once (Define), and takes
// locks on resources it names. vdb-mysql-engine defines InnoDB's record, gap,
// next-key, interval and insert-intention locks this way.
//
// A resource is one key image, one range of images, or a whole index, within
// (table, index). Two resources overlap when they are in the same (table,
// index) and name the same point, a point inside a range, overlapping ranges,
// or anything and a whole index. The engine's Conflicts is asked only about
// overlapping resources held by other transactions.
package lock

import (
	"bytes"
	"sort"
	"sync"
	"time"

	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/types"
)

// EntrySize is the accounted size of one lock entry.
const EntrySize = 160

// Waiter describes the transaction asking for a lock.
type Waiter struct {
	Tx      uint64 // transaction id; a larger id is a younger transaction
	Conn    uint64 // the engine's connection id, reported to lock waiters
	Timeout time.Duration
	Killed  <-chan struct{}
	OnWait  types.WaitFunc
}

type entry struct {
	res  types.Resource
	mode types.LockMode
	tx   uint64
	conn uint64
	seq  uint64
}

type slot struct {
	table types.TableID
	index types.IndexID
}

// Reserver is the memory guard's interface.
type Reserver interface {
	Reserve(n int, kind string) error
}

// Manager is the environment's lock table. Safe for concurrent use.
type Manager struct {
	mu      sync.Mutex
	modes   types.LockModes
	points  map[slot]map[string][]*entry
	ranges  map[slot][]*entry // ranges and whole-index entries
	byTx    map[uint64][]*entry
	waitFor map[uint64]map[uint64]bool
	victims map[uint64]chan struct{}
	changed chan struct{}
	mem     Reserver
	seq     uint64

	entries, waits, deadlocks, timeouts uint64
}

// New returns an empty lock table. Until Define is called every two locks on
// overlapping resources conflict.
func New(mem Reserver) *Manager {
	return &Manager{
		points:  map[slot]map[string][]*entry{},
		ranges:  map[slot][]*entry{},
		byTx:    map[uint64][]*entry{},
		waitFor: map[uint64]map[uint64]bool{},
		victims: map[uint64]chan struct{}{},
		changed: make(chan struct{}),
		mem:     mem,
	}
}

// Define installs the engine's lock modes. Called once, before the first lock.
func (m *Manager) Define(modes types.LockModes) {
	m.mu.Lock()
	m.modes = modes
	m.mu.Unlock()
}

func (m *Manager) conflict(held, wanted types.LockMode, hr, wr types.Resource) bool {
	if m.modes.Conflicts == nil {
		return true
	}
	return m.modes.Conflicts(held, wanted, hr, wr)
}

func (m *Manager) waitOnly(mode types.LockMode) bool {
	return m.modes.WaitOnly != nil && m.modes.WaitOnly(mode)
}

func (m *Manager) covers(held, wanted types.LockMode) bool {
	if m.modes.Covers == nil {
		return held == wanted
	}
	return m.modes.Covers(held, wanted)
}

func isPoint(r types.Resource) bool { return !r.Whole && r.Range == nil }

func rangesOverlap(a, b types.ImageRange) bool {
	return (len(a.Low) == 0 || delta.InHigh(a.Low, b)) && (len(b.Low) == 0 || delta.InHigh(b.Low, a))
}

// overlap reports whether two resources of the same slot overlap.
func overlap(a, b types.Resource) bool {
	switch {
	case a.Whole || b.Whole:
		return true
	case isPoint(a) && isPoint(b):
		return bytes.Equal(a.Point, b.Point)
	case isPoint(a):
		return delta.InRange(a.Point, *b.Range)
	case isPoint(b):
		return delta.InRange(b.Point, *a.Range)
	default:
		return rangesOverlap(*a.Range, *b.Range)
	}
}

func sameResource(a, b types.Resource) bool {
	if a.Table != b.Table || a.Index != b.Index || a.Whole != b.Whole || isPoint(a) != isPoint(b) {
		return false
	}
	if a.Whole {
		return true
	}
	if isPoint(a) {
		return bytes.Equal(a.Point, b.Point)
	}
	x, y := *a.Range, *b.Range
	return bytes.Equal(x.Low, y.Low) && bytes.Equal(x.High, y.High) && x.LowIncl == y.LowIncl && x.HighIncl == y.HighIncl
}

// conflicts returns the entries of other transactions that block r in mode.
func (m *Manager) conflicts(tx uint64, r types.Resource, mode types.LockMode) []*entry {
	var out []*entry
	s := slot{r.Table, r.Index}
	check := func(e *entry) {
		if e.tx != tx && overlap(e.res, r) && m.conflict(e.mode, mode, e.res, r) {
			out = append(out, e)
		}
	}
	if isPoint(r) {
		for _, e := range m.points[s][string(r.Point)] {
			check(e)
		}
	} else {
		for _, es := range m.points[s] {
			for _, e := range es {
				check(e)
			}
		}
	}
	for _, e := range m.ranges[s] {
		check(e)
	}
	return out
}

func (m *Manager) holds(tx uint64, r types.Resource, mode types.LockMode) bool {
	for _, e := range m.byTx[tx] {
		if sameResource(e.res, r) && m.covers(e.mode, mode) {
			return true
		}
	}
	return false
}

// Acquire takes a lock for w, waiting while another transaction holds a
// conflicting one. It returns ErrLockWaitTimeout, ErrDeadlock, ErrKilled or
// ErrLockMemory, or nil when granted. A lock of a wait-only mode is not kept
// once granted: it only waits.
func (m *Manager) Acquire(w Waiter, r types.Resource, mode types.LockMode) error {
	var deadline <-chan time.Time
	reported := false
	for {
		m.mu.Lock()
		wo := m.waitOnly(mode)
		if !wo && m.holds(w.Tx, r, mode) {
			m.mu.Unlock()
			return nil
		}
		cs := m.conflicts(w.Tx, r, mode)
		if len(cs) == 0 {
			delete(m.waitFor, w.Tx)
			if wo {
				m.mu.Unlock()
				return nil
			}
			if m.mem != nil {
				if err := m.mem.Reserve(EntrySize, "lock"); err != nil {
					m.mu.Unlock()
					return err
				}
			}
			m.grant(w, r, mode)
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

func (m *Manager) grant(w Waiter, r types.Resource, mode types.LockMode) {
	m.seq++
	e := &entry{res: r, mode: mode, tx: w.Tx, conn: w.Conn, seq: m.seq}
	s := slot{r.Table, r.Index}
	if isPoint(r) {
		mm := m.points[s]
		if mm == nil {
			mm = map[string][]*entry{}
			m.points[s] = mm
		}
		mm[string(r.Point)] = append(mm[string(r.Point)], e)
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

// Mark returns the position of tx's newest lock, for ReleaseSince.
func (m *Manager) Mark(tx uint64) types.LockMark {
	m.mu.Lock()
	defer m.mu.Unlock()
	es := m.byTx[tx]
	if len(es) == 0 {
		return types.LockMark(m.seq)
	}
	return types.LockMark(es[len(es)-1].seq)
}

// ReleaseAll drops every lock tx holds and wakes waiters.
func (m *Manager) ReleaseAll(tx uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseLocked(tx, func(*entry) bool { return true })
}

// ReleaseSince drops the locks tx took after mark.
func (m *Manager) ReleaseSince(tx uint64, mark types.LockMark) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseLocked(tx, func(e *entry) bool { return e.seq > uint64(mark) })
}

// ReleaseIf drops the locks of tx for which drop returns true.
func (m *Manager) ReleaseIf(tx uint64, drop func(r types.Resource, mode types.LockMode) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseLocked(tx, func(e *entry) bool { return drop(e.res, e.mode) })
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
		s := slot{e.res.Table, e.res.Index}
		if isPoint(e.res) {
			k := string(e.res.Point)
			es := m.points[s][k]
			for i, x := range es {
				if x == e {
					es = append(es[:i], es[i+1:]...)
					break
				}
			}
			if len(es) == 0 {
				delete(m.points[s], k)
			} else {
				m.points[s][k] = es
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

// Stats reports the lock table's counters.
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
