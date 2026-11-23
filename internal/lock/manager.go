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

	"github.com/tidwall/btree"
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

// txLocks indexes one transaction's entries: by grant order (Mark,
// ReleaseSince), by exact resource (holds, Release) and by interval (Covered).
type txLocks struct {
	bySeq  btree.Map[uint64, *entry]
	byRes  map[slot]map[string][]*entry
	ranges map[slot]*itree
}

// Reserver is the memory guard's interface.
type Reserver interface {
	Reserve(n int, kind string) error
}

// Manager is the environment's lock table. Safe for concurrent use.
type Manager struct {
	mu      sync.Mutex
	modes   types.LockModes
	points  map[slot]*btree.Map[string, []*entry]
	ranges  map[slot]*itree // ranges and whole-index entries
	byTx    map[uint64]*txLocks
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
		points:  map[slot]*btree.Map[string, []*entry]{},
		ranges:  map[slot]*itree{},
		byTx:    map[uint64]*txLocks{},
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

// conflicts returns the entries of other transactions that block r in mode,
// point entries first and each group in grant order.
func (m *Manager) conflicts(tx uint64, r types.Resource, mode types.LockMode) []*entry {
	var out []*entry
	s := slot{r.Table, r.Index}
	check := func(e *entry) {
		if e.tx != tx && overlap(e.res, r) && m.conflict(e.mode, mode, e.res, r) {
			out = append(out, e)
		}
	}
	lo, hi := span(r)
	if pts := m.points[s]; pts != nil {
		if isPoint(r) {
			es, _ := pts.Get(string(r.Point))
			for _, e := range es {
				check(e)
			}
		} else {
			var pe []*entry
			pts.Ascend(lo, func(k string, es []*entry) bool {
				if !hi.inf && k >= hi.b {
					return false
				}
				for _, e := range es {
					if e.tx != tx {
						pe = append(pe, e)
					}
				}
				return true
			})
			sort.Slice(pe, func(i, j int) bool { return pe[i].seq < pe[j].seq })
			for _, e := range pe {
				check(e)
			}
		}
	}
	if rs := m.ranges[s]; rs != nil {
		var re []*entry
		rs.overlapping(lo, hi, func(e *entry) bool {
			if e.tx != tx {
				re = append(re, e)
			}
			return true
		})
		sort.Slice(re, func(i, j int) bool { return re[i].seq < re[j].seq })
		for _, e := range re {
			check(e)
		}
	}
	return out
}

func (m *Manager) holds(tx uint64, r types.Resource, mode types.LockMode) bool {
	t := m.byTx[tx]
	if t == nil {
		return false
	}
	for _, e := range t.byRes[slot{r.Table, r.Index}][bytesKey(r)] {
		if m.covers(e.mode, mode) {
			return true
		}
	}
	return false
}

// Covered reports whether tx holds a range or whole-index lock that contains
// the point resource r in a mode that covers mode (Covers), so that a lock on
// r itself would add nothing. It does not look at point locks.
func (m *Manager) Covered(tx uint64, r types.Resource, mode types.LockMode) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.byTx[tx]
	if t == nil {
		return false
	}
	rs := t.ranges[slot{r.Table, r.Index}]
	if rs == nil {
		return false
	}
	found := false
	lo, hi := span(r)
	rs.overlapping(lo, hi, func(e *entry) bool {
		if contains(e.res, r) && m.covers(e.mode, mode) {
			found = true
			return false
		}
		return true
	})
	return found
}

// contains reports whether held, a range or whole index, contains all of r.
func contains(held, r types.Resource) bool {
	switch {
	case held.Whole:
		return true
	case isPoint(held) || r.Whole:
		return false
	case isPoint(r):
		return delta.InRange(r.Point, *held.Range)
	}
	h, x := *held.Range, *r.Range
	lowOK := len(h.Low) == 0 || (len(x.Low) != 0 && delta.InLow(x.Low, h) && (h.LowIncl || !x.LowIncl || !bytes.Equal(x.Low, h.Low)))
	highOK := len(h.High) == 0 || (len(x.High) != 0 && delta.InHigh(x.High, h) && (h.HighIncl || !x.HighIncl || !bytes.Equal(x.High, h.High)))
	return lowOK && highOK
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
	t := m.byTx[w.Tx]
	if t == nil {
		t = &txLocks{byRes: map[slot]map[string][]*entry{}, ranges: map[slot]*itree{}}
		m.byTx[w.Tx] = t
	}
	if isPoint(r) {
		pts := m.points[s]
		if pts == nil {
			pts = &btree.Map[string, []*entry]{}
			m.points[s] = pts
		}
		k := string(r.Point)
		es, _ := pts.Get(k)
		pts.Set(k, append(es, e))
	} else {
		rs := m.ranges[s]
		if rs == nil {
			rs = &itree{}
			m.ranges[s] = rs
		}
		rs.insert(e)
		trs := t.ranges[s]
		if trs == nil {
			trs = &itree{}
			t.ranges[s] = trs
		}
		trs.insert(e)
	}
	t.bySeq.Set(e.seq, e)
	br := t.byRes[s]
	if br == nil {
		br = map[string][]*entry{}
		t.byRes[s] = br
	}
	k := bytesKey(r)
	br[k] = append(br[k], e)
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
	if t := m.byTx[tx]; t != nil {
		if seq, _, ok := t.bySeq.Max(); ok {
			return types.LockMark(seq)
		}
	}
	return types.LockMark(m.seq)
}

// ReleaseAll drops every lock tx holds and wakes waiters.
func (m *Manager) ReleaseAll(tx uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.byTx[tx]
	if t == nil {
		m.forget(tx)
		return
	}
	var drop []*entry
	t.bySeq.Scan(func(_ uint64, e *entry) bool { drop = append(drop, e); return true })
	m.releaseLocked(tx, drop)
}

// ReleaseSince drops the locks tx took after mark.
func (m *Manager) ReleaseSince(tx uint64, mark types.LockMark) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.byTx[tx]
	if t == nil {
		m.forget(tx) // as before: a transaction holding nothing has no edges
		return
	}
	var drop []*entry
	t.bySeq.Ascend(uint64(mark)+1, func(_ uint64, e *entry) bool { drop = append(drop, e); return true })
	m.releaseLocked(tx, drop)
}

// Release drops the locks tx took on exactly resource r after mark. A lock on
// r that tx held at or before mark stays. The engine uses it at READ
// COMMITTED to free the lock of a row the statement read and did not keep.
func (m *Manager) Release(tx uint64, r types.Resource, mark types.LockMark) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.byTx[tx]
	if t == nil {
		m.forget(tx) // as before: a transaction holding nothing has no edges
		return
	}
	var drop []*entry
	for _, e := range t.byRes[slot{r.Table, r.Index}][bytesKey(r)] {
		if e.seq > uint64(mark) {
			drop = append(drop, e)
		}
	}
	m.releaseLocked(tx, drop)
}

// ReleaseIf drops the locks of tx for which drop returns true.
func (m *Manager) ReleaseIf(tx uint64, drop func(r types.Resource, mode types.LockMode) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.byTx[tx]
	if t == nil {
		m.forget(tx) // as before: a transaction holding nothing has no edges
		return
	}
	var out []*entry
	t.bySeq.Scan(func(_ uint64, e *entry) bool {
		if drop(e.res, e.mode) {
			out = append(out, e)
		}
		return true
	})
	m.releaseLocked(tx, out)
}

func removeEntry(es []*entry, e *entry) []*entry {
	for i, x := range es {
		if x == e {
			return append(es[:i], es[i+1:]...)
		}
	}
	return es
}

// releaseLocked removes the given entries of tx from every index.
func (m *Manager) releaseLocked(tx uint64, drop []*entry) {
	t := m.byTx[tx]
	for _, e := range drop {
		s := slot{e.res.Table, e.res.Index}
		if isPoint(e.res) {
			pts := m.points[s]
			k := string(e.res.Point)
			es, _ := pts.Get(k)
			if es = removeEntry(es, e); len(es) == 0 {
				pts.Delete(k)
				if pts.Len() == 0 {
					delete(m.points, s)
				}
			} else {
				pts.Set(k, es)
			}
		} else {
			m.ranges[s].remove(e)
			if m.ranges[s].n == 0 {
				delete(m.ranges, s)
			}
			t.ranges[s].remove(e)
			if t.ranges[s].n == 0 {
				delete(t.ranges, s)
			}
		}
		t.bySeq.Delete(e.seq)
		k := bytesKey(e.res)
		if es := removeEntry(t.byRes[s][k], e); len(es) == 0 {
			delete(t.byRes[s], k)
			if len(t.byRes[s]) == 0 {
				delete(t.byRes, s)
			}
		} else {
			t.byRes[s][k] = es
		}
	}
	m.entries -= uint64(len(drop))
	if t.bySeq.Len() == 0 {
		delete(m.byTx, tx)
		m.forget(tx)
	}
	if len(drop) > 0 {
		close(m.changed)
		m.changed = make(chan struct{})
	}
}

// forget drops tx's wait-for edges and victim channel once it holds nothing.
func (m *Manager) forget(tx uint64) {
	delete(m.waitFor, tx)
	delete(m.victims, tx)
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
	if t := m.byTx[tx]; t != nil {
		return t.bySeq.Len()
	}
	return 0
}
