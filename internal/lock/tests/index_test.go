// Package lock_test — tests/index_test.go
//
// The lock table's indexes (issue #16): conflicts found through the point and
// interval indexes match a brute-force walk of every entry; Covered; Release;
// and taking a lock costs the same however many entries a transaction holds.
package lock_test

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/virtual-db/vdb-core/internal/delta"
	. "github.com/virtual-db/vdb-core/internal/lock"
	"github.com/virtual-db/vdb-core/internal/types"
)

// refOverlap is the documented overlap rule, written out without indexes.
func refOverlap(a, b types.Resource) bool {
	if a.Table != b.Table || a.Index != b.Index {
		return false
	}
	pt := func(r types.Resource) bool { return !r.Whole && r.Range == nil }
	switch {
	case a.Whole || b.Whole:
		return true
	case pt(a) && pt(b):
		return string(a.Point) == string(b.Point)
	case pt(a):
		return delta.InRange(a.Point, *b.Range)
	case pt(b):
		return delta.InRange(b.Point, *a.Range)
	}
	x, y := *a.Range, *b.Range
	return (len(x.Low) == 0 || delta.InHigh(x.Low, y)) && (len(y.Low) == 0 || delta.InHigh(y.Low, x))
}

func randImg(r *rand.Rand) types.KeyImage {
	n := r.Intn(3)
	b := make([]byte, n)
	for i := range b {
		b[i] = "ab\xff"[r.Intn(3)]
	}
	return b
}

func randRes(r *rand.Rand) types.Resource {
	res := types.Resource{Table: types.TableID(1 + r.Intn(2)), Index: types.IndexID(r.Intn(2))}
	switch r.Intn(10) {
	case 0:
		res.Whole = true
	case 1, 2, 3, 4:
		res.Point = randImg(r)
		if len(res.Point) == 0 {
			res.Point = types.KeyImage("a")
		}
	default:
		res.Range = &types.ImageRange{Low: randImg(r), High: randImg(r), LowIncl: r.Intn(2) == 0, HighIncl: r.Intn(2) == 0}
	}
	return res
}

func resString(r types.Resource) string {
	switch {
	case r.Whole:
		return fmt.Sprintf("%d/%d whole", r.Table, r.Index)
	case r.Range == nil:
		return fmt.Sprintf("%d/%d pt %q", r.Table, r.Index, r.Point)
	}
	return fmt.Sprintf("%d/%d rg %q %v %q %v", r.Table, r.Index, r.Range.Low, r.Range.LowIncl, r.Range.High, r.Range.HighIncl)
}

// TestIndexesMatchBruteForce: Conflicts is asked about exactly the entries
// of other transactions that overlap the wanted resource.
func TestIndexesMatchBruteForce(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	for round := 0; round < 200; round++ {
		var asked []string
		m := New(nil)
		m.Define(types.LockModes{
			Conflicts: func(_, _ types.LockMode, held, _ types.Resource) bool {
				asked = append(asked, resString(held))
				return false
			},
		})
		type held struct {
			tx  uint64
			res types.Resource
		}
		var hs []held
		for i := 0; i < 60; i++ {
			h := held{uint64(1 + rnd.Intn(3)), randRes(rnd)}
			before := m.Held(h.tx)
			if err := m.Acquire(w(h.tx), h.res, X); err != nil {
				t.Fatal(err)
			}
			if m.Held(h.tx) > before { // not already held
				hs = append(hs, h)
			}
		}
		// Release some, as statements and unlock_row would.
		if round%2 == 0 {
			m.ReleaseAll(3)
			kept := hs[:0]
			for _, h := range hs {
				if h.tx != 3 {
					kept = append(kept, h)
				}
			}
			hs = kept
		}
		for q := 0; q < 40; q++ {
			want := randRes(rnd)
			asked = nil
			if err := m.Acquire(w(4), want, X); err != nil {
				t.Fatal(err)
			}
			m.ReleaseAll(4)
			var exp []string
			for _, h := range hs {
				if refOverlap(h.res, want) {
					exp = append(exp, resString(h.res))
				}
			}
			sort.Strings(exp)
			sort.Strings(asked)
			if fmt.Sprint(exp) != fmt.Sprint(asked) {
				t.Fatalf("round %d, want %s:\n asked %v\n expected %v", round, resString(want), asked, exp)
			}
		}
	}
}

func TestCovered(t *testing.T) {
	m := newM(nil)
	if err := m.Acquire(w(1), rg("b", "d"), S); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		r    types.Resource
		mode types.LockMode
		want bool
	}{
		{pt("c"), S, true},
		{pt("b"), S, true},
		{pt("d"), S, true},
		{pt("e"), S, false},
		{pt("c"), X, false}, // S does not cover X
		{rg("b", "c"), S, true},
		{rg("a", "c"), S, false},
		{types.Resource{Table: 1, Index: 2, Point: types.KeyImage("c")}, S, false},
	} {
		if got := m.Covered(1, c.r, c.mode); got != c.want {
			t.Errorf("Covered(%s, %d) = %v, want %v", resString(c.r), c.mode, got, c.want)
		}
	}
	if m.Covered(2, pt("c"), S) {
		t.Error("another transaction's range covers nothing for tx 2")
	}
	m.Acquire(w(1), pt("x"), X)
	if m.Covered(1, pt("x"), S) {
		t.Error("a point lock is not a covering range")
	}
	m.Acquire(w(1), types.Resource{Table: 1, Whole: true}, X)
	if !m.Covered(1, pt("zz"), X) {
		t.Error("a whole-index X lock covers every point")
	}
}

func TestRelease(t *testing.T) {
	m := newM(nil)
	m.Acquire(w(1), pt("a"), X) // held before the statement
	mark := m.Mark(1)
	m.Acquire(w(1), pt("a"), X) // no new entry: already held
	m.Acquire(w(1), pt("b"), X)
	m.Acquire(w(1), pt("c"), X)
	m.Release(1, pt("a"), mark)
	m.Release(1, pt("b"), mark)
	if m.Held(1) != 2 {
		t.Fatalf("held %d, want 2 (a from before the statement, and c)", m.Held(1))
	}
	if err := m.Acquire(w(2), pt("b"), X); err != nil {
		t.Fatalf("b was released: %v", err)
	}
	if code(m.Acquire(w(2), pt("a"), X)) != types.ErrLockWaitTimeout {
		t.Fatal("a was held before the statement and stays")
	}
	if code(m.Acquire(w(2), pt("c"), X)) != types.ErrLockWaitTimeout {
		t.Fatal("c was not released")
	}
	// Release then ReleaseSince: the statement's other locks still go.
	m.ReleaseSince(1, mark)
	if m.Held(1) != 1 {
		t.Fatalf("held %d, want 1", m.Held(1))
	}
	if st := m.Stats(); st.Entries != 2 { // tx 1's a, tx 2's b
		t.Fatalf("entries %d, want 2", st.Entries)
	}
}

// fill gives tx n point locks and n range locks (next-key style).
func fill(m *Manager, tx uint64, n int) {
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%08d", i)
		if err := m.Acquire(w(tx), pt(k), X); err != nil {
			panic(err)
		}
		if err := m.Acquire(w(tx), rg(k+"a", k+"b"), X); err != nil {
			panic(err)
		}
	}
}

func perLock(m *Manager, tx uint64, from, n int) time.Duration {
	start := time.Now()
	for i := from; i < from+n; i++ {
		k := fmt.Sprintf("%08d", i)
		m.Acquire(w(tx), pt(k), X)
		m.Acquire(w(tx), rg(k+"a", k+"b"), X)
		m.Covered(tx, pt(k+"a5"), X)
	}
	return time.Since(start) / time.Duration(2*n)
}

// TestLockCostFlat: the cost of a lock does not grow with the entries the
// transaction already holds. The old list walk made this quadratic: with
// 200,000 held, each new lock walked them all.
func TestLockCostFlat(t *testing.T) {
	if testing.Short() {
		t.Skip("builds 400,000 lock entries")
	}
	small, big := newM(nil), newM(nil)
	fill(small, 1, 1_000)
	fill(big, 1, 200_000)
	perLock(small, 1, 1_000, 2_000) // warm up
	a := perLock(small, 1, 3_000, 5_000)
	b := perLock(big, 1, 200_000, 5_000)
	t.Logf("per lock: %v with 1k held, %v with 200k held", a, b)
	if b > 20*a+5*time.Microsecond {
		t.Fatalf("lock cost grows with held entries: %v vs %v", b, a)
	}
}

func BenchmarkAcquire(b *testing.B) {
	for _, held := range []int{10, 1_000_000} {
		b.Run(fmt.Sprintf("held=%d", held), func(b *testing.B) {
			m := newM(nil)
			fill(m, 1, held/2)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.Acquire(w(1), pt(fmt.Sprintf("x%09d", i)), X)
			}
		})
	}
}

// TestRangeIndexes: the indexes of a table on which another transaction
// holds a range or whole-index lock (vdb-mysql-engine#6).
func TestRangeIndexes(t *testing.T) {
	m := newM(nil)
	idx := func(r types.Resource, i types.IndexID) types.Resource { r.Index = i; return r }
	if got := m.RangeIndexes(2, 1); len(got) != 0 {
		t.Fatalf("empty table: %v", got)
	}
	m.Acquire(w(1), idx(rg("b", "d"), 3), S)
	m.Acquire(w(1), idx(pt("x"), 4), X) // a point lock is not a range
	m.Acquire(w(2), idx(rg("m", "p"), 1), S)
	m.Acquire(w(1), types.Resource{Table: 2, Index: 5, Whole: true}, X)
	if got := fmt.Sprint(m.RangeIndexes(2, 1)); got != "[3]" {
		t.Fatalf("for tx 2: %s, want [3]", got)
	}
	if got := fmt.Sprint(m.RangeIndexes(1, 1)); got != "[1]" {
		t.Fatalf("for tx 1: %s, want [1] (its own ranges do not count)", got)
	}
	if got := fmt.Sprint(m.RangeIndexes(9, 1)); got != "[1 3]" {
		t.Fatalf("for tx 9: %s, want [1 3]", got)
	}
	if got := fmt.Sprint(m.RangeIndexes(9, 2)); got != "[5]" {
		t.Fatalf("whole-index lock on table 2: %s, want [5]", got)
	}
	m.ReleaseAll(1)
	if got := fmt.Sprint(m.RangeIndexes(2, 1)); got != "[]" {
		t.Fatalf("after release: %s", got)
	}
}
