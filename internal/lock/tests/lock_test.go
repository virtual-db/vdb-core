// Package lock_test — tests/lock_test.go
//
// The lock table against a test definition of modes (doc 04 §5): overlap of
// points, ranges and whole indexes; a conflict function that sees only
// overlapping resources; wait-only modes; first-come ordering; ReleaseSince;
// deadlock detection with three sessions; timeout; kill; lock memory.
package lock_test

import (
	"errors"
	"testing"
	"time"

	. "github.com/virtual-db/vdb-core/internal/lock"
	"github.com/virtual-db/vdb-core/internal/types"
)

// Test modes: shared and exclusive, conflicting unless both shared; a
// wait-only "insert" mode that waits for any exclusive lock.
const (
	S types.LockMode = iota
	X
	Ins
)

var modes = types.LockModes{
	Names: []string{"S", "X", "INS"},
	Conflicts: func(held, wanted types.LockMode, _, _ types.Resource) bool {
		if wanted == Ins {
			return held == X
		}
		if held == Ins {
			return false
		}
		return held == X || wanted == X
	},
	Covers:   func(held, wanted types.LockMode) bool { return held == X || held == wanted },
	WaitOnly: func(m types.LockMode) bool { return m == Ins },
}

func newM(mem Reserver) *Manager {
	m := New(mem)
	m.Define(modes)
	return m
}

func w(tx uint64) Waiter { return Waiter{Tx: tx, Conn: tx, Timeout: 100 * time.Millisecond} }

func code(err error) types.ErrorCode {
	var e *types.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return types.ErrNone
}

func pt(k string) types.Resource { return types.Resource{Table: 1, Point: types.KeyImage(k)} }

func rg(lo, hi string) types.Resource {
	return types.Resource{Table: 1, Range: &types.ImageRange{Low: types.KeyImage(lo), High: types.KeyImage(hi), LowIncl: true, HighIncl: true}}
}

func TestOverlapAndConflict(t *testing.T) {
	cases := []struct {
		held     types.Resource
		hm       types.LockMode
		want     types.Resource
		wm       types.LockMode
		conflict bool
	}{
		{pt("a"), S, pt("a"), S, false},
		{pt("a"), S, pt("a"), X, true},
		{pt("a"), X, pt("b"), X, false},
		{rg("a", "c"), X, pt("b"), S, true},
		{rg("a", "c"), X, pt("d"), S, false},
		{rg("a", "c"), S, rg("b", "d"), X, true},
		{types.Resource{Table: 1, Whole: true}, X, pt("z"), S, true},
		{types.Resource{Table: 1, Index: 2, Point: types.KeyImage("a")}, X, pt("a"), X, false},
		{rg("a", "c"), X, pt("b"), Ins, true},
		{rg("a", "c"), S, pt("b"), Ins, false},
	}
	for i, c := range cases {
		m := newM(nil)
		if err := m.Acquire(w(1), c.held, c.hm); err != nil {
			t.Fatal(err)
		}
		err := m.Acquire(w(2), c.want, c.wm)
		if got := code(err) == types.ErrLockWaitTimeout; got != c.conflict {
			t.Errorf("case %d: conflict = %v, want %v (%v)", i, got, c.conflict, err)
		}
	}
}

func TestWaitOnlyIsNotKept(t *testing.T) {
	m := newM(nil)
	if err := m.Acquire(w(1), pt("a"), Ins); err != nil {
		t.Fatal(err)
	}
	if m.Held(1) != 0 {
		t.Fatal("a wait-only lock is not kept")
	}
}

func TestReleaseSince(t *testing.T) {
	m := newM(nil)
	m.Acquire(w(1), pt("a"), X)
	mark := m.Mark(1)
	m.Acquire(w(1), pt("b"), X)
	m.ReleaseSince(1, mark)
	if m.Held(1) != 1 {
		t.Fatalf("held %d, want 1", m.Held(1))
	}
	if err := m.Acquire(w(2), pt("b"), X); err != nil {
		t.Fatalf("b was released: %v", err)
	}
}

func TestDeadlockThreeSessions(t *testing.T) {
	m := newM(nil)
	for i, k := range []string{"a", "b", "c"} {
		if err := m.Acquire(Waiter{Tx: uint64(i + 1), Timeout: 5 * time.Second}, pt(k), X); err != nil {
			t.Fatal(err)
		}
	}
	errs := make(chan error, 2)
	go func() { errs <- m.Acquire(Waiter{Tx: 1, Timeout: 5 * time.Second}, pt("b"), X) }()
	time.Sleep(30 * time.Millisecond)
	go func() { errs <- m.Acquire(Waiter{Tx: 2, Timeout: 5 * time.Second}, pt("c"), X) }()
	time.Sleep(30 * time.Millisecond)
	err := m.Acquire(Waiter{Tx: 3, Timeout: 5 * time.Second}, pt("a"), X)
	if code(err) != types.ErrDeadlock {
		t.Fatalf("the youngest transaction is the victim, got %v", err)
	}
	m.ReleaseAll(3)
	if err := <-errs; err != nil {
		t.Fatalf("tx 2 gets c once tx 3 rolls back: %v", err)
	}
	m.ReleaseAll(2)
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}

func TestKill(t *testing.T) {
	m := newM(nil)
	m.Acquire(w(1), pt("a"), X)
	kill := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- m.Acquire(Waiter{Tx: 2, Timeout: 5 * time.Second, Killed: kill}, pt("a"), X)
	}()
	time.Sleep(20 * time.Millisecond)
	close(kill)
	if code(<-done) != types.ErrKilled {
		t.Fatal("a kill ends the wait")
	}
}

type full struct{}

func (full) Reserve(int, string) error { return &types.Error{Code: types.ErrLockMemory} }

func TestLockMemory(t *testing.T) {
	m := newM(full{})
	if code(m.Acquire(w(1), pt("a"), S)) != types.ErrLockMemory {
		t.Fatal("lock memory limit refuses new entries")
	}
}
