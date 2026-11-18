// Package lock_test — tests/lock_test.go
//
// InnoDB's compatibility matrix as a table test, interval locks against
// inserts, deadlock detection with three sessions, timeout, kill, and the
// lock memory limit (doc 04 §5).
package lock_test

import (
	"errors"
	"testing"
	"time"

	. "github.com/virtual-db/vdb-core/internal/lock"
	"github.com/virtual-db/vdb-core/internal/types"
)

func w(tx uint64) Waiter { return Waiter{Tx: tx, Conn: tx, Timeout: 100 * time.Millisecond} }

func code(err error) types.ErrorCode {
	var e *types.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return types.ErrNone
}

func rec(key string, m types.LockMode) Request {
	return Request{Table: 1, Kind: Record, Mode: m, Key: types.KeyImage(key)}
}

func TestMatrix(t *testing.T) {
	cases := []struct {
		held, want Request
		conflict   bool
	}{
		{rec("a", types.LockShared), rec("a", types.LockShared), false},
		{rec("a", types.LockShared), rec("a", types.LockExclusive), true},
		{rec("a", types.LockExclusive), rec("a", types.LockShared), true},
		{rec("a", types.LockExclusive), rec("b", types.LockExclusive), false},
		{Request{Table: 1, Kind: Gap, Range: types.ImageRange{Low: types.KeyImage("a"), High: types.KeyImage("c")}},
			Request{Table: 1, Kind: Gap, Range: types.ImageRange{Low: types.KeyImage("a"), High: types.KeyImage("c")}}, false},
		{Request{Table: 1, Kind: Gap, Range: types.ImageRange{Low: types.KeyImage("a"), High: types.KeyImage("c"), LowIncl: true, HighIncl: true}},
			Request{Table: 1, Kind: InsertIntention, Key: types.KeyImage("b")}, true},
		{Request{Table: 1, Kind: Interval, Mode: types.LockExclusive, Range: types.ImageRange{Low: types.KeyImage("a"), High: types.KeyImage("c"), LowIncl: true, HighIncl: true}},
			rec("b", types.LockShared), true},
		{Request{Table: 1, Kind: Interval, Mode: types.LockShared, Range: types.ImageRange{Low: types.KeyImage("a"), High: types.KeyImage("c"), LowIncl: true, HighIncl: true}},
			Request{Table: 1, Kind: InsertIntention, Key: types.KeyImage("d")}, false},
	}
	for i, c := range cases {
		m := New(nil)
		if err := m.Acquire(w(1), c.held); err != nil {
			t.Fatal(err)
		}
		err := m.Acquire(w(2), c.want)
		if got := code(err) == types.ErrLockWaitTimeout; got != c.conflict {
			t.Errorf("case %d: conflict = %v, want %v (%v)", i, got, c.conflict, err)
		}
	}
}

func TestDeadlockThreeSessions(t *testing.T) {
	m := New(nil)
	for i, k := range []string{"a", "b", "c"} {
		if err := m.Acquire(Waiter{Tx: uint64(i + 1), Timeout: 5 * time.Second}, rec(k, types.LockExclusive)); err != nil {
			t.Fatal(err)
		}
	}
	errs := make(chan error, 2)
	go func() { errs <- m.Acquire(Waiter{Tx: 1, Timeout: 5 * time.Second}, rec("b", types.LockExclusive)) }()
	time.Sleep(30 * time.Millisecond)
	go func() { errs <- m.Acquire(Waiter{Tx: 2, Timeout: 5 * time.Second}, rec("c", types.LockExclusive)) }()
	time.Sleep(30 * time.Millisecond)
	err := m.Acquire(Waiter{Tx: 3, Timeout: 5 * time.Second}, rec("a", types.LockExclusive))
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
	m := New(nil)
	m.Acquire(w(1), rec("a", types.LockExclusive))
	kill := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- m.Acquire(Waiter{Tx: 2, Timeout: 5 * time.Second, Killed: kill}, rec("a", types.LockExclusive))
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
	m := New(full{})
	if code(m.Acquire(w(1), rec("a", types.LockShared))) != types.ErrLockMemory {
		t.Fatal("lock memory limit refuses new entries")
	}
}
