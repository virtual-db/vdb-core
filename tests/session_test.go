// Package core_test — tests/session_test.go
//
// Black-box tests of sessions over a fake Source: the merge rule for scans,
// lookups and the exact count; inserts with duplicates in the Delta and on the
// Source; updates and deletes of Source and Delta rows; savepoints and
// statement rollback; snapshot isolation between sessions; row locks with
// timeout and deadlock; truncate; foreign keys with the image callback.
package core_test

import (
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	core "github.com/virtual-db/vdb-core"
)

// ── fake Source: tables of integer keys ──

type fakeTable struct {
	rows map[int]core.Row // key -> full row; column 0 is the key
}

type fakeSource struct {
	mu        sync.Mutex
	tables    map[core.TableID]*fakeTable
	snapshots int
}

func (f *fakeSource) Snapshot(core.SessionInfo, core.StmtContext) (core.SourceSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshots++
	// Copy the tables: the snapshot sees the Source as it is now.
	snap := &fakeSnap{tables: map[core.TableID]map[int]core.Row{}}
	for id, t := range f.tables {
		m := map[int]core.Row{}
		for k, r := range t.rows {
			m[k] = r
		}
		snap.tables[id] = m
	}
	return snap, nil
}

type fakeSnap struct {
	tables map[core.TableID]map[int]core.Row
}

func key(i int) core.SourceKey {
	return core.EncodeSourceKey([]core.Value{{B: []byte(strconv.Itoa(i))}})
}

func img(i int) core.KeyImage {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

func (s *fakeSnap) sorted(t core.TableID) []int {
	var ks []int
	for k := range s.tables[t] {
		ks = append(ks, k)
	}
	sort.Ints(ks)
	return ks
}

func (s *fakeSnap) Scan(req core.SourceScan) (core.SourceCursor, error) {
	var sr core.SourceRows
	ks := s.sorted(req.Table)
	if req.Dir == core.Backward {
		sort.Sort(sort.Reverse(sort.IntSlice(ks)))
	}
	for _, k := range ks {
		sr.Rows = append(sr.Rows, core.Project(s.tables[req.Table][k], req.Columns))
		sr.Keys = append(sr.Keys, key(k))
	}
	return &fakeCursor{sr: sr}, nil
}

func (s *fakeSnap) Lookup(t core.TableID, _ core.IndexID, keys []core.Key, cols core.ColumnSet) (core.SourceRows, error) {
	var sr core.SourceRows
	for i, k := range keys {
		n := int(binary.BigEndian.Uint64(k.Image))
		if r, ok := s.tables[t][n]; ok {
			sr.Rows = append(sr.Rows, core.Project(r, cols))
			sr.Keys = append(sr.Keys, key(n))
			sr.KeyIdx = append(sr.KeyIdx, uint32(i))
		}
	}
	return sr, nil
}

func (s *fakeSnap) Exists(t core.TableID, keys []core.SourceKey) ([]bool, error) {
	out := make([]bool, len(keys))
	for i, k := range keys {
		v := core.DecodeSourceKey(k)
		n, _ := strconv.Atoi(string(v[0].B))
		_, out[i] = s.tables[t][n]
	}
	return out, nil
}

func (s *fakeSnap) Count(t core.TableID) (uint64, error) { return uint64(len(s.tables[t])), nil }

func (s *fakeSnap) ByColumns(t core.TableID, cols core.ColumnSet, values []core.Row, want core.ColumnSet) (core.SourceCursor, error) {
	var sr core.SourceRows
	for _, k := range s.sorted(t) {
		r := s.tables[t][k]
		for _, v := range values {
			match := true
			for i, c := range cols {
				if string(r[c].B) != string(v[i].B) || r[c].Null || v[i].Null {
					match = false
				}
			}
			if match {
				sr.Rows = append(sr.Rows, core.Project(r, want))
				sr.Keys = append(sr.Keys, key(k))
			}
		}
	}
	return &fakeCursor{sr: sr}, nil
}

func (s *fakeSnap) Close() error { return nil }

type fakeCursor struct {
	sr   core.SourceRows
	done bool
}

func (c *fakeCursor) Next(int) (core.SourceRows, error) {
	if c.done {
		return core.SourceRows{}, io.EOF
	}
	c.done = true
	return c.sr, nil
}
func (c *fakeCursor) Close() error { return nil }

// ── environment ──

type env struct {
	t      *testing.T
	app    *core.App
	api    core.DriverAPI
	src    *fakeSource
	users  core.TableID
	orders core.TableID
}

func row(id int, name string) core.Row {
	return core.Row{{B: []byte(strconv.Itoa(id))}, {B: []byte(name)}}
}

func images(id int) []core.KeyImage { return []core.KeyImage{img(id)} }

func newEnv(t *testing.T) *env {
	src := &fakeSource{tables: map[core.TableID]*fakeTable{}}
	app := core.New(core.Config{DefaultLockWait: 300 * time.Millisecond})
	app.UseSource(src)
	api := app.DriverAPI()
	pk := core.IndexDef{ID: 0, Name: "PRIMARY", Primary: true, Unique: true, Columns: []uint16{0}, PartLens: []uint32{8}, OrderProven: true, EqualityProven: true}
	users := core.TableDef{Database: "shop", Name: "users", Kind: core.Mirrored,
		Columns: []core.ColumnDef{{Name: "id"}, {Name: "name"}}, Indexes: []core.IndexDef{pk}, Identity: 0,
		Source: &core.SourceMapping{Database: "shop", Table: "users"}}
	orders := core.TableDef{Database: "shop", Name: "orders", Kind: core.Mirrored,
		Columns: []core.ColumnDef{{Name: "id"}, {Name: "user_id", Nullable: true}},
		Indexes: []core.IndexDef{pk, {ID: 1, Name: "user_id", Columns: []uint16{1}, PartLens: []uint32{8}, OrderProven: true, EqualityProven: true}},
		Identity: 0, Source: &core.SourceMapping{Database: "shop", Table: "orders"},
		ForeignKeys: []core.ForeignKeyDef{{Name: "fk_user", Columns: []uint16{1}, ChildIndex: 1, ParentDB: "shop", ParentTable: "users",
			ParentColumns: []string{"id"}, OnDelete: core.FKCascade, OnUpdate: core.FKRestrict}}}
	if err := api.Catalog().Load([]core.TableDef{users, orders}, nil); err != nil {
		t.Fatal(err)
	}
	u, _ := api.Catalog().TableByName("shop", "users")
	o, _ := api.Catalog().TableByName("shop", "orders")
	src.tables[u.ID] = &fakeTable{rows: map[int]core.Row{1: row(1, "ann"), 2: row(2, "bob"), 3: row(3, "cid")}}
	src.tables[o.ID] = &fakeTable{rows: map[int]core.Row{10: row(10, "1"), 11: row(11, "1"), 12: row(12, "2")}}
	return &env{t: t, app: app, api: api, src: src, users: u.ID, orders: o.ID}
}

func (e *env) session(conn uint64) core.Session {
	s, err := e.api.OpenSession(core.SessionInfo{ConnID: conn, Account: "app@%"})
	if err != nil {
		e.t.Fatal(err)
	}
	s.SetCallbacks(nil, func(t core.TableID, rows []core.Row) ([][]core.KeyImage, error) {
		out := make([][]core.KeyImage, len(rows))
		for i, r := range rows {
			id, _ := strconv.Atoi(string(r[0].B))
			ims := []core.KeyImage{img(id)}
			if t == e.orders && !r[1].Null {
				u, _ := strconv.Atoi(string(r[1].B))
				ims = append(ims, img(u))
			} else if t == e.orders {
				ims = append(ims, []byte{0})
			}
			out[i] = ims
		}
		return out, nil
	})
	return s
}

func stmt(t *testing.T, s core.Session, iso core.Isolation) {
	if err := s.StmtBegin(core.StmtContext{Isolation: iso, ForeignKeyChecks: true}); err != nil {
		t.Fatal(err)
	}
}

func scanAll(t *testing.T, s core.Session, tab core.TableID) []string {
	h, err := s.Scan(core.ScanRequest{Table: tab, Columns: core.ColumnSet{0, 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	var out []string
	for {
		b, err := h.Merged().Next(100, 0)
		for _, r := range b.Rows {
			out = append(out, string(r[0].B)+":"+string(r[1].B))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(out)
	return out
}

func equal(t *testing.T, got []string, want ...string) {
	t.Helper()
	sort.Strings(want)
	sort.Strings(got)
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func srcRef(id int) core.RowRef {
	return core.RowRef{Origin: core.OriginSource, PK: core.Key{Image: img(id)}, SourceKey: key(id)}
}

// ── tests ──

func TestMergeRuleAndCount(t *testing.T) {
	e := newEnv(t)
	s := e.session(1)
	stmt(t, s, core.RepeatableRead)
	if _, err := s.Insert(e.users, []core.Row{row(4, "dan")}, [][]core.KeyImage{images(4)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(e.users, srcRef(2), row(2, "bobby"), images(2)); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(e.users, srcRef(3)); err != nil {
		t.Fatal(err)
	}
	equal(t, scanAll(t, s, e.users), "1:ann", "2:bobby", "4:dan")
	n, declined, err := s.Count(e.users)
	if err != nil || declined || n != 3 {
		t.Fatalf("count = %d %v %v", n, declined, err)
	}
	b, _, err := s.Lookup(e.users, 0, []core.Key{{Image: img(1)}, {Image: img(2)}, {Image: img(3)}, {Image: img(4)}}, core.ColumnSet{1}, core.LockNone)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for i, r := range b.Rows {
		got = append(got, strconv.Itoa(int(b.KeyIdx[i]))+"="+string(r[0].B))
	}
	equal(t, got, "0=ann", "1=bobby", "3=dan")
	if err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	s2 := e.session(2)
	stmt(t, s2, core.RepeatableRead)
	equal(t, scanAll(t, s2, e.users), "1:ann", "2:bobby", "4:dan")
}

func TestDuplicates(t *testing.T) {
	e := newEnv(t)
	s := e.session(1)
	stmt(t, s, core.RepeatableRead)
	res, err := s.Insert(e.users, []core.Row{row(5, "x"), row(1, "dup-source"), row(5, "dup-delta")},
		[][]core.KeyImage{images(5), images(1), images(5)})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Err != nil || res[1].Err == nil || res[1].Err.Code != core.ErrDuplicateKey || res[2].Err == nil {
		t.Fatalf("results %+v", res)
	}
	// After deleting Source row 1, inserting key 1 is allowed.
	if err := s.Delete(e.users, srcRef(1)); err != nil {
		t.Fatal(err)
	}
	res, _ = s.Insert(e.users, []core.Row{row(1, "new")}, [][]core.KeyImage{images(1)})
	if res[0].Err != nil {
		t.Fatalf("reinsert: %v", res[0].Err)
	}
}

func TestSavepointsAndStatementRollback(t *testing.T) {
	e := newEnv(t)
	s := e.session(1)
	stmt(t, s, core.RepeatableRead)
	s.Insert(e.users, []core.Row{row(6, "a")}, [][]core.KeyImage{images(6)})
	s.StmtEnd()
	s.Savepoint("sp1")
	stmt(t, s, core.RepeatableRead)
	s.Insert(e.users, []core.Row{row(7, "b")}, [][]core.KeyImage{images(7)})
	s.StmtEnd()
	stmt(t, s, core.RepeatableRead)
	s.Insert(e.users, []core.Row{row(8, "c")}, [][]core.KeyImage{images(8)})
	if err := s.StmtRollback(); err != nil {
		t.Fatal(err)
	}
	stmt(t, s, core.RepeatableRead)
	equal(t, scanAll(t, s, e.users), "1:ann", "2:bob", "3:cid", "6:a", "7:b")
	if err := s.RollbackTo("sp1"); err != nil {
		t.Fatal(err)
	}
	equal(t, scanAll(t, s, e.users), "1:ann", "2:bob", "3:cid", "6:a")
	s.Rollback()
	stmt(t, s, core.RepeatableRead)
	equal(t, scanAll(t, s, e.users), "1:ann", "2:bob", "3:cid")
}

func TestIsolationAndLocks(t *testing.T) {
	e := newEnv(t)
	a, b := e.session(1), e.session(2)
	stmt(t, a, core.RepeatableRead)
	stmt(t, b, core.RepeatableRead)
	equal(t, scanAll(t, b, e.users), "1:ann", "2:bob", "3:cid") // b's snapshot
	if err := a.Update(e.users, srcRef(1), row(1, "anna"), images(1)); err != nil {
		t.Fatal(err)
	}
	// b waits on a's row lock and times out.
	err := b.Update(e.users, srcRef(1), row(1, "annie"), images(1))
	var ce *core.Error
	if !errors.As(err, &ce) || ce.Code != core.ErrLockWaitTimeout {
		t.Fatalf("want lock wait timeout, got %v", err)
	}
	if err := a.Commit(); err != nil {
		t.Fatal(err)
	}
	// b still sees its snapshot at REPEATABLE READ.
	equal(t, scanAll(t, b, e.users), "1:ann", "2:bob", "3:cid")
	b.Rollback()
	stmt(t, b, core.ReadCommitted)
	equal(t, scanAll(t, b, e.users), "1:anna", "2:bob", "3:cid")
}

func TestDeadlock(t *testing.T) {
	e := newEnv(t)
	a, b := e.session(1), e.session(2)
	stmt(t, a, core.RepeatableRead)
	stmt(t, b, core.RepeatableRead)
	if err := a.Update(e.users, srcRef(1), row(1, "a1"), images(1)); err != nil {
		t.Fatal(err)
	}
	if err := b.Update(e.users, srcRef(2), row(2, "b2"), images(2)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Update(e.users, srcRef(2), row(2, "a2"), images(2)) }()
	time.Sleep(50 * time.Millisecond)
	errB := b.Update(e.users, srcRef(1), row(1, "b1"), images(1))
	var ce *core.Error
	if !errors.As(errB, &ce) || ce.Code != core.ErrDeadlock {
		t.Fatalf("want deadlock for the younger transaction, got %v", errB)
	}
	b.Rollback()
	if err := <-done; err != nil {
		t.Fatalf("a: %v", err)
	}
}

func TestTruncateDetaches(t *testing.T) {
	e := newEnv(t)
	s := e.session(1)
	stmt(t, s, core.RepeatableRead)
	if err := s.Truncate(e.users); err != nil {
		t.Fatal(err)
	}
	s.Insert(e.users, []core.Row{row(9, "z")}, [][]core.KeyImage{images(9)})
	if err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	stmt(t, s, core.RepeatableRead)
	equal(t, scanAll(t, s, e.users), "9:z")
	d, _ := e.api.Catalog().Table(e.users)
	if d.Kind != core.Detached {
		t.Fatalf("kind = %v", d.Kind)
	}
}

func TestForeignKeys(t *testing.T) {
	e := newEnv(t)
	s := e.session(1)
	stmt(t, s, core.RepeatableRead)
	// Child with a missing parent.
	res, err := s.Insert(e.orders, []core.Row{row(20, "99")}, [][]core.KeyImage{{img(20), img(99)}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Err == nil || res[0].Err.Code != core.ErrNoReferencedRow {
		t.Fatalf("want no referenced row, got %+v", res[0].Err)
	}
	// Child with a Source parent.
	res, _ = s.Insert(e.orders, []core.Row{row(21, "3")}, [][]core.KeyImage{{img(21), img(3)}})
	if res[0].Err != nil {
		t.Fatalf("insert child: %v", res[0].Err)
	}
	// ON DELETE CASCADE removes Source children 10 and 11 of user 1.
	old := srcRef(1)
	old.Whole = row(1, "ann")
	if err := s.Delete(e.users, old); err != nil {
		t.Fatal(err)
	}
	equal(t, scanAll(t, s, e.orders), "12:2", "21:3")
}
