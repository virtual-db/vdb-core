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
	"fmt"
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
	asked     [][]int // keys of each Source Lookup, in order
}

func (f *fakeSource) Reserve(core.SessionInfo, core.StmtContext) (core.SourceSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshots++
	// Copy the tables: the snapshot sees the Source as it is now.
	snap := &fakeSnap{src: f, tables: map[core.TableID]map[int]core.Row{}}
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
	src    *fakeSource
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

// Lookup answers keys on the primary key (index 0), or on column 1 compared
// as a decimal number (index 1): several rows per key.
func (s *fakeSnap) Lookup(t core.TableID, idx core.IndexID, keys []core.Key, cols core.ColumnSet) (core.SourceCursor, error) {
	var sr core.SourceRows
	var asked []int
	for i, k := range keys {
		n := int(binary.BigEndian.Uint64(k.Image))
		asked = append(asked, n)
		if idx == 0 {
			if r, ok := s.tables[t][n]; ok {
				sr.Rows = append(sr.Rows, core.Project(r, cols))
				sr.Keys = append(sr.Keys, key(n))
				sr.KeyIdx = append(sr.KeyIdx, uint32(i))
			}
			continue
		}
		for _, id := range s.sorted(t) {
			r := s.tables[t][id]
			if string(r[1].B) == strconv.Itoa(n) {
				sr.Rows = append(sr.Rows, core.Project(r, cols))
				sr.Keys = append(sr.Keys, key(id))
				sr.KeyIdx = append(sr.KeyIdx, uint32(i))
			}
		}
	}
	s.src.mu.Lock()
	s.src.asked = append(s.src.asked, asked)
	s.src.mu.Unlock()
	return &fakeCursor{sr: sr}, nil
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
func (s *fakeSnap) Start() error { return nil }

// fakeCursor returns its rows maxRows at a time, as a paging Source would.
type fakeCursor struct {
	sr core.SourceRows
	i  int
}

func (c *fakeCursor) Next(maxRows int) (core.SourceRows, error) {
	if c.i >= len(c.sr.Rows) {
		return core.SourceRows{}, io.EOF
	}
	end := len(c.sr.Rows)
	if maxRows > 0 && c.i+maxRows < end {
		end = c.i + maxRows
	}
	var out core.SourceRows
	out.Rows = c.sr.Rows[c.i:end]
	if len(c.sr.Keys) > 0 {
		out.Keys = c.sr.Keys[c.i:end]
	}
	if len(c.sr.KeyIdx) > 0 {
		out.KeyIdx = c.sr.KeyIdx[c.i:end]
	}
	c.i = end
	return out, nil
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
	app.UseDriver(&sourceServer{src: src})
	api := app.DriverAPI()
	pk := core.IndexDef{ID: 0, Name: "PRIMARY", Primary: true, Unique: true, Columns: []uint16{0}, OrderProven: true, EqualityProven: true}
	users := core.TableDef{Namespace: []string{"shop"}, Name: "users", Kind: core.Mirrored,
		Columns: []core.ColumnDef{{Name: "id"}, {Name: "name"}}, Indexes: []core.IndexDef{pk}, Identity: 0,
		Source: &core.SourceMapping{Namespace: []string{"shop"}, Table: "users"}}
	orders := core.TableDef{Namespace: []string{"shop"}, Name: "orders", Kind: core.Mirrored,
		Columns:  []core.ColumnDef{{Name: "id"}, {Name: "user_id", Nullable: true}},
		Indexes:  []core.IndexDef{pk, {ID: 1, Name: "user_id", Columns: []uint16{1}, OrderProven: true, EqualityProven: true}},
		Identity: 0, Source: &core.SourceMapping{Namespace: []string{"shop"}, Table: "orders"}}
	if err := api.Catalog().Load([]core.TableDef{users, orders}, nil); err != nil {
		t.Fatal(err)
	}
	u, _ := api.Catalog().TableByName([]string{"shop"}, "users")
	o, _ := api.Catalog().TableByName([]string{"shop"}, "orders")
	src.tables[u.ID] = &fakeTable{rows: map[int]core.Row{1: row(1, "ann"), 2: row(2, "bob"), 3: row(3, "cid")}}
	src.tables[o.ID] = &fakeTable{rows: map[int]core.Row{10: row(10, "1"), 11: row(11, "1"), 12: row(12, "2")}}
	return &env{t: t, app: app, api: api, src: src, users: u.ID, orders: o.ID}
}

func (e *env) session(conn uint64) core.Session {
	s, err := e.api.OpenSession(core.SessionInfo{ConnID: conn, Account: "app@%"})
	if err != nil {
		e.t.Fatal(err)
	}
	return s
}

func stmt(t *testing.T, s core.Session, scope core.SnapshotScope) {
	if err := s.StmtBegin(core.StmtContext{Snapshot: scope}); err != nil {
		t.Fatal(err)
	}
}

// Test lock modes: shared and exclusive, as an engine would define them.
const (
	lockS core.LockMode = iota
	lockX
)

func (e *env) defineLocks() {
	e.api.Locks().Define(core.LockModes{Names: []string{"S", "X"},
		Conflicts: func(h, w core.LockMode, _, _ core.Resource) bool { return h == lockX || w == lockX },
		Covers:    func(h, w core.LockMode) bool { return h == lockX || h == w }})
}

// lockRow takes the engine's exclusive lock on a row before it writes it.
func (e *env) lockRow(s core.Session, ref core.RowRef) error {
	return e.api.Locks().Acquire(s, core.Resource{Table: e.users, Point: core.LockKey(ref.SourceKey, ref.PK.Image)}, lockX, nil)
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
	stmt(t, s, core.PerTransaction)
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
	b := lookupAll(t, s, e.users, 0, []core.Key{{Image: img(1)}, {Image: img(2)}, {Image: img(3)}, {Image: img(4)}}, core.ColumnSet{1}, nil, 2)
	var got []string
	for i, r := range b.Rows {
		got = append(got, strconv.Itoa(int(b.KeyIdx[i]))+"="+string(r[0].B))
	}
	equal(t, got, "0=ann", "1=bobby", "3=dan")
	if err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	s2 := e.session(2)
	stmt(t, s2, core.PerTransaction)
	equal(t, scanAll(t, s2, e.users), "1:ann", "2:bobby", "4:dan")
}

func TestDuplicates(t *testing.T) {
	e := newEnv(t)
	s := e.session(1)
	stmt(t, s, core.PerTransaction)
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
	stmt(t, s, core.PerTransaction)
	s.Insert(e.users, []core.Row{row(6, "a")}, [][]core.KeyImage{images(6)})
	s.StmtEnd()
	s.Savepoint("sp1")
	stmt(t, s, core.PerTransaction)
	s.Insert(e.users, []core.Row{row(7, "b")}, [][]core.KeyImage{images(7)})
	s.StmtEnd()
	stmt(t, s, core.PerTransaction)
	s.Insert(e.users, []core.Row{row(8, "c")}, [][]core.KeyImage{images(8)})
	if err := s.StmtRollback(); err != nil {
		t.Fatal(err)
	}
	stmt(t, s, core.PerTransaction)
	equal(t, scanAll(t, s, e.users), "1:ann", "2:bob", "3:cid", "6:a", "7:b")
	if err := s.RollbackTo("sp1"); err != nil {
		t.Fatal(err)
	}
	equal(t, scanAll(t, s, e.users), "1:ann", "2:bob", "3:cid", "6:a")
	s.Rollback()
	stmt(t, s, core.PerTransaction)
	equal(t, scanAll(t, s, e.users), "1:ann", "2:bob", "3:cid")
}

func TestIsolationAndLocks(t *testing.T) {
	e := newEnv(t)
	a, b := e.session(1), e.session(2)
	e.defineLocks()
	stmt(t, a, core.PerTransaction)
	stmt(t, b, core.PerTransaction)
	equal(t, scanAll(t, b, e.users), "1:ann", "2:bob", "3:cid") // b's snapshot
	if err := e.lockRow(a, srcRef(1)); err != nil {
		t.Fatal(err)
	}
	if err := a.Update(e.users, srcRef(1), row(1, "anna"), images(1)); err != nil {
		t.Fatal(err)
	}
	// b waits on a's row lock and times out.
	err := e.lockRow(b, srcRef(1))
	var ce *core.Error
	if !errors.As(err, &ce) || ce.Code != core.ErrLockWaitTimeout {
		t.Fatalf("want lock wait timeout, got %v", err)
	}
	if err := a.Commit(); err != nil {
		t.Fatal(err)
	}
	// b still sees its snapshot with a per-transaction scope.
	equal(t, scanAll(t, b, e.users), "1:ann", "2:bob", "3:cid")
	b.Rollback()
	stmt(t, b, core.PerStatement)
	equal(t, scanAll(t, b, e.users), "1:anna", "2:bob", "3:cid")
}

func TestDeadlock(t *testing.T) {
	e := newEnv(t)
	a, b := e.session(1), e.session(2)
	e.defineLocks()
	stmt(t, a, core.PerTransaction)
	stmt(t, b, core.PerTransaction)
	if err := e.lockRow(a, srcRef(1)); err != nil {
		t.Fatal(err)
	}
	if err := e.lockRow(b, srcRef(2)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.lockRow(a, srcRef(2)) }()
	time.Sleep(50 * time.Millisecond)
	errB := e.lockRow(b, srcRef(1))
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
	stmt(t, s, core.PerTransaction)
	if err := s.Truncate(e.users); err != nil {
		t.Fatal(err)
	}
	s.Insert(e.users, []core.Row{row(9, "z")}, [][]core.KeyImage{images(9)})
	if err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	stmt(t, s, core.PerTransaction)
	equal(t, scanAll(t, s, e.users), "9:z")
	d, _ := e.api.Catalog().Table(e.users)
	if d.Kind != core.Detached {
		t.Fatalf("kind = %v", d.Kind)
	}
}

func TestByColumns(t *testing.T) {
	e := newEnv(t)
	s := e.session(1)
	stmt(t, s, core.PerTransaction)
	// A Delta child of user 3, then the children of user 1 and of user 3.
	if res, err := s.Insert(e.orders, []core.Row{row(21, "3")}, [][]core.KeyImage{{img(21), img(3)}}); err != nil || res[0].Err != nil {
		t.Fatalf("insert: %v %v", err, res)
	}
	b := byColumnsAll(t, s, e.orders, 1, img(1), core.Row{{B: []byte("1")}}, nil)
	if len(b.Rows) != 2 {
		t.Fatalf("children of user 1: %d rows, want 2 (Source rows 10 and 11)", len(b.Rows))
	}
	b = byColumnsAll(t, s, e.orders, 1, img(3), core.Row{{B: []byte("3")}}, nil)
	if len(b.Rows) != 1 || string(b.Rows[0][0].B) != "21" {
		t.Fatalf("children of user 3: %v, want the Delta row 21", b.Rows)
	}
}

// ── lookups and column reads as cursors (issue #17) ──

func drain(t *testing.T, c core.Cursor, maxRows int) (core.Batch, int) {
	t.Helper()
	var all core.Batch
	calls := 0
	for {
		b, err := c.Next(maxRows, 0)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		calls++
		if len(b.Rows) > maxRows {
			t.Fatalf("Next(%d) returned %d rows", maxRows, len(b.Rows))
		}
		all.Rows = append(all.Rows, b.Rows...)
		all.Refs = append(all.Refs, b.Refs...)
		all.KeyIdx = append(all.KeyIdx, b.KeyIdx...)
	}
	c.Close()
	return all, calls
}

func lookupAll(t *testing.T, s core.Session, tb core.TableID, idx core.IndexID, keys []core.Key, cols core.ColumnSet, lk *core.ReadLocks, maxRows int) core.Batch {
	t.Helper()
	c, err := s.Lookup(tb, idx, keys, cols, lk)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := drain(t, c, maxRows)
	return b
}

func byColumnsAll(t *testing.T, s core.Session, tb core.TableID, idx core.IndexID, im core.KeyImage, vals core.Row, lk *core.ReadLocks) core.Batch {
	t.Helper()
	c, err := s.ByColumns(tb, idx, im, core.ColumnSet{1}, vals, core.ColumnSet{0, 1}, lk)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := drain(t, c, 1)
	return b
}

func keyRows(b core.Batch, col int) []string {
	var out []string
	for i, r := range b.Rows {
		out = append(out, strconv.Itoa(int(b.KeyIdx[i]))+"="+string(r[col].B))
	}
	return out
}

// TestLookupRepeatedKeyInBatch: a key asked twice in one key batch is asked
// of the Source once, and its rows come back once per occurrence.
func TestLookupRepeatedKeyInBatch(t *testing.T) {
	e := newEnv(t)
	s := e.session(1)
	stmt(t, s, core.PerTransaction)
	k := func(i int) core.Key { return core.Key{Image: img(i), Raw: []byte(strconv.Itoa(i))} }
	b := lookupAll(t, s, e.users, 0, []core.Key{k(1), k(2), k(1)}, core.ColumnSet{1}, nil, 10)
	equal(t, keyRows(b, 0), "0=ann", "1=bob", "2=ann")
	if len(e.src.asked) != 1 || len(e.src.asked[0]) != 2 {
		t.Fatalf("Source asked %v, want one request for keys 1 and 2", e.src.asked)
	}
	// No cache across lookups (SD-4): the next lookup asks again.
	lookupAll(t, s, e.users, 0, []core.Key{k(1)}, core.ColumnSet{1}, nil, 10)
	if len(e.src.asked) != 2 {
		t.Fatalf("Source asked %v, want a second request", e.src.asked)
	}
}

// TestLookupManyRowsPerKey: a key with more rows than one page spans several
// Next calls, and the Source is read one key batch per request.
func TestLookupManyRowsPerKey(t *testing.T) {
	e := newEnv(t)
	rows := map[int]core.Row{}
	for i := 0; i < 50; i++ {
		rows[100+i] = row(100+i, strconv.Itoa(1+i%2)) // 25 orders of user 1, 25 of user 2
	}
	e.src.tables[e.orders].rows = rows
	s := e.session(1)
	stmt(t, s, core.PerTransaction)
	k := func(i int) core.Key { return core.Key{Image: img(i), Raw: []byte(strconv.Itoa(i))} }
	c, err := s.Lookup(e.orders, 1, []core.Key{k(1), k(2), k(1)}, core.ColumnSet{0, 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, calls := drain(t, c, 4)
	if len(b.Rows) != 75 {
		t.Fatalf("%d rows, want 75 (25 for each of three keys)", len(b.Rows))
	}
	if calls < 75/4 {
		t.Fatalf("%d Next calls for 75 rows of at most 4", calls)
	}
	per := map[uint32]int{}
	for i, r := range b.Rows {
		per[b.KeyIdx[i]]++
		want := "1"
		if b.KeyIdx[i] == 1 {
			want = "2"
		}
		if string(r[1].B) != want {
			t.Fatalf("row %v answers key %d", r, b.KeyIdx[i])
		}
	}
	if per[0] != 25 || per[1] != 25 || per[2] != 25 {
		t.Fatalf("rows per key %v", per)
	}
	if len(e.src.asked) != 1 || len(e.src.asked[0]) != 2 {
		t.Fatalf("Source asked %v, want one request for user ids 1 and 2", e.src.asked)
	}
}

// TestLookupDeltaFirstAndMissing: Delta answers come first; the Missing hook
// sees each unique key found nowhere, including keys the Delta deleted.
func TestLookupDeltaFirstAndMissing(t *testing.T) {
	e := newEnv(t)
	s := e.session(1)
	stmt(t, s, core.PerTransaction)
	if res, err := s.Insert(e.users, []core.Row{row(4, "dan")}, [][]core.KeyImage{images(4)}); err != nil || res[0].Err != nil {
		t.Fatalf("insert: %v %v", err, res)
	}
	if err := s.Delete(e.users, srcRef(3)); err != nil {
		t.Fatal(err)
	}
	var missing []int
	lk := &core.ReadLocks{Missing: func(_ core.TableID, _ core.IndexID, im core.KeyImage) error {
		missing = append(missing, int(binary.BigEndian.Uint64(im)))
		return nil
	}}
	k := func(i int) core.Key { return core.Key{Image: img(i), Raw: []byte(strconv.Itoa(i))} }
	b := lookupAll(t, s, e.users, 0, []core.Key{k(1), k(4), k(3), k(9)}, core.ColumnSet{1}, lk, 1)
	equal(t, keyRows(b, 0), "1=dan", "0=ann")
	sort.Ints(missing)
	if len(missing) != 2 || missing[0] != 3 || missing[1] != 9 {
		t.Fatalf("missing %v, want [3 9]", missing)
	}
}

// TestByColumnsStopsEarly: a caller that needs one child (RESTRICT) reads one
// batch and closes the cursor.
func TestByColumnsStopsEarly(t *testing.T) {
	e := newEnv(t)
	s := e.session(1)
	stmt(t, s, core.PerTransaction)
	c, err := s.ByColumns(e.orders, 1, img(1), core.ColumnSet{1}, core.Row{{B: []byte("1")}}, core.ColumnSet{0, 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Next(1, 0)
	if err != nil || len(b.Rows) != 1 {
		t.Fatalf("first child: %v %v", b.Rows, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestReadLocksProbe: a locking read calls Probe for every row it locks,
// with the version it returns; Source rows come with the read's columns,
// Delta rows with every column (vdb-mysql-engine#6). RangeIndexes names the
// indexes on which other transactions hold ranges.
func TestReadLocksProbe(t *testing.T) {
	e := newEnv(t)
	e.defineLocks()
	a, b := e.session(1), e.session(2)
	stmt(t, a, core.PerTransaction)
	stmt(t, b, core.PerTransaction)
	if err := e.lockRow(a, srcRef(2)); err != nil {
		t.Fatal(err)
	}
	if err := a.Update(e.users, srcRef(2), row(2, "bo"), images(2)); err != nil {
		t.Fatal(err)
	}
	if err := a.Commit(); err != nil {
		t.Fatal(err)
	}
	stmt(t, a, core.PerTransaction)
	var probed []string
	lk := &core.ReadLocks{
		Row: func(tb core.TableID, key core.KeyImage) error {
			return e.api.Locks().Acquire(a, core.Resource{Table: tb, Point: key}, lockS, nil)
		},
		Probe: func(tb core.TableID, r core.Row, cols core.ColumnSet) error {
			probed = append(probed, fmt.Sprintf("%s:%s cols=%v", r[0].B, r[len(r)-1].B, cols))
			return nil
		},
	}
	h, err := a.Scan(core.ScanRequest{Table: e.users, Columns: core.ColumnSet{0, 1}, Locks: lk})
	if err != nil {
		t.Fatal(err)
	}
	for {
		_, err := h.Merged().Next(100, 0)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	h.Close()
	sort.Strings(probed)
	equal(t, probed, "1:ann cols=[0 1]", "2:bo cols=[]", "3:cid cols=[0 1]")

	// Ranges: b holds one on index 0 of users; a sees it, b does not.
	if err := e.api.Locks().Acquire(b, core.Resource{Table: e.users, Index: 0, Range: &core.ImageRange{Low: img(1), High: img(2), LowIncl: true, HighIncl: true}}, lockS, nil); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(e.api.Locks().RangeIndexes(a, e.users)); got != "[0]" {
		t.Fatalf("RangeIndexes for a: %s", got)
	}
	if got := fmt.Sprint(e.api.Locks().RangeIndexes(b, e.users)); got != "[]" {
		t.Fatalf("RangeIndexes for b: %s", got)
	}
	if got := fmt.Sprint(e.api.Locks().RangeIndexes(a, e.orders)); got != "[]" {
		t.Fatalf("RangeIndexes for orders: %s", got)
	}
}
