// Package core_test — tests/unique_test.go
//
// Uniqueness and isolation over many writes in one transaction: the unique
// checks see the transaction's own earlier writes, the latest committed state
// and the Source; a cursor opened before the writes keeps its snapshot; other
// sessions with the uncommitted read view run while a transaction writes
// (run with -race). BenchmarkUpdateUniqueRows measures the cost of each update.
package core_test

import (
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/virtual-db/vdb-core"
)

// uniqEnv has one table "acct" (id, e1, e2): PRIMARY on id, UNIQUE on e1 and
// on e2. Values are decimal strings; images are their 8-byte big-endian form.
type uniqEnv struct {
	t    testing.TB
	api  core.DriverAPI
	src  *fakeSource
	acct core.TableID
}

func newUniqEnv(t testing.TB, kind core.TableKind, srcRows int) *uniqEnv {
	src := &fakeSource{tables: map[core.TableID]*fakeTable{}}
	app := core.New(core.Config{DefaultLockWait: 300 * time.Millisecond})
	app.UseDriver(&sourceServer{src: src})
	api := app.DriverAPI()
	ix := func(id core.IndexID, name string, col uint16) core.IndexDef {
		return core.IndexDef{ID: id, Name: name, Primary: id == 0, Unique: true, Columns: []uint16{col}, OrderProven: true, EqualityProven: true}
	}
	d := core.TableDef{Namespace: []string{"shop"}, Name: "acct", Kind: kind,
		Columns:  []core.ColumnDef{{Name: "id"}, {Name: "e1"}, {Name: "e2"}},
		Indexes:  []core.IndexDef{ix(0, "PRIMARY", 0), ix(1, "e1", 1), ix(2, "e2", 2)},
		Identity: 0}
	if kind == core.Mirrored {
		d.Source = &core.SourceMapping{Namespace: []string{"shop"}, Table: "acct"}
	}
	if err := api.Catalog().Load([]core.TableDef{d}, nil); err != nil {
		t.Fatal(err)
	}
	a, _ := api.Catalog().TableByName([]string{"shop"}, "acct")
	ft := &fakeTable{rows: map[int]core.Row{}}
	for i := 1; i <= srcRows; i++ {
		ft.rows[i] = arow(i, 1000+i, 5000+i)
	}
	src.tables[a.ID] = ft
	return &uniqEnv{t: t, api: api, src: src, acct: a.ID}
}

func num(i int) core.Value { return core.Value{B: []byte(strconv.Itoa(i))} }

func arow(id, e1, e2 int) core.Row { return core.Row{num(id), num(e1), num(e2)} }

func aimg(id, e1, e2 int) []core.KeyImage { return []core.KeyImage{img(id), img(e1), img(e2)} }

func (e *uniqEnv) session(conn uint64) core.Session {
	s, err := e.api.OpenSession(core.SessionInfo{ConnID: conn, Account: "app@%"})
	if err != nil {
		e.t.Fatal(err)
	}
	return s
}

func dupOn(t *testing.T, err error, ix core.IndexID) {
	t.Helper()
	ce, ok := err.(*core.Error)
	if !ok || ce.Code != core.ErrDuplicateKey || ce.Index != ix {
		t.Fatalf("want duplicate on index %d, got %v", ix, err)
	}
}

func scanAcct(t *testing.T, c core.Cursor) []string {
	t.Helper()
	var out []string
	for {
		b, err := c.Next(7, 0)
		for _, r := range b.Rows {
			out = append(out, string(r[0].B)+":"+string(r[1].B))
		}
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Every update of a long run in one transaction is checked against the
// transaction's own earlier writes, its own row excluded, and the Source.
func TestUniqueChecksSeeOwnWritesAcrossManyUpdates(t *testing.T) {
	e := newUniqEnv(t, core.Mirrored, 60)
	s := e.session(1)
	stmt(t, s, core.PerTransaction)
	for i := 1; i <= 50; i++ {
		if err := s.Update(e.acct, srcRef(i), arow(i, 2000+i, 5000+i), aimg(i, 2000+i, 5000+i)); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}
	// e1 2002 is row 2's, written earlier in this transaction.
	dupOn(t, s.Update(e.acct, core.RowRef{Origin: core.OriginDelta, PK: core.Key{Image: img(1)}}, arow(1, 2002, 5001), aimg(1, 2002, 5001)), 1)
	// e1 1052 is Source row 52's, which this transaction has not touched.
	dupOn(t, s.Update(e.acct, core.RowRef{Origin: core.OriginDelta, PK: core.Key{Image: img(1)}}, arow(1, 1052, 5001), aimg(1, 1052, 5001)), 1)
	// e2 5003 is row 3's, written earlier in this transaction.
	dupOn(t, s.Update(e.acct, core.RowRef{Origin: core.OriginDelta, PK: core.Key{Image: img(1)}}, arow(1, 2001, 5003), aimg(1, 2001, 5003)), 2)
	// e1 1002 was Source row 2's; row 2 now holds 2002, so 1002 is free.
	if err := s.Update(e.acct, core.RowRef{Origin: core.OriginDelta, PK: core.Key{Image: img(1)}}, arow(1, 1002, 5001), aimg(1, 1002, 5001)); err != nil {
		t.Fatalf("free value: %v", err)
	}
	// A row keeps its own values.
	if err := s.Update(e.acct, core.RowRef{Origin: core.OriginDelta, PK: core.Key{Image: img(3)}}, arow(3, 2003, 5003), aimg(3, 2003, 5003)); err != nil {
		t.Fatalf("own values: %v", err)
	}
	// Identity key: an insert of a key this transaction wrote, and of an untouched Source key.
	res, err := s.Insert(e.acct, []core.Row{arow(4, 9004, 9104), arow(55, 9055, 9155), arow(70, 9070, 9170)},
		[][]core.KeyImage{aimg(4, 9004, 9104), aimg(55, 9055, 9155), aimg(70, 9070, 9170)})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Err == nil || res[0].Err.Code != core.ErrDuplicateKey || res[1].Err == nil || res[1].Err.Code != core.ErrDuplicateKey || res[2].Err != nil {
		t.Fatalf("insert results %+v", res)
	}
	// Two inserts in one call: the second sees the first.
	res, _ = s.Insert(e.acct, []core.Row{arow(71, 9071, 9171), arow(72, 9071, 9172)},
		[][]core.KeyImage{aimg(71, 9071, 9171), aimg(72, 9071, 9172)})
	if res[0].Err != nil || res[1].Err == nil || res[1].Err.Index != 1 {
		t.Fatalf("same-call results %+v", res)
	}
}

// Statement rollback undoes the writes the unique checks saw.
func TestUniqueChecksAfterStatementRollback(t *testing.T) {
	e := newUniqEnv(t, core.Mirrored, 10)
	s := e.session(1)
	stmt(t, s, core.PerTransaction)
	if err := s.Update(e.acct, srcRef(1), arow(1, 3001, 5001), aimg(1, 3001, 5001)); err != nil {
		t.Fatal(err)
	}
	s.StmtEnd()
	stmt(t, s, core.PerTransaction)
	for i := 2; i <= 9; i++ {
		if err := s.Update(e.acct, srcRef(i), arow(i, 3000+i*10, 5000+i), aimg(i, 3000+i*10, 5000+i)); err != nil {
			t.Fatal(err)
		}
	}
	dupOn(t, s.Update(e.acct, srcRef(10), arow(10, 3020, 5010), aimg(10, 3020, 5010)), 1)
	if err := s.StmtRollback(); err != nil {
		t.Fatal(err)
	}
	stmt(t, s, core.PerTransaction)
	if err := s.Update(e.acct, srcRef(10), arow(10, 3020, 5010), aimg(10, 3020, 5010)); err != nil {
		t.Fatalf("after rollback: %v", err)
	}
	dupOn(t, s.Update(e.acct, srcRef(9), arow(9, 3001, 5009), aimg(9, 3001, 5009)), 1)
}

// A transaction's unique check sees what another transaction committed after
// its snapshot, while its reads keep the snapshot.
func TestUniqueChecksSeeLatestCommitted(t *testing.T) {
	e := newUniqEnv(t, core.Mirrored, 10)
	a, b := e.session(1), e.session(2)
	stmt(t, a, core.PerTransaction)
	stmt(t, b, core.PerTransaction)
	h, err := b.Scan(core.ScanRequest{Table: e.acct, Columns: core.ColumnSet{0, 1}})
	if err != nil {
		t.Fatal(err)
	}
	before := scanAcct(t, h.Merged())
	h.Close()
	if err := a.Update(e.acct, srcRef(5), arow(5, 4005, 5005), aimg(5, 4005, 5005)); err != nil {
		t.Fatal(err)
	}
	// Not committed yet: b's check does not see it.
	if err := b.Update(e.acct, srcRef(6), arow(6, 4006, 5006), aimg(6, 4006, 5006)); err != nil {
		t.Fatal(err)
	}
	if err := a.Commit(); err != nil {
		t.Fatal(err)
	}
	dupOn(t, b.Update(e.acct, srcRef(7), arow(7, 4005, 5007), aimg(7, 4005, 5007)), 1)
	h, err = b.Scan(core.ScanRequest{Table: e.acct, Columns: core.ColumnSet{0, 1}})
	if err != nil {
		t.Fatal(err)
	}
	after := scanAcct(t, h.Merged())
	h.Close()
	want := []string{}
	for _, r := range before {
		if r == "6:1006" {
			r = "6:4006"
		}
		want = append(want, r)
	}
	// b sees its own write and not a's committed one (5 keeps 1005).
	equal(t, after, want...)
}

// A cursor opened before a run of writes keeps the rows it was opened on.
func TestCursorStableAcrossWrites(t *testing.T) {
	e := newUniqEnv(t, core.Local, 0)
	s := e.session(1)
	stmt(t, s, core.PerTransaction)
	var rows []core.Row
	var ims [][]core.KeyImage
	for i := 1; i <= 40; i++ {
		rows = append(rows, arow(i, 1000+i, 5000+i))
		ims = append(ims, aimg(i, 1000+i, 5000+i))
	}
	if _, err := s.Insert(e.acct, rows, ims); err != nil {
		t.Fatal(err)
	}
	h, err := s.Scan(core.ScanRequest{Table: e.acct, Columns: core.ColumnSet{0, 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	c := h.Merged()
	first, err := c.Next(10, 0)
	if err != nil || len(first.Rows) != 10 {
		t.Fatalf("first batch: %d rows, %v", len(first.Rows), err)
	}
	for i := 1; i <= 40; i++ {
		ref := core.RowRef{Origin: core.OriginDelta, PK: core.Key{Image: img(i)}}
		if err := s.Update(e.acct, ref, arow(i, 7000+i, 5000+i), aimg(i, 7000+i, 5000+i)); err != nil {
			t.Fatal(err)
		}
	}
	got := 10
	for _, r := range scanAcct(t, c) {
		got++
		if strings.Contains(r, ":7") {
			t.Fatalf("cursor saw a later write: %s", r)
		}
	}
	if got != 40 {
		t.Fatalf("cursor returned %d rows, want 40", got)
	}
}

// Sessions with the uncommitted read view read a writer's draft while it
// writes. Run with -race: the writer's unique checks must not race them.
func TestUncommittedReadersDuringWrites(t *testing.T) {
	e := newUniqEnv(t, core.Local, 0)
	w := e.session(1)
	stmt(t, w, core.PerTransaction)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 3; r++ {
		rs := e.session(uint64(10 + r))
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				if err := rs.StmtBegin(core.StmtContext{Snapshot: core.PerStatement, View: core.ViewUncommitted}); err != nil {
					t.Error(err)
					return
				}
				h, err := rs.Scan(core.ScanRequest{Table: e.acct, Columns: core.ColumnSet{0, 1}})
				if err == nil {
					for {
						if _, err := h.Merged().Next(50, 0); err != nil {
							break
						}
					}
					h.Close()
				}
				// The reader's own unique check under the uncommitted view.
				id := base + n
				rs.Insert(e.acct, []core.Row{arow(id, id, id)}, [][]core.KeyImage{aimg(id, id, id)})
				rs.StmtEnd()
				rs.Rollback()
			}
		}(100000 * (r + 1))
	}
	for i := 1; i <= 300; i++ {
		res, err := w.Insert(e.acct, []core.Row{arow(i, 1000+i, 5000+i)}, [][]core.KeyImage{aimg(i, 1000+i, 5000+i)})
		if err != nil || res[0].Err != nil {
			t.Fatalf("insert %d: %v %+v", i, err, res)
		}
		ref := core.RowRef{Origin: core.OriginDelta, PK: core.Key{Image: img(i)}}
		if err := w.Update(e.acct, ref, arow(i, 2000+i, 5000+i), aimg(i, 2000+i, 5000+i)); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}
	close(stop)
	wg.Wait()
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
}

// BenchmarkUpdateUniqueRows updates b.N Delta rows of a table with three
// unique indexes in one transaction, the shape of a large UPDATE.
func BenchmarkUpdateUniqueRows(b *testing.B) {
	e := newUniqEnv(b, core.Local, 0)
	s := e.session(1)
	if err := s.StmtBegin(core.StmtContext{Snapshot: core.PerTransaction}); err != nil {
		b.Fatal(err)
	}
	const chunk = 1000
	for i := 1; i <= b.N; i += chunk {
		var rows []core.Row
		var ims [][]core.KeyImage
		for j := i; j < i+chunk && j <= b.N; j++ {
			rows = append(rows, arow(j, 1_000_000+j, 5_000_000+j))
			ims = append(ims, aimg(j, 1_000_000+j, 5_000_000+j))
		}
		if _, err := s.Insert(e.acct, rows, ims); err != nil {
			b.Fatal(err)
		}
	}
	s.StmtEnd()
	if err := s.StmtBegin(core.StmtContext{Snapshot: core.PerTransaction}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		ref := core.RowRef{Origin: core.OriginDelta, PK: core.Key{Image: img(i)}}
		if err := s.Update(e.acct, ref, arow(i, 3_000_000+i, 5_000_000+i), aimg(i, 3_000_000+i, 5_000_000+i)); err != nil {
			b.Fatal(err)
		}
	}
}
