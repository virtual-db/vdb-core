// Package merge — lookup.go
//
// Key lookups (doc 05 §1.3) and the exact count (§1.4).
package merge

import (
	"io"

	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/txn"
	"github.com/virtual-db/vdb-core/internal/types"
)

// Lookup answers keys on index idx as a cursor (Source scaling plan K1, K2):
// the Delta answers first, key by key; then the keys the Delta does not
// decide go to the Source one key batch (vdb_lookup_batch keys) at a time,
// each read through the Source's cursor. Each row carries the index of the
// key it answers. A key value asked more than once within one key batch is
// asked of the Source once and its rows returned once per occurrence; there
// is no cache across batches (SD-4), so memory depends on the batch size, not
// on how many rows the statement has looked up.
func (m *Merge) Lookup(tx *txn.Tx, t types.TableID, idx types.IndexID, keys []types.Key, cols types.ColumnSet, lk *types.ReadLocks) (types.LookupCursor, error) {
	d, err := def(tx, t)
	if err != nil {
		return nil, err
	}
	if d.Kind == types.Provided {
		return nil, &types.Error{Code: types.ErrRefused, Table: t, Msg: "provided tables are read by scans only"}
	}
	view, err := tx.View(t)
	if err != nil {
		return nil, err
	}
	ix := d.Index(idx)
	c := &lookupCursor{
		m: m, tx: tx, d: d, view: view, idx: idx, keys: keys, cols: cols, lk: lk,
		whole:  d.Identity == types.WholeRow,
		unique: ix != nil && (ix.Unique || ix.Primary),
		found:  make([]bool, len(keys)),
	}
	c.identity = idx == d.Identity && !c.whole
	c.source = hasSource(d, view)
	if c.source && view != nil && d.ShadowsNeeded() && view.Len()+int(view.State.TouchedN) > 0 {
		c.shadows = view.ShadowImages()
	}
	return c, nil
}

// lookupCursor is one Lookup. Phases: the Delta, key by key (ki, it); then
// the Source, one key batch at a time (part, sc).
type lookupCursor struct {
	m                       *Merge
	tx                      *txn.Tx
	d                       *types.TableDef
	view                    *delta.Table
	idx                     types.IndexID
	keys                    []types.Key
	cols                    types.ColumnSet
	lk                      *types.ReadLocks
	whole, unique, identity bool
	source                  bool
	shadows                 []types.KeyImage
	found                   []bool

	// Delta phase.
	ki       int
	it       *delta.Iter
	answered bool
	pending  []int // keys the Delta did not decide

	// Source phase.
	pi    int     // next position in pending
	part  []int   // the current key batch's original key indexes
	fan   [][]int // per key asked of the Source: the original key indexes with that value
	sc    types.SourceCursor
	scEnd bool

	out  types.Batch // rows ready to return
	done bool
}

func (c *lookupCursor) Shadows() []types.KeyImage { return c.shadows }

func (c *lookupCursor) Close() error {
	if c.it != nil {
		c.it.Close()
		c.it = nil
	}
	if c.sc != nil {
		_ = c.sc.Close()
		c.sc = nil
	}
	c.done = true
	return nil
}

func (c *lookupCursor) Next(maxRows, maxBytes int) (types.Batch, error) {
	if maxRows <= 0 {
		maxRows = c.m.cfg.KeyBatch
	}
	for len(c.out.Rows) < maxRows && !c.done {
		if err := c.step(maxRows); err != nil {
			c.Close()
			return types.Batch{}, err
		}
	}
	if len(c.out.Rows) == 0 {
		return types.Batch{}, io.EOF
	}
	n := len(c.out.Rows)
	if n > maxRows {
		n = maxRows
	}
	if maxBytes > 0 {
		size := 0
		for i, r := range c.out.Rows[:n] {
			size += types.RowSize(r)
			if size > maxBytes && i > 0 {
				n = i
				break
			}
		}
	}
	b := types.Batch{Rows: c.out.Rows[:n:n], Refs: c.out.Refs[:n:n], KeyIdx: c.out.KeyIdx[:n:n]}
	c.out = types.Batch{Rows: c.out.Rows[n:], Refs: c.out.Refs[n:], KeyIdx: c.out.KeyIdx[n:]}
	return c.m.mergedHook(c.tx, c.d, c.cols, b)
}

// step adds some rows to c.out, or advances a phase.
func (c *lookupCursor) step(maxRows int) error {
	switch {
	case c.ki < len(c.keys):
		return c.deltaStep(maxRows)
	case c.sc != nil:
		return c.sourceStep(maxRows)
	case c.pi < len(c.pending) && c.source:
		return c.openBatch()
	}
	// No Source to ask: the pending keys found nothing.
	if !c.source && c.pi < len(c.pending) {
		c.part = c.pending[c.pi:]
		c.pi = len(c.pending)
		if err := c.missing(); err != nil {
			return err
		}
	}
	c.done = true
	return nil
}

func (c *lookupCursor) deltaStep(maxRows int) error {
	k := c.keys[c.ki]
	if c.view == nil {
		return c.endKey(false)
	}
	if c.identity {
		answered := false
		if r, ok := c.view.Get(k.Image); ok {
			n := len(c.out.Rows)
			if err := c.m.addDelta(c.tx, c.d, &c.out, r, uint32(c.ki), c.cols, c.lk, c.whole); err != nil {
				return err
			}
			c.found[c.ki] = len(c.out.Rows) > n
			answered = true
		} else if c.view.TouchedByImage(k.Image) {
			answered = true
		}
		return c.endKey(answered)
	}
	if c.it == nil {
		c.it = c.view.IndexInRange(c.idx, types.ImageRange{Low: k.Image, High: k.Image, LowIncl: true, HighIncl: true}, types.Forward)
		c.answered = false
	}
	for len(c.out.Rows) < maxRows {
		r := c.it.Next()
		if r == nil {
			c.it.Close()
			c.it = nil
			return c.endKey(c.answered)
		}
		n := len(c.out.Rows)
		if err := c.m.addDelta(c.tx, c.d, &c.out, r, uint32(c.ki), c.cols, c.lk, c.whole); err != nil {
			return err
		}
		if len(c.out.Rows) > n {
			c.found[c.ki] = true
		}
		// A unique index holds many NULL keys: only a key with no NULL part
		// is answered by one row.
		if c.unique && !keyHasNull(k) {
			c.answered = true
		}
	}
	return nil
}

// endKey finishes the Delta's answer for key ki. A key the Delta decides
// without a row (deleted or changed in the Delta) gets the Missing hook now;
// an undecided key goes to the Source.
func (c *lookupCursor) endKey(answered bool) error {
	ki := c.ki
	c.ki++
	if !answered {
		c.pending = append(c.pending, ki)
		return nil
	}
	return c.missingKeys([]int{ki})
}

// openBatch asks the Source for the next key batch, each distinct key value once.
func (c *lookupCursor) openBatch() error {
	end := c.pi + c.m.cfg.KeyBatch
	if end > len(c.pending) {
		end = len(c.pending)
	}
	c.part = c.pending[c.pi:end]
	c.pi = end
	first := map[string]int{}
	var req []types.Key
	c.fan = c.fan[:0]
	for _, ki := range c.part {
		raw := string(c.keys[ki].Raw)
		if j, ok := first[raw]; ok {
			c.fan[j] = append(c.fan[j], ki)
			continue
		}
		first[raw] = len(req)
		req = append(req, c.keys[ki])
		c.fan = append(c.fan, []int{ki})
	}
	snap, err := c.tx.Source()
	if err != nil {
		return err
	}
	sc, err := snap.Lookup(c.d.ID, c.idx, req, c.cols)
	if err != nil {
		return &types.Error{Code: types.ErrSource, Table: c.d.ID, Msg: err.Error()}
	}
	c.sc, c.scEnd = sc, false
	return nil
}

func (c *lookupCursor) sourceStep(maxRows int) error {
	if c.scEnd {
		_ = c.sc.Close()
		c.sc = nil
		return c.missing()
	}
	sr, err := c.sc.Next(maxRows)
	if err == io.EOF {
		c.scEnd = true
	} else if err != nil {
		return &types.Error{Code: types.ErrSource, Table: c.d.ID, Msg: err.Error()}
	}
	if len(sr.Rows) == 0 {
		if err == nil {
			c.scEnd = true // a cursor that returns nothing is done
		}
		return nil
	}
	fb, orig, ferr := c.m.filterSourceIdx(c.tx, c.d, c.view, sr, c.cols, c.lk, c.whole, nil)
	if ferr != nil {
		return ferr
	}
	for k := range fb.Rows {
		for _, ki := range c.fan[sr.KeyIdx[orig[k]]] {
			c.out.Rows = append(c.out.Rows, fb.Rows[k])
			c.out.Refs = append(c.out.Refs, fb.Refs[k])
			c.out.KeyIdx = append(c.out.KeyIdx, uint32(ki))
			c.found[ki] = true
		}
	}
	return nil
}

// missing runs the engine's Missing hook for each key of the finished batch
// that a unique lookup did not find (for InnoDB, a gap lock, D-64).
func (c *lookupCursor) missing() error { return c.missingKeys(c.part) }

func (c *lookupCursor) missingKeys(kis []int) error {
	if c.lk == nil || c.lk.Missing == nil || !c.unique {
		return nil
	}
	for _, ki := range kis {
		if !c.found[ki] {
			if err := c.lk.Missing(c.d.ID, c.idx, c.keys[ki].Image); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Merge) addDelta(tx *txn.Tx, d *types.TableDef, b *types.Batch, r *delta.DeltaRow, ki uint32, cols types.ColumnSet, lk *types.ReadLocks, whole bool) error {
	if lk != nil && lk.Row != nil {
		if err := lk.Row(d.ID, types.LockKey(r.SourceKey, r.Image)); err != nil {
			return err
		}
		latest, _ := tx.Latest(d.ID, r.Image, nil)
		if latest == nil {
			return nil
		}
		r = latest
	}
	b.Rows = append(b.Rows, types.Project(r.Row, cols))
	b.Refs = append(b.Refs, deltaRef(r, whole))
	b.KeyIdx = append(b.KeyIdx, ki)
	return nil
}

// Count returns the exact row count (product doc 05 §5): Source COUNT(*) minus
// the shadowed keys that exist on the Source plus the Delta's rows. It
// declines when there are more shadowed keys than CountDeclineKeys.
func (m *Merge) Count(tx *txn.Tx, t types.TableID) (uint64, bool, error) {
	d, err := def(tx, t)
	if err != nil {
		return 0, false, err
	}
	if d.Kind == types.Provided {
		return 0, true, nil
	}
	view, err := tx.View(t)
	if err != nil {
		return 0, false, err
	}
	var deltaRows uint64
	if view != nil {
		deltaRows = uint64(view.Len())
	}
	if !hasSource(d, view) {
		return deltaRows, false, nil
	}
	var touched []*delta.Touched
	if view != nil {
		touched = view.TouchedKeys()
	}
	if len(touched) > m.cfg.CountDeclineKeys {
		return 0, true, nil
	}
	snap, err := tx.Source()
	if err != nil {
		return 0, false, err
	}
	n, err := snap.Count(t)
	if err != nil {
		return 0, false, &types.Error{Code: types.ErrSource, Table: t, Msg: err.Error()}
	}
	var gone uint64
	for start := 0; start < len(touched); start += m.cfg.KeyBatch {
		end := start + m.cfg.KeyBatch
		if end > len(touched) {
			end = len(touched)
		}
		ks := make([]types.SourceKey, end-start)
		for i := range ks {
			ks[i] = touched[start+i].Key
		}
		ex, err := snap.Exists(t, ks)
		if err != nil {
			return 0, false, &types.Error{Code: types.ErrSource, Table: t, Msg: err.Error()}
		}
		for i, e := range ex {
			if e {
				gone += uint64(touched[start+i].Count)
			}
		}
	}
	if gone > n {
		gone = n
	}
	return n - gone + deltaRows, false, nil
}

// keyHasNull reports whether any part of a lookup key is NULL.
func keyHasNull(k types.Key) bool {
	for _, v := range types.DecodeSourceKey(types.SourceKey(k.Raw)) {
		if v.Null {
			return true
		}
	}
	return false
}
