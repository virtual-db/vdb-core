// Package merge — lookup.go
//
// Key lookups (doc 05 §1.3) and the exact count (§1.4).
package merge

import (
	"strconv"

	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/txn"
	"github.com/virtual-db/vdb-core/internal/types"
)

// LookupResult is a lookup's rows with the shadow set the engine applies to
// Source rows when the table needs it.
type LookupResult struct {
	Batch   types.Batch
	Shadows []types.KeyImage
}

// Lookup answers keys on index idx: the Delta first, the Source only for the
// keys the Delta does not decide. Each row carries the index of the key it answers.
func (m *Merge) Lookup(tx *txn.Tx, t types.TableID, idx types.IndexID, keys []types.Key, cols types.ColumnSet, lk *types.ReadLocks) (LookupResult, error) {
	d, err := def(tx, t)
	if err != nil {
		return LookupResult{}, err
	}
	var out LookupResult
	if d.Kind == types.Provided {
		return out, &types.Error{Code: types.ErrRefused, Table: t, Msg: "provided tables are read by scans only"}
	}
	view, err := tx.View(t)
	if err != nil {
		return out, err
	}
	whole := d.Identity == types.WholeRow
	identity := idx == d.Identity && !whole
	ix := d.Index(idx)
	unique := ix != nil && (ix.Unique || ix.Primary)

	var pending []int
	for i, k := range keys {
		answered := false
		if view != nil {
			if identity {
				if r, ok := view.Get(k.Image); ok {
					if err := m.addDelta(tx, d, &out.Batch, r, uint32(i), cols, lk, whole); err != nil {
						return out, err
					}
					answered = true
				} else if view.TouchedByImage(k.Image) {
					answered = true
				}
			} else {
				it := view.IndexInRange(idx, types.ImageRange{Low: k.Image, High: k.Image, LowIncl: true, HighIncl: true}, types.Forward)
				for r := it.Next(); r != nil; r = it.Next() {
					if err := m.addDelta(tx, d, &out.Batch, r, uint32(i), cols, lk, whole); err != nil {
						it.Close()
						return out, err
					}
					// A unique index holds many NULL keys: only a key with
					// no NULL part is answered by one row.
					if unique && !keyHasNull(k) {
						answered = true
					}
				}
				it.Close()
			}
		}
		if !answered {
			pending = append(pending, i)
		}
	}

	found := make([]bool, len(keys))
	for _, ki := range out.Batch.KeyIdx {
		found[ki] = true
	}
	if len(pending) > 0 && hasSource(d, view) {
		if err := m.lookupSource(tx, d, view, idx, keys, pending, cols, lk, whole, &out.Batch, found); err != nil {
			return out, err
		}
		if view != nil && d.ShadowsNeeded() && view.Len()+int(view.State.TouchedN) > 0 {
			out.Shadows = view.ShadowImages()
		}
	}
	// The engine's Missing hook sees every key a unique lookup did not find
	// (for InnoDB, a gap lock, D-64).
	if lk != nil && lk.Missing != nil && unique {
		for i, k := range keys {
			if !found[i] {
				if err := lk.Missing(t, idx, k.Image); err != nil {
					return out, err
				}
			}
		}
	}
	var err2 error
	out.Batch, err2 = m.mergedHook(tx, d, cols, out.Batch)
	return out, err2
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
		if err := probe(lk, d.ID, r.Row, nil); err != nil {
			return err
		}
	}
	b.Rows = append(b.Rows, types.Project(r.Row, cols))
	b.Refs = append(b.Refs, deltaRef(r, whole))
	b.KeyIdx = append(b.KeyIdx, ki)
	return nil
}

func (m *Merge) lookupSource(tx *txn.Tx, d *types.TableDef, view *delta.Table, idx types.IndexID, keys []types.Key, pending []int,
	cols types.ColumnSet, lk *types.ReadLocks, whole bool, b *types.Batch, found []bool) error {
	snap, err := tx.Source()
	if err != nil {
		return err
	}
	cacheKey := func(k types.Key) string {
		return strconv.Itoa(int(d.ID)) + "/" + strconv.Itoa(int(idx)) + "/" + colsKey(cols) + "/" + string(k.Raw)
	}
	var ask []int
	for _, i := range pending {
		if c, ok := tx.LookupCache[cacheKey(keys[i])]; ok {
			sr := types.SourceRows{Rows: c.Rows, Keys: c.Keys, KeyIdx: make([]uint32, len(c.Rows))}
			for j := range sr.KeyIdx {
				sr.KeyIdx[j] = uint32(i)
			}
			if err := m.addSource(tx, d, view, sr, cols, lk, whole, b, found); err != nil {
				return err
			}
			continue
		}
		ask = append(ask, i)
	}
	for start := 0; start < len(ask); start += m.cfg.KeyBatch {
		end := start + m.cfg.KeyBatch
		if end > len(ask) {
			end = len(ask)
		}
		part := ask[start:end]
		req := make([]types.Key, len(part))
		for j, i := range part {
			req[j] = keys[i]
		}
		sr, err := snap.Lookup(d.ID, idx, req, cols)
		if err != nil {
			return &types.Error{Code: types.ErrSource, Table: d.ID, Msg: err.Error()}
		}
		// Map the batch-local key index back and fill the cache.
		if tx.LookupCache == nil {
			tx.LookupCache = map[string]txn.CachedLookup{}
		}
		// A key value asked more than once in the batch is answered once per
		// occurrence; the cache keeps the answer of its first occurrence only,
		// or later lookups of it would return its rows twice.
		first := map[string]uint32{}
		for bi, i := range part {
			ck := cacheKey(keys[i])
			if _, ok := first[ck]; !ok {
				first[ck] = uint32(bi)
				tx.LookupCache[ck] = txn.CachedLookup{}
			}
		}
		for j := range sr.Rows {
			bi := sr.KeyIdx[j]
			orig := part[bi]
			sr.KeyIdx[j] = uint32(orig)
			ck := cacheKey(keys[orig])
			if first[ck] != bi {
				continue
			}
			c := tx.LookupCache[ck]
			c.Rows = append(c.Rows, sr.Rows[j])
			if j < len(sr.Keys) {
				c.Keys = append(c.Keys, sr.Keys[j])
			} else {
				c.Keys = append(c.Keys, nil)
			}
			tx.LookupCache[ck] = c
		}
		if err := m.addSource(tx, d, view, sr, cols, lk, whole, b, found); err != nil {
			return err
		}
	}
	return nil
}

func (m *Merge) addSource(tx *txn.Tx, d *types.TableDef, view *delta.Table, sr types.SourceRows, cols types.ColumnSet,
	lk *types.ReadLocks, whole bool, b *types.Batch, found []bool) error {
	fb, orig, err := m.filterSourceIdx(tx, d, view, sr, cols, lk, whole, nil)
	if err != nil {
		return err
	}
	for k := range fb.Rows {
		ki := sr.KeyIdx[orig[k]]
		b.Rows = append(b.Rows, fb.Rows[k])
		b.Refs = append(b.Refs, fb.Refs[k])
		b.KeyIdx = append(b.KeyIdx, ki)
		found[ki] = true
	}
	return nil
}

func colsKey(c types.ColumnSet) string {
	var b []byte
	for _, x := range c {
		b = strconv.AppendUint(b, uint64(x), 10)
		b = append(b, ',')
	}
	return string(b)
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
