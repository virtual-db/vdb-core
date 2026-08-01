// Package write provides write interception and delta overlay handlers.
package write

import (
	"fmt"

	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/schema"
	"github.com/virtual-db/vdb-core/types"
)

// Overlay merges the delta state for table onto the source record slice.
// Extracted as a named function so it is unit-testable without the handler
// dispatch machinery. Returns a new slice; source is never modified.
func Overlay(
	d *delta.Delta,
	sc *schema.Cache,
	table string,
	source []map[string]any,
) ([]map[string]any, error) {

	state, err := d.TableState(table)
	if err != nil {
		return nil, fmt.Errorf("TableState(%q): %w", table, err)
	}

	// A truncated table has no source rows visible. Return only delta inserts
	// (rows inserted after the TRUNCATE), ignoring all source data.
	if state.Truncated {
		result := make([]map[string]any, 0, len(state.Inserts))
		for _, rec := range state.Inserts {
			result = append(result, rec)
		}
		return result, nil
	}

	if len(state.Inserts) == 0 && len(state.Updates) == 0 && len(state.Tombstones) == 0 {
		return source, nil
	}

	result := make([]map[string]any, 0, len(source))
	seen := make(map[string]struct{}, len(source))

	for _, rec := range source {
		key := delta.RecordKey(rec)
		seen[key] = struct{}{}

		if _, tombstoned := state.Tombstones[key]; tombstoned {
			continue
		}
		if overlay, updated := state.Updates[key]; updated {
			result = append(result, overlay)
		} else {
			result = append(result, rec)
		}
	}

	for insertKey, insertedRec := range state.Inserts {
		if _, inSource := seen[insertKey]; !inSource {
			// A tombstone for a net-new delta key means the row was deleted in
			// a later transaction. Suppress it so the row is not re-surfaced
			// after the delete committed. (Merge guarantees that inserts and
			// tombstones are mutually exclusive after every COMMIT, but we
			// check here as defense-in-depth.)
			if _, tombstoned := state.Tombstones[insertKey]; tombstoned {
				continue
			}
			// If an ODKU update targeted this net-new row (e.g. the row was
			// inserted into a prior transaction and the current transaction
			// applied an ON DUPLICATE KEY UPDATE against it), the update
			// overlay is keyed by the original insert key and takes precedence
			// over the stale insert record.
			if overlay, updated := state.Updates[insertKey]; updated {
				result = append(result, overlay)
			} else {
				result = append(result, insertedRec)
			}
		}
	}

	return result, nil
}

// OverlayIter is a streaming iterator that applies delta overlays to source
// records one at a time. Source records are never materialised — they flow
// through the iterator and are eligible for GC as soon as the iterator
// advances past them. Only the delta state (inserts, updates, tombstones)
// and the "seen" key set are held in memory.
type OverlayIter struct {
	source   types.RecordIter
	state    delta.DeltaTableState
	seen     map[string]struct{}
	exhausted bool
	// insertKeys is the pre-collected list of delta insert keys not yet yielded.
	insertKeys []string
	insertIdx  int
	closed     bool
	// passthrough is true when there are no tombstones, updates, or inserts.
	// In passthrough mode, source records are returned directly without any
	// overlay processing, eliminating the overhead of key lookups.
	passthrough bool
}

// NewOverlayIter constructs a streaming overlay iterator. The returned
// iterator must be closed by the caller. The source iterator is closed
// automatically when the overlay iterator is closed.
func NewOverlayIter(
	d *delta.Delta,
	_ *schema.Cache,
	table string,
	source types.RecordIter,
) (*OverlayIter, error) {
	state, err := d.TableState(table)
	if err != nil {
		return nil, fmt.Errorf("TableState(%q): %w", table, err)
	}

	// Pre-collect insert keys so we can stream them after source exhaustion.
	insertKeys := make([]string, 0, len(state.Inserts))
	for k := range state.Inserts {
		insertKeys = append(insertKeys, k)
	}

	return &OverlayIter{
		source:      source,
		state:       state,
		seen:        make(map[string]struct{}),
		insertKeys:  insertKeys,
		passthrough: len(state.Inserts) == 0 && len(state.Updates) == 0 && len(state.Tombstones) == 0 && !state.Truncated,
	}, nil
}

// Next returns the next record after applying the delta overlay.
// Source records are streamed one at a time; after the source is exhausted,
// delta inserts not present in the source are yielded.
func (it *OverlayIter) Next() (map[string]any, error) {
	if it.closed {
		return nil, fmt.Errorf("EOF")
	}

	// Fast path: no delta state, pass source records through directly.
	if it.passthrough {
		return it.source.Next()
	}

	// Phase 1: stream source records, applying tombstones and updates.
	if !it.exhausted {
		for {
			rec, err := it.source.Next()
			if err != nil {
				// Source exhausted — move to phase 2.
				it.exhausted = true
				break
			}
			if rec == nil {
				it.exhausted = true
				break
			}

			key := delta.RecordKey(rec)
			it.seen[key] = struct{}{}

			if _, tombstoned := it.state.Tombstones[key]; tombstoned {
				continue
			}
			if overlay, updated := it.state.Updates[key]; updated {
				return overlay, nil
			}
			return rec, nil
		}
	}

	// Phase 2: yield delta inserts not present in source.
	for it.insertIdx < len(it.insertKeys) {
		key := it.insertKeys[it.insertIdx]
		it.insertIdx++

		if _, inSource := it.seen[key]; inSource {
			continue
		}
		if _, tombstoned := it.state.Tombstones[key]; tombstoned {
			continue
		}
		if overlay, updated := it.state.Updates[key]; updated {
			return overlay, nil
		}
		return it.state.Inserts[key], nil
	}

	return nil, fmt.Errorf("EOF")
}

// Close releases the source iterator and any resources held by the overlay.
func (it *OverlayIter) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	srcErr := it.source.Close()
	it.source = nil
	it.state = delta.DeltaTableState{}
	it.seen = nil
	it.insertKeys = nil
	return srcErr
}
