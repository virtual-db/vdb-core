package driverapi_test

import (
	"errors"
	"fmt"
	"testing"

	. "github.com/virtual-db/vdb-core/internal/driverapi"
	"github.com/virtual-db/vdb-core/internal/payloads"
	"github.com/virtual-db/vdb-core/internal/points"
)

// compile-time check: Impl is reachable via the dot import.
var _ *Impl

// ---------------------------------------------------------------------------
// RecordsSource
// ---------------------------------------------------------------------------

func TestRecordsSource_HandlerCanModifyRecords(t *testing.T) {
	impl, pipe, _, _, _ := newTestImpl(t)

	pipe.DeclarePipeline(points.PipelineRecordsSource, []string{points.PointRecordsSourceTransform})
	pipe.MustAttach(points.PointRecordsSourceTransform, 10, func(ctx any, p any) (any, any, error) {
		modified := payloads.RecordsSourcePayload{
			Records: &sliceRecordIter{records: []map[string]any{{"id": 42}}},
		}
		return ctx, modified, nil
	})

	got, err := impl.RecordsSource(1, "t", &sliceRecordIter{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Consume the streaming iterator.
	var records []map[string]any
	for {
		rec, err := got.Next()
		if err != nil {
			break
		}
		records = append(records, rec)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0]["id"] != 42 {
		t.Fatalf("expected record id 42, got %v", records[0]["id"])
	}
}

func TestRecordsSource_HandlerError_IsReturned(t *testing.T) {
	impl, pipe, _, _, _ := newTestImpl(t)

	pipe.DeclarePipeline(points.PipelineRecordsSource, []string{points.PointRecordsSourceTransform})
	pipe.MustAttach(points.PointRecordsSourceTransform, 10, func(ctx any, p any) (any, any, error) {
		return ctx, p, errors.New("injected records source error")
	})

	got, err := impl.RecordsSource(1, "t", &sliceRecordIter{})
	if err == nil {
		t.Fatal("expected non-nil error from handler, got nil")
	}
	if got != nil {
		t.Fatalf("expected nil records on error, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// RecordsMerged
// ---------------------------------------------------------------------------

// TestRecordsMerged_HandlerError_ReturnsOriginalRecords verifies the
// fire-and-forget fallback: when the pipeline returns an error, RecordsMerged
// logs the error and returns the original records unchanged (not nil, not the
// error).
func TestRecordsMerged_HandlerError_ReturnsOriginalRecords(t *testing.T) {
	impl, pipe, _, _, _ := newTestImpl(t)

	original := []map[string]any{{"id": 1}}

	pipe.DeclarePipeline(points.PipelineRecordsMerged, []string{points.PointRecordsMergedTransform})
	pipe.MustAttach(points.PointRecordsMergedTransform, 10, func(ctx any, p any) (any, any, error) {
		return ctx, p, errors.New("injected records merged error")
	})

	got, err := impl.RecordsMerged(1, "t", original)
	if err != nil {
		t.Fatalf("expected nil error (fire-and-forget fallback), got: %v", err)
	}
	if got == nil {
		t.Fatal("expected original records to be returned, got nil")
	}
	if len(got) != len(original) {
		t.Fatalf("expected %d records, got %d", len(original), len(got))
	}
	if got[0]["id"] != original[0]["id"] {
		t.Fatalf("expected record id %v, got %v", original[0]["id"], got[0]["id"])
	}
}

// sliceRecordIter is a test helper that adapts a slice to payloads.RecordIter.
type sliceRecordIter struct {
	records []map[string]any
	idx     int
}

func (it *sliceRecordIter) Next() (map[string]any, error) {
	if it.idx >= len(it.records) {
		return nil, fmt.Errorf("EOF")
	}
	rec := it.records[it.idx]
	it.idx++
	return rec, nil
}

func (it *sliceRecordIter) Close() error { return nil }
