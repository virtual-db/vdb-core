package write_test

import (
	"fmt"
	"testing"

	"github.com/virtual-db/vdb-core/internal/connection"
	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/framework"
	"github.com/virtual-db/vdb-core/internal/payloads"
	"github.com/virtual-db/vdb-core/internal/points"
	"github.com/virtual-db/vdb-core/internal/schema"
	. "github.com/virtual-db/vdb-core/internal/write"
)

func newWritePipe(t *testing.T) (*framework.Pipeline, *schema.Cache, *delta.Delta) {
	t.Helper()
	var global framework.GlobalContext
	pipe := framework.NewPipeline(&global)
	bus := framework.NewBus(&global)
	global = framework.SealContext(framework.NewGlobalContextBuilder(), bus, pipe)

	pipe.DeclarePipeline(points.PipelineWriteInsert, []string{
		points.PointWriteInsertBuildContext, points.PointWriteInsertApply, points.PointWriteInsertEmit,
	})
	pipe.DeclarePipeline(points.PipelineWriteUpdate, []string{
		points.PointWriteUpdateBuildContext, points.PointWriteUpdateApply, points.PointWriteUpdateEmit,
	})
	pipe.DeclarePipeline(points.PipelineWriteDelete, []string{
		points.PointWriteDeleteBuildContext, points.PointWriteDeleteApply, points.PointWriteDeleteEmit,
	})
	pipe.DeclarePipeline(points.PipelineRecordsSource, []string{
		points.PointRecordsSourceBuildContext, points.PointRecordsSourceTransform, points.PointRecordsSourceEmit,
	})
	pipe.DeclarePipeline(points.PipelineRecordsMerged, []string{
		points.PointRecordsMergedBuildContext, points.PointRecordsMergedTransform, points.PointRecordsMergedEmit,
	})
	pipe.DeclarePipeline(points.PipelineWriteTruncate, []string{
		points.PointWriteTruncateBuildContext, points.PointWriteTruncateApply, points.PointWriteTruncateEmit,
	})

	sc := schema.NewCache()
	d := delta.New()
	conns := connection.NewState()
	h := New(sc, d, conns)
	if err := h.Register(pipe); err != nil {
		t.Fatalf("Register: %v", err)
	}
	return pipe, sc, d
}

func TestHandlers_WriteInsert_AddsToDelta(t *testing.T) {
	pipe, sc, d := newWritePipe(t)

	sc.Load("users", []string{"id", "name"}, "id")

	_, err := pipe.Process(points.PipelineWriteInsert, payloads.WriteInsertPayload{
		Table:  "users",
		Record: map[string]any{"id": 1, "name": "alice"},
	})
	if err != nil {
		t.Fatalf("Process(WriteInsert): %v", err)
	}

	state, err := d.TableState("users")
	if err != nil {
		t.Fatalf("TableState: %v", err)
	}
	if got := len(state.Inserts); got != 1 {
		t.Errorf("expected 1 insert in delta, got %d", got)
	}
}

func TestHandlers_WriteUpdate_AddsToDelta(t *testing.T) {
	pipe, sc, d := newWritePipe(t)

	sc.Load("users", []string{"id", "name"}, "id")

	_, err := pipe.Process(points.PipelineWriteUpdate, payloads.WriteUpdatePayload{
		Table:     "users",
		OldRecord: map[string]any{"id": 1, "name": "alice"},
		NewRecord: map[string]any{"id": 1, "name": "alicia"},
	})
	if err != nil {
		t.Fatalf("Process(WriteUpdate): %v", err)
	}

	state, err := d.TableState("users")
	if err != nil {
		t.Fatalf("TableState: %v", err)
	}
	if got := len(state.Updates); got != 1 {
		t.Errorf("expected 1 update in delta, got %d", got)
	}
}

func TestHandlers_WriteDelete_AddsTombstone(t *testing.T) {
	pipe, sc, d := newWritePipe(t)

	sc.Load("users", []string{"id", "name"}, "id")

	_, err := pipe.Process(points.PipelineWriteDelete, payloads.WriteDeletePayload{
		Table:  "users",
		Record: map[string]any{"id": 1, "name": "alice"},
	})
	if err != nil {
		t.Fatalf("Process(WriteDelete): %v", err)
	}

	state, err := d.TableState("users")
	if err != nil {
		t.Fatalf("TableState: %v", err)
	}
	if got := len(state.Tombstones); got != 1 {
		t.Errorf("expected 1 tombstone in delta, got %d", got)
	}
}

func TestHandlers_RecordsSource_AppliesOverlay(t *testing.T) {
	pipe, sc, d := newWritePipe(t)

	sc.Load("items", []string{"id"}, "id")

	if err := d.ApplyInsert("items", map[string]any{"id": 99}); err != nil {
		t.Fatalf("ApplyInsert: %v", err)
	}

	result, err := pipe.Process(points.PipelineRecordsSource, payloads.RecordsSourcePayload{
		Table:   "items",
		Records: &sliceRecordIter{},
	})
	if err != nil {
		t.Fatalf("Process(RecordsSource): %v", err)
	}

	p, ok := result.(payloads.RecordsSourcePayload)
	if !ok {
		t.Fatalf("expected result to be RecordsSourcePayload, got %T", result)
	}
	// Consume the streaming overlay iterator.
	var records []map[string]any
	for {
		rec, err := p.Records.Next()
		if err != nil {
			break
		}
		records = append(records, rec)
	}
	if got := len(records); got != 1 {
		t.Fatalf("expected 1 record in overlay result, got %d", got)
	}
	if got := records[0]["id"]; got != 99 {
		t.Errorf("expected inserted record id=99, got %v", got)
	}
}

func TestHandlers_Register_ReturnsNilError(t *testing.T) {
	var global framework.GlobalContext
	pipe := framework.NewPipeline(&global)
	bus := framework.NewBus(&global)
	global = framework.SealContext(framework.NewGlobalContextBuilder(), bus, pipe)

	pipe.DeclarePipeline(points.PipelineWriteInsert, []string{
		points.PointWriteInsertBuildContext, points.PointWriteInsertApply, points.PointWriteInsertEmit,
	})
	pipe.DeclarePipeline(points.PipelineWriteUpdate, []string{
		points.PointWriteUpdateBuildContext, points.PointWriteUpdateApply, points.PointWriteUpdateEmit,
	})
	pipe.DeclarePipeline(points.PipelineWriteDelete, []string{
		points.PointWriteDeleteBuildContext, points.PointWriteDeleteApply, points.PointWriteDeleteEmit,
	})
	pipe.DeclarePipeline(points.PipelineRecordsSource, []string{
		points.PointRecordsSourceBuildContext, points.PointRecordsSourceTransform, points.PointRecordsSourceEmit,
	})
	pipe.DeclarePipeline(points.PipelineRecordsMerged, []string{
		points.PointRecordsMergedBuildContext, points.PointRecordsMergedTransform, points.PointRecordsMergedEmit,
	})
	pipe.DeclarePipeline(points.PipelineWriteTruncate, []string{
		points.PointWriteTruncateBuildContext, points.PointWriteTruncateApply, points.PointWriteTruncateEmit,
	})

	sc := schema.NewCache()
	d := delta.New()
	conns := connection.NewState()
	h := New(sc, d, conns)
	if err := h.Register(pipe); err != nil {
		t.Fatalf("expected Register to return nil error, got: %v", err)
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
