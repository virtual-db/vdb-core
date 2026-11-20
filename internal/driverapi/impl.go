// Package driverapi implements types.DriverAPI (implementation plan doc 03
// §4): sessions, the catalog, client connections and statements, status.
// Write and transaction calls run their pipelines; reads call merge directly
// and run vdb.records.source and vdb.records.merged once per batch.
package driverapi

import (
	"fmt"

	"github.com/virtual-db/vdb-core/internal/catalog"
	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/framework"
	"github.com/virtual-db/vdb-core/internal/memory"
	"github.com/virtual-db/vdb-core/internal/payloads"
	"github.com/virtual-db/vdb-core/internal/points"
	"github.com/virtual-db/vdb-core/internal/session"
	"github.com/virtual-db/vdb-core/internal/txn"
	"github.com/virtual-db/vdb-core/internal/types"
)

// Pipelines is what the implementation needs from the framework.
type Pipelines interface {
	Process(pipeline string, payload any) (any, error)
	HasHandlers(point string) bool
	Emit(event string, payload any)
	Transforms() []string
}

// Impl implements types.DriverAPI.
type Impl struct {
	p        Pipelines
	sessions *session.Registry
	txn      *txn.Manager
	catalog  *catalog.Catalog
	mem      *memory.Guard
}

var _ types.DriverAPI = (*Impl)(nil)

// New returns an Impl.
func New(p Pipelines, sessions *session.Registry, m *txn.Manager, cat *catalog.Catalog, mem *memory.Guard) *Impl {
	return &Impl{p: p, sessions: sessions, txn: m, catalog: cat, mem: mem}
}

// OpenSession opens a session for a mysqld connection.
func (a *Impl) OpenSession(info types.SessionInfo) (types.Session, error) {
	return a.sessions.Open(info), nil
}

// Kill ends any lock wait of the connection and marks its session killed.
func (a *Impl) Kill(connID uint64) { a.sessions.Kill(connID) }

// Catalog returns the catalog of record.
func (a *Impl) Catalog() types.Catalog { return a.catalog }

// ConnectionOpened runs vdb.connection.opened.
func (a *Impl) ConnectionOpened(connID uint64, account, addr string) error {
	_, err := a.p.Process(points.PipelineConnectionOpened, payloads.ConnectionOpenedPayload{ConnectionID: connID, User: account, Address: addr})
	if err != nil {
		return fmt.Errorf("driverapi.connection_opened: %w", err)
	}
	return nil
}

// ConnectionClosed runs vdb.connection.closed.
func (a *Impl) ConnectionClosed(connID uint64, account, addr string) {
	_, _ = a.p.Process(points.PipelineConnectionClosed, payloads.ConnectionClosedPayload{ConnectionID: connID, User: account, Address: addr})
}

// QueryReceived runs vdb.query.received and returns the possibly rewritten statement.
func (a *Impl) QueryReceived(connID uint64, query, database string) (string, error) {
	out, err := a.p.Process(points.PipelineQueryReceived, payloads.QueryReceivedPayload{ConnectionID: connID, Query: query, Database: database})
	if err != nil {
		return "", fmt.Errorf("driverapi.query_received: %w", err)
	}
	pl, err := payloads.Decode[payloads.QueryReceivedPayload](out)
	if err != nil {
		return query, nil
	}
	return pl.Query, nil
}

// QueryCompleted emits vdb.query.completed.
func (a *Impl) QueryCompleted(connID uint64, q types.QueryInfo) {
	a.p.Emit(points.EventQueryCompleted, payloads.QueryCompletedPayload{
		ConnectionID: connID, Query: q.Query, Digest: q.Digest, Database: q.Database, Account: q.Account,
		DurationMs: float64(q.Duration.Microseconds()) / 1000, RowsAffected: q.RowsAffected, RowsSent: q.RowsSent, ErrorCode: q.ErrorCode})
}

// DeltaStats reports the Delta's counters.
func (a *Impl) DeltaStats() types.DeltaStats { return a.txn.Store().Stats() }

// LockStats reports the lock manager's counters.
func (a *Impl) LockStats() types.LockStats { return a.txn.Locks().Stats() }

// MemoryStats reports the memory guard's readings.
func (a *Impl) MemoryStats() types.MemoryStats { return a.mem.Stats() }

// Transforms lists the columns plugins declared they transform.
func (a *Impl) Transforms() []string { return a.p.Transforms() }

// Emit publishes an engine event.
func (a *Impl) Emit(event string, payload any) { a.p.Emit(event, payload) }

// HasHandlers reports whether anything besides the built-in handlers is attached to a point.
func (a *Impl) HasHandlers(point string) bool { return a.p.HasHandlers(point) }

// Delta returns a stable read of the committed Delta.
func (a *Impl) Delta() types.DeltaReader { return reader{a.txn.Store().Snapshot()} }

type reader struct{ r *delta.Root }

func (d reader) Tables() []types.TableID { return d.r.Tables() }

func (d reader) Rows(t types.TableID, fn func(types.DeltaRowInfo) bool) {
	tb := d.r.Table(t)
	if tb == nil {
		return
	}
	for _, r := range tb.Copy().Rows() {
		if !fn(types.DeltaRowInfo{Table: t, Image: r.Image, Row: r.Row, Origin: r.Origin, SourceKey: r.SourceKey}) {
			return
		}
	}
}

func (d reader) Touched(t types.TableID, fn func(types.TouchedInfo) bool) {
	tb := d.r.Table(t)
	if tb == nil {
		return
	}
	for _, tc := range tb.Copy().TouchedKeys() {
		if !fn(types.TouchedInfo{Table: t, Key: tc.Key, Count: tc.Count}) {
			return
		}
	}
}

// Runner adapts the framework pipeline to session.Runner.
type Runner struct{ Pipe *framework.Pipeline }

// Process runs a pipeline with the current global context.
func (r Runner) Process(name string, payload any) (any, error) { return r.Pipe.Process(name, payload) }
