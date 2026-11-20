package core

import (
	"strings"
	"sync"

	"github.com/virtual-db/vdb-core/internal/catalog"
	"github.com/virtual-db/vdb-core/internal/changelog"
	"github.com/virtual-db/vdb-core/internal/delta"
	"github.com/virtual-db/vdb-core/internal/driverapi"
	"github.com/virtual-db/vdb-core/internal/emit"
	"github.com/virtual-db/vdb-core/internal/framework"
	"github.com/virtual-db/vdb-core/internal/lifecycle"
	"github.com/virtual-db/vdb-core/internal/lock"
	"github.com/virtual-db/vdb-core/internal/memory"
	"github.com/virtual-db/vdb-core/internal/merge"
	"github.com/virtual-db/vdb-core/internal/payloads"
	"github.com/virtual-db/vdb-core/internal/plugin"
	"github.com/virtual-db/vdb-core/internal/points"
	"github.com/virtual-db/vdb-core/internal/session"
	"github.com/virtual-db/vdb-core/internal/txn"
	"github.com/virtual-db/vdb-core/internal/types"
	"github.com/virtual-db/vdb-core/internal/write"
)

// Compile-time check: driverapi.Impl must satisfy the root DriverAPI interface.
var _ DriverAPI = (*driverapi.Impl)(nil)

// lifecycleAppAdapter implements lifecycle.App on behalf of *App, keeping the
// lifecycle-only methods off the public *App surface.
type lifecycleAppAdapter struct {
	app *App
}

func (a lifecycleAppAdapter) Bus() *framework.Bus                 { return a.app.bus }
func (a lifecycleAppAdapter) Pipe() *framework.Pipeline           { return a.app.pipe }
func (a lifecycleAppAdapter) SetGlobal(g framework.GlobalContext) { a.app.global = g }
func (a lifecycleAppAdapter) GetServer() lifecycle.Server         { return a.app.server }
func (a lifecycleAppAdapter) ServerErrCh() chan<- error           { return a.app.serverErrCh }
func (a lifecycleAppAdapter) Plugins() *plugin.Manager            { return a.app.plugins }
func (a lifecycleAppAdapter) Restore(cs *types.ChangeSet) error   { return a.app.txn.Replay(cs) }

// pipes adapts *App to driverapi.Pipelines.
type pipes struct{ app *App }

func (p pipes) Process(name string, payload any) (any, error) {
	return p.app.pipe.Process(name, payload)
}
func (p pipes) HasHandlers(point string) bool  { return p.app.pipe.HasHandlers(point) }
func (p pipes) Emit(event string, payload any) { p.app.bus.Emit(event, payload) }
func (p pipes) Transforms() []string           { return p.app.plugins.Transforms() }

// App is the central framework object. It holds the pipeline registry, the
// event bus, the global context, the engine's server and Source, and every
// core component for the lifetime of the process.
type App struct {
	cfg    Config
	pipe   *framework.Pipeline
	bus    *framework.Bus
	global framework.GlobalContext

	catalog  *catalog.Catalog
	store    *delta.Store
	locks    *lock.Manager
	txn      *txn.Manager
	seq      *changelog.Sequence
	mem      *memory.Guard
	merge    *merge.Merge
	writer   *write.Writer
	sessions *session.Registry
	clients  *session.Clients

	plugins *plugin.Manager
	server  Server
	source  Source
	apiImpl DriverAPI

	shutdown    chan struct{}
	serverErrCh chan error
	running     bool
	stopped     bool
	mu          sync.Mutex
}

// New builds, in order: the pipeline and bus, the catalog, the Delta store,
// the memory guard, the lock manager, the transaction manager, the change
// log, the merge and the writer, then the DriverAPI; then declares pipelines
// and events and registers the built-in handler groups.
func New(cfg Config) *App {
	app := &App{cfg: cfg}

	app.pipe = framework.NewPipeline(&app.global)
	app.bus = framework.NewBus(&app.global)

	app.catalog = catalog.New()
	app.store = delta.NewStore()
	app.mem = memory.New(memory.Config{
		Reserve: cfg.MemoryReserve, DeltaMaxBytes: cfg.DeltaMaxBytes,
		DeltaBytes: app.store.Bytes,
		OnRefusal: func(kind string, limit uint64) {
			app.bus.Emit(points.EventLimitReached, payloads.LimitReachedPayload{Limit: kind, Value: limit})
		},
	})
	app.locks = lock.New(app.mem)
	app.seq = &changelog.Sequence{}
	app.txn = txn.New(app.store, app.locks, app.catalog, app.seq)
	app.merge = merge.New(merge.Config{CountDeclineKeys: cfg.CountDeclineKeys, KeyBatch: cfg.LookupBatch}, merge.Hooks{})
	app.writer = write.New(app.mem, app.merge)
	app.clients = session.NewClients()
	app.sessions = session.NewRegistry(session.Deps{Txn: app.txn, Merge: app.merge, Write: app.writer, Runner: driverapi.Runner{Pipe: app.pipe}, DefaultLockWait: cfg.DefaultLockWait})

	app.wire()
	app.apiImpl = driverapi.New(pipes{app}, app.sessions, app.txn, app.catalog, app.mem)

	declarePipelines(app.pipe)
	declareEvents(app.bus)

	mustRegisterBuiltinHandlers(app)

	app.plugins = plugin.NewManager(cfg.PluginDir, 0)
	app.shutdown = make(chan struct{})
	app.serverErrCh = make(chan error, 1)

	return app
}

// wire connects the components' hooks to pipelines and events.
func (a *App) wire() {
	a.txn.SetCommit(func(tx *txn.Tx, cs *types.ChangeSet) error {
		_, err := a.pipe.Process(points.PipelineTransactionCommit,
			payloads.TransactionCommitPayload{ConnectionID: tx.Info.ConnID, Change: cs})
		return err
	})
	a.catalog.SetHooks(catalog.Hooks{
		Loaded: func(t types.TableDef) {
			a.bus.Emit(points.EventSchemaLoaded, schemaPayload(t))
		},
		Invalidated: func(t types.TableDef) {
			a.bus.Emit(points.EventSchemaInvalidated, schemaPayload(t))
		},
		Record: func(cs *types.ChangeSet) error {
			cs.Seq = a.seq.Next()
			cs.V = changelog.FormatVersion
			_, err := a.pipe.Process(points.PipelineTransactionCommit, payloads.TransactionCommitPayload{Change: cs})
			return err
		},
	})
	a.merge.SetHooks(merge.Hooks{
		Source: a.recordsHook(points.PipelineRecordsSource, points.PointRecordsSourceTransform, points.PointRecordsSourceEmit),
		Merged: a.recordsHook(points.PipelineRecordsMerged, points.PointRecordsMergedTransform, points.PointRecordsMergedEmit),
		KeyConflict: func(t types.TableID, img types.KeyImage) {
			name := ""
			if d, ok := a.catalog.Table(t); ok {
				name = d.QualifiedName()
			}
			a.bus.Emit(points.EventKeyConflict, payloads.KeyConflictPayload{Table: name, TableID: t, KeyImage: img})
		},
	})
}

// recordsHook runs a records pipeline on a batch, only when something other
// than the built-in handlers is attached to it.
func (a *App) recordsHook(pipeline, transform, emitPoint string) merge.RecordsFunc {
	return func(tx *txn.Tx, d *types.TableDef, cols types.ColumnSet, rows []types.Row) ([]types.Row, error) {
		if !a.pipe.HasHandlers(transform) && !a.pipe.HasHandlers(emitPoint) {
			return rows, nil
		}
		names := make([]string, len(cols))
		for i, c := range cols {
			if int(c) < len(d.Columns) {
				names[i] = d.Columns[c].Name
			}
		}
		out, err := a.pipe.Process(pipeline, payloads.RecordsPayload{
			ConnectionID: tx.Info.ConnID, Table: d.QualifiedName(), Columns: names, Rows: rows})
		if err != nil {
			return nil, err
		}
		p, err := payloads.Decode[payloads.RecordsPayload](out)
		if err != nil {
			return nil, err
		}
		return p.Rows, nil
	}
}

func schemaPayload(t types.TableDef) payloads.SchemaPayload {
	cols := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		cols[i] = c.Name
	}
	return payloads.SchemaPayload{Database: strings.Join(t.Namespace, "."), Table: t.Name, TableID: t.ID, Kind: t.Kind.String(), Version: t.Version, Columns: cols}
}

// mustRegisterBuiltinHandlers registers all built-in framework handlers.
// No anonymous functions: all logic lives in named handler struct methods.
func mustRegisterBuiltinHandlers(app *App) {
	groups := []interface {
		Register(framework.Registrar) error
	}{
		lifecycle.New(lifecycleAppAdapter{app}),
		app.clients,
		&txn.Handlers{M: app.txn},
		&write.Handlers{W: app.writer, TxFor: app.sessions.Tx},
		merge.Handlers{},
		emit.New(),
	}
	for _, g := range groups {
		if err := g.Register(app.pipe.Builtin()); err != nil {
			panic("core: handler registration: " + err.Error())
		}
	}
}
