# vdb-core

`github.com/virtual-db/vdb-core`

The database-agnostic core of VirtualDB: the Delta (copy-on-write B-trees of rows, touched Source keys and index entries), the merge with the Source, transactions with savepoints and snapshot pairs, row and gap locks with deadlock detection, uniqueness, foreign keys, the catalog of record, change sets for persistence, and the memory limit, plus the pipelines, event bus and plugin host. It never sees SQL: keys are sort images and values are opaque bytes computed by the engine.

An **engine** (VDB for MySQL is `vdb-mysql`) implements `Server` and `Source`, opens a `Session` per database connection through `DriverAPI`, and calls `Run`.

---

## Requirements

- Go 1.26.9 (the version every VDB module uses)
- Module: `github.com/virtual-db/vdb-core`

---

## Quick Start

```go
app := core.New(core.Config{PluginDir: "plugins", MemoryProbe: probe})
app.UseSource(mySourceAdapter)

api := app.DriverAPI()
srv := myengine.New(cfg, api)

app.
    UseDriver(srv).
    Attach("vdb.query.received.intercept", 5, func(ctx any, p any) (any, any, error) {
        // inspect or rewrite queries before they hit the source DB
        return ctx, p, nil
    }).
    Subscribe("vdb.record.inserted", func(ctx any, p any) error {
        // react to inserts after they land in the delta
        return nil
    })

log.Fatal(app.Run())
```

---

## Core Concepts

**Pipeline** — Every database operation runs through a named `vdb.*` pipeline. Each pipeline has an ordered sequence of named points. Handlers attach to a point at a numeric priority; lower numbers run first. Built-in framework handlers reserve priority **10**. A `sync` persistence handler attaches to `vdb.transaction.commit.apply` below 10 (its error fails the commit); a restore handler attaches to `vdb.server.start.configure` below 10 and returns change sets in the payload's `Restore` field.

**Event bus** — After key operations complete the framework emits a named `vdb.*` event. Delivery is fire-and-forget; a subscriber error is logged but does not stop other subscribers.

**Delta store** — Writes (INSERT, UPDATE, DELETE) are recorded in memory without touching the source database. On every read, the delta is overlaid on top of source records. Transactions get a private staging delta (`TxDelta`) that is merged into the live delta on COMMIT and discarded on ROLLBACK.

**Plugin system** — Plugins are out-of-process executables discovered from `PluginDir` at startup. Each communicates over a Unix socket using JSON-RPC 2.0 and declares which pipeline points it handles and which events it subscribes to.

---

## `App` API

| Method | Description |
|---|---|
| `New(cfg Config) *App` | Construct and initialise the framework. |
| `DriverAPI() DriverAPI` | Get the framework's `DriverAPI` bridge. Call before `UseDriver`. |
| `UseDriver(s Server) *App` | Register the engine's server. Must be called before `Run`. |
| `UseSource(s Source) *App` | Register the engine's Source adapter. Must be called before `Run`. |
| `HasHandlers(point string) bool` | Whether anything other than a built-in handler is attached to a point. |
| `Attach(point string, priority int, fn PointFunc) *App` | Register a handler at a pipeline point. Panics on unknown point or after `Run`. |
| `Subscribe(event string, fn EventFunc) *App` | Register an event subscriber. Panics after `Run`. |
| `DeclareEvent(event string)` | Declare a custom event (for extensions and plugins). |
| `DeclarePipeline(name string, points []string)` | Declare a custom pipeline (for extensions and plugins). |
| `Emit(event string, payload any)` | Dispatch a payload to all subscribers of the named event. |
| `Process(pipeline string, payload any) (any, error)` | Run a pipeline manually. |
| `Run() error` | Execute the startup sequence and block. May only be called once. |
| `Stop()` | Graceful shutdown. Idempotent. Unblocks `Run`. |

---

## Interfaces

```go
// Implemented by the driver.
type Server interface {
    Run() error
    Stop() error
}

// Implemented by the engine: the Source the merge reads (docs/implementing-a-driver.md).
type Source interface {
    Snapshot(s SessionInfo, ctx StmtContext) (SourceSnapshot, error)
}

// Implemented by the core. The engine opens one Session per database connection.
type DriverAPI interface {
    OpenSession(info SessionInfo) (Session, error)
    Kill(connID uint64)
    Catalog() Catalog
    ConnectionOpened(connID uint64, account, addr string) error
    ConnectionClosed(connID uint64, account, addr string)
    QueryReceived(connID uint64, query, database string) (string, error)
    QueryCompleted(connID uint64, q QueryInfo)
    DeltaStats() DeltaStats
    LockStats() LockStats
    MemoryStats() MemoryStats
    Delta() DeltaReader
    Transforms() []string
    Emit(event string, payload any)
}

// Session: statements, transactions and savepoints; Scan, Lookup, Count;
// Insert, Update, Delete, Truncate, AutoIncrement; staged DDL; End.
// See types.go and internal/types/api.go for every signature.
```

---

## Pipelines

| Pipeline | Points |
|---|---|
| `vdb.context.create` | `build_context` → `contribute` → `seal` → `emit` |
| `vdb.server.start` | `build_context` → `configure` → `launch` → `emit` |
| `vdb.server.stop` | `build_context` → `drain` → `halt` → `emit` |
| `vdb.connection.opened` | `build_context` → `accept` → `track` → `emit` |
| `vdb.connection.closed` | `build_context` → `cleanup` → `release` → `emit` |
| `vdb.transaction.begin` | `build_context` → `authorize` → `emit` |
| `vdb.transaction.commit` | `build_context` → `apply` → `emit` |
| `vdb.transaction.rollback` | `build_context` → `apply` → `emit` |
| `vdb.query.received` | `build_context` → `intercept` → `emit` |
| `vdb.records.source` | `build_context` → `transform` → `emit` |
| `vdb.records.merged` | `build_context` → `transform` → `emit` |
| `vdb.write.insert` | `build_context` → `apply` → `emit` |
| `vdb.write.update` | `build_context` → `apply` → `emit` |
| `vdb.write.delete` | `build_context` → `apply` → `emit` |
| `vdb.write.truncate` | `build_context` → `apply` → `emit` |

Use `<pipeline>.<point>` as the point name when calling `Attach` (e.g. `vdb.query.received.intercept`).

---

## Events

| Event | Emitted after |
|---|---|
| `vdb.server.stopped` | Graceful shutdown completes |
| `vdb.connection.opened` | A client connection is accepted |
| `vdb.connection.closed` | A client connection is released |
| `vdb.transaction.started` | A transaction begins |
| `vdb.transaction.committed` | A transaction commits |
| `vdb.transaction.rolledback` | A transaction rolls back |
| `vdb.query.completed` | A query finishes execution |
| `vdb.record.inserted` | A record lands in the delta as an insert |
| `vdb.record.updated` | A record overlay lands in the delta |
| `vdb.record.deleted` | A tombstone lands in the delta |
| `vdb.schema.loaded` | (above) A table is added to the catalog of record |
| `vdb.schema.invalidated` | A table is changed by the Source or by client DDL |
| `vdb.table.truncated` | A table is truncated (detached from the Source) |
| `vdb.source.requested` | The engine sent a request to the Source |
| `vdb.limit.reached` | A memory or lock limit refused a write |
| `vdb.snapshot.expired` | A Source snapshot expired |
| `vdb.key.conflict` | A Source row was hidden by an equal Delta key (D-33) |

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Elastic License 2.0 (ELv2). See [LICENSE.md](LICENSE.md).