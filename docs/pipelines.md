# Pipeline Reference

VirtualDB's pipeline engine uses [`github.com/AnqorDX/pipeline`](https://github.com/AnqorDX/pipeline) internally.

## Concepts

### Pipelines and Points

A **pipeline** is a named, ordered sequence of **points**. Each point is a named slot within the pipeline. Pipelines are identified by dot-separated names (e.g. `vdb.query.received`).

### Handlers

A **handler** (`PointFunc`) attaches to a point at a numeric **priority**. When a pipeline runs, all handlers registered at each point execute in ascending priority order before the pipeline advances to the next point.

Multiple handlers may share the same point and priority; within a shared priority their execution order is not guaranteed.

### Attaching Handlers

```go
app.Attach("vdb.query.received.intercept", 20, func(ctx any, payload any) (any, any, error) {
    // payload is the QueryReceivedPayload described below; return it (or a
    // changed copy) as the second value.
    return ctx, payload, nil
})
```

The full point name passed to `app.Attach` is `<pipeline>.<point>` — e.g. `vdb.query.received.intercept`.

### Priority Conventions

| Priority range | Use |
|---|---|
| 1–9 | Run **before** the built-in framework handler |
| **10** | Reserved for built-in framework handlers |
| 11–99 | Run **after** the built-in framework handler |

### Error Behaviour

A handler returning a non-nil error **aborts all remaining points** in that pipeline and propagates the error back to the `DriverAPI` caller. The only exceptions are pipelines where the documentation below explicitly states that errors are logged and execution continues.

### `build_context` Points

Every pipeline includes a `build_context` point as its first point. The framework registers a handler at priority 10 on every `build_context` point. This handler stamps a fresh `CorrelationID` onto the `HandlerContext` before any domain logic runs.

### Payloads

Each pipeline carries one payload; a handler returns the payload the next one receives. Plugins are separate programs in any language: they receive each payload as a JSON object and return it, changed
or not, as JSON with the same field names. Identifiers are integers. A row is an array of column values in the order
`Columns` names; each value is a JSON string when its bytes are valid UTF-8, `null` for SQL `NULL`, and
`{"base64": "..."}` otherwise. Inside the core the payloads are the structs of `internal/payloads`. The core never
interprets row values.

---

## Pipelines

The built-in handler of each point runs at priority 10. Points not listed have none. Every `build_context` point has
the built-in handler that stamps a fresh `CorrelationID`.

| Pipeline | Run by | Points and built-in handlers | Payload |
|---|---|---|---|
| `vdb.context.create` *(host only)* | `Run`, before any plugin starts | `contribute`: host code adds values to the global context; `seal` (10): the context is sealed | `*framework.GlobalContextBuilder` |
| `vdb.server.start` | `Run`, after every plugin is connected | `configure` (10): replays the change sets a persistence plugin returned in `Restore` (attach below 10 to return them); `launch` (10): `Server.Run` in a goroutine | `ServerStartPayload`: `Restore []core.ChangeSet` |
| `vdb.server.stop` *(host only)* | `Stop` | `drain` (10): plugins are sent `shutdown` and awaited; `halt` (10): `Server.Stop`; `emit` (10): `vdb.server.stopped` | `ServerStopPayload`: `Reason` |
| `vdb.connection.opened` | `DriverAPI.ConnectionOpened` | `accept`: an error refuses the connection; `track` (10): the connection is registered; `emit` (10) | `ConnectionOpenedPayload`: `ConnectionID`, `User`, `Address` |
| `vdb.connection.closed` | `DriverAPI.ConnectionClosed` | `cleanup`; `release` (10): the connection is removed; `emit` (10) | `ConnectionClosedPayload`: `ConnectionID`, `User`, `Address` |
| `vdb.transaction.begin` | A session's first statement of a transaction | `authorize`: an error refuses the transaction; `emit` (10) | `TransactionBeginPayload`: `ConnectionID`, `ReadOnly` |
| `vdb.transaction.commit` | A commit, and every change set the core records | `apply` (10): publishes the transaction; a persistence plugin's `sync` handler attaches below 10, and its error fails the commit; `emit` (10) | `TransactionCommitPayload`: `ConnectionID`, `Change *core.ChangeSet` |
| `vdb.transaction.rollback` | `Rollback` and `RollbackTo` | `apply`; `emit` (10) | `TransactionRollbackPayload`: `ConnectionID`, `Savepoint` (empty for a full rollback) |
| `vdb.query.received` | `DriverAPI.QueryReceived` | `intercept`: handlers may rewrite `Query`; `emit` | `QueryReceivedPayload`: `ConnectionID`, `Query`, `Database` |
| `vdb.records.source` | Each batch of Source rows a read returns, only when a handler other than a built-in is attached | `transform`: handlers may change `Rows`; `emit` | `RecordsPayload`: `ConnectionID`, `Table`, `Columns`, `Rows` |
| `vdb.records.merged` | Each batch of merged rows, under the same condition | `transform`; `emit` | `RecordsPayload`, as above |
| `vdb.write.insert` | `Session.Insert` | `apply` (10): rows go into the Delta and `Results` is filled; `emit` (10): `vdb.record.inserted` | `WriteInsertPayload`: `ConnectionID`, `Table`, `TableID`, `Columns`, `Rows`, `Images`, `Results` |
| `vdb.write.update` | `Session.Update` | `apply` (10); `emit` (10): `vdb.record.updated` | `WriteUpdatePayload`: `ConnectionID`, `Table`, `TableID`, `Columns`, `Old core.RowRef`, `New`, `Images` |
| `vdb.write.delete` | `Session.Delete` | `apply` (10); `emit` (10): `vdb.record.deleted` | `WriteDeletePayload`: `ConnectionID`, `Table`, `TableID`, `Old core.RowRef` |
| `vdb.write.truncate` | `Session.Truncate`, `Session.DeleteAll` | `apply` (10): the table is detached from the Source and its Delta emptied (for `DeleteAll`, a DELETE without WHERE, only as a Delta change that rollback undoes, with no catalog change); `emit` (10): `vdb.table.truncated` | `WriteTruncatePayload`: `ConnectionID`, `Table`, `TableID`, `Delete`, `Rows` |

Host-only pipelines run when no plugin can be connected yet (`vdb.context.create`) or as part of the plugins'
own shutdown (`vdb.server.stop`); plugins cannot attach to them. Every other pipeline accepts plugin handlers.
