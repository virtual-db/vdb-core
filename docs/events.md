# Event Reference

VirtualDB core emits structured events over an internal event bus backed by [`github.com/AnqorDX/dispatch`](https://github.com/AnqorDX/dispatch).

## Fundamentals

- Events are **fire-and-forget**. A subscriber error is logged but does not stop other subscribers and does not affect the emitting caller.
- Subscribe to an event with `app.Subscribe(name, fn EventFunc)`.
- If the event name has not been declared, `Subscribe` logs a warning and drops the subscription silently.
- Declare a new event with `app.DeclareEvent(name)`.
- Emit a declared event with `app.Emit(name, payload)`.

The `EventFunc` signature:

```go
type EventFunc func(ctx any, payload any) error
```

Handlers receive `payload any` and must type-assert it to the expected payload type for the event.

---

## Standard Events

Plugins are separate programs in any language: they receive each payload as a JSON object and return it, changed
or not, as JSON with the same field names. Identifiers are integers. A row is an array of column values in the order
`Columns` names; each value is a JSON string when its bytes are valid UTF-8, `null` for SQL `NULL`, and
`{"base64": "..."}` otherwise. Inside the core the payloads are the structs of `internal/payloads`.

| Event | Emitted | Payload |
|---|---|---|
| `vdb.server.stopped` | After `Server.Stop` returns and every plugin has exited | `ServerStopPayload`: `Reason` |
| `vdb.connection.opened` | After a client connection is accepted and tracked | `ConnectionOpenedPayload`: `ConnectionID`, `User`, `Address` |
| `vdb.connection.closed` | After a client connection is released | `ConnectionClosedPayload`: `ConnectionID`, `User`, `Address` |
| `vdb.transaction.started` | After a transaction opens | `TransactionBeginPayload`: `ConnectionID`, `ReadOnly` |
| `vdb.transaction.committed` | After a commit is published | `TransactionCommitPayload`: `ConnectionID`, `Change` (the transaction's `*core.ChangeSet`) |
| `vdb.transaction.rolledback` | After a rollback | `TransactionRollbackPayload`: `ConnectionID`, `Savepoint` (empty for a full rollback) |
| `vdb.query.completed` | When the engine reports a finished statement | `QueryCompletedPayload`: `ConnectionID`, `Query`, `Digest`, `Database`, `Account`, `DurationMs`, `RowsAffected`, `RowsSent`, `ErrorCode` (0 on success) |
| `vdb.record.inserted` | After rows land in the Delta through `vdb.write.insert` | `WriteInsertPayload`: `ConnectionID`, `Table`, `TableID`, `Columns`, `Rows`, `Images`, `Results` (one per row) |
| `vdb.record.updated` | After an update lands through `vdb.write.update` | `WriteUpdatePayload`: `ConnectionID`, `Table`, `TableID`, `Columns`, `Old` (`core.RowRef`), `New`, `Images` |
| `vdb.record.deleted` | After a delete lands through `vdb.write.delete` | `WriteDeletePayload`: `ConnectionID`, `Table`, `TableID`, `Old` (`core.RowRef`) |
| `vdb.table.truncated` | After a truncate, or a DELETE without WHERE (`Delete` set), through `vdb.write.truncate` (the table is detached from the Source) | `WriteTruncatePayload`: `ConnectionID`, `Table`, `TableID`, `Delete`, `Rows` |
| `vdb.schema.loaded` | When a table is added to the catalog of record | `SchemaPayload`: `Database`, `Table`, `TableID`, `Kind`, `Version`, `Columns` |
| `vdb.schema.invalidated` | When a table is changed by the Source or by client DDL | `SchemaPayload`, as above |
| `vdb.limit.reached` | When the memory or lock limit refuses work | `LimitReachedPayload`: `Limit`, `Value`, `ConnectionID` |
| `vdb.key.conflict` | When a Source row is hidden by an equal Delta key | `KeyConflictPayload`: `Table`, `TableID`, `KeyImage` |
| `vdb.source.requested` | Emitted by the engine after each Source request | `SourceRequestedPayload`: `ConnectionID`, `Digest`, `Shape`, `Table`, `DurationUs`, `Rows`, `Bytes`, `Error` |
| `vdb.snapshot.expired` | Emitted by the engine when a Source snapshot expires | `SnapshotExpiredPayload`: `ConnectionID`, `AgeMs` |

The last two are declared by the core and emitted by the engine through `DriverAPI.Emit`, as any engine may.

---

## Custom Events

### Declaring and emitting from a product or plugin

Declare the event before any subscriber or emitter can use it. Declaration is typically done during plugin or product initialisation.

```go
// Declare the event once, e.g. in plugin initialisation.
app.DeclareEvent("myplugin.cache.evicted")

// Emit the event from anywhere after declaration.
app.Emit("myplugin.cache.evicted", MyCacheEvictedPayload{
    Table:    "orders",
    KeyCount: 42,
})
```

Subscribe from any other component:

```go
app.Subscribe("myplugin.cache.evicted", func(ctx any, payload any) error {
    p := payload.(MyCacheEvictedPayload)
    log.Printf("cache evicted %d keys from table %s", p.KeyCount, p.Table)
    return nil
})
```

There is no requirement that custom event names begin with `vdb.`.

### Plugin declaration via JSON-RPC

Plugins that run as out-of-process JSON-RPC servers must declare any events they intend to emit inside their `declare` notification payload, under the `"events"` key. The framework registers those names at plugin load time so that subsequent `emit_event` calls are accepted.

```jsonc
// declare notification payload (sent by the plugin at startup)
{
    "events": [
        "myplugin.cache.evicted",
        "myplugin.sync.completed"
    ]
}

// emit_event JSON-RPC method call (sent by the plugin at runtime)
{
    "method": "emit_event",
    "params": {
        "name":    "myplugin.cache.evicted",
        "payload": { "Table": "orders", "KeyCount": 42 }
    }
}
```

If a plugin calls `emit_event` for a name it did not declare in its `declare` notification, the framework logs a warning and drops the emission.