# Handlers Reference

Technical reference for writing pipeline handlers and event subscribers in VirtualDB `core`.

---

## `PointFunc` — the handler signature

```go
type PointFunc func(ctx any, payload any) (any, any, error)
```

Every handler attached to a pipeline point must conform to this signature.

| Parameter / return | Description |
|---|---|
| `ctx any` | A `framework.HandlerContext`. Type-assert to access `Global` and `CorrelationID`. |
| `payload any` | The typed payload struct for the pipeline. Varies per pipeline — see `pipelines.md`. |
| First return (`any`) | The `ctx` value to pass to the next handler. Pass it through unchanged unless you have a deliberate reason to replace it. |
| Second return (`any`) | The `payload` value to pass to the next handler. Return the (possibly modified) payload to propagate changes downstream. |
| `error` | Returning a non-nil error aborts all remaining points in the pipeline and propagates the error back to the `DriverAPI` caller. |

---

## `EventFunc` — the subscriber signature

```go
type EventFunc func(ctx any, payload any) error
```

Subscribers registered on the event bus use this signature.

| Parameter / return | Description |
|---|---|
| `ctx any` | A `framework.HandlerContext`. |
| `payload any` | The typed payload struct for the event being delivered. |
| `error` | Returning a non-nil error is logged but does not abort delivery to other subscribers. |

---

## Handler context — `framework.HandlerContext`

Every handler receives a `framework.HandlerContext` as its `ctx any` argument. Type-assert it before use.

```go
import "github.com/virtual-db/core/internal/framework"

func myHandler(ctx any, payload any) (any, any, error) {
    hctx := ctx.(framework.HandlerContext)
    _ = hctx.Global        // framework.GlobalContext
    _ = hctx.CorrelationID // framework.CorrelationID
    return ctx, payload, nil
}
```

### Fields

#### `Global framework.GlobalContext`

The sealed, immutable process-wide key-value store. Populated during the `vdb.context.create` lifecycle and available to all handlers for the entire process lifetime. It cannot be written to after the `seal` point completes.

Methods:

| Method | Description |
|---|---|
| `Get(key string) any` | Returns a value set during `vdb.context.create.contribute`, or `nil` if the key was never set. |
| `Bus() framework.EventBus` | Returns the event bus. Handlers can call `Bus().Emit(...)` to publish custom events. |

#### `CorrelationID framework.CorrelationID`

Tracing identifiers for the current operation. Stamped fresh by the built-in framework handler at priority 10 on every `build_context` point before any domain logic runs.

| Field | Type | Description |
|---|---|---|
| `Root` | `string` | UUID of the originating operation in the causal chain. |
| `Parent` | `string` | UUID of the immediately preceding operation. |
| `ID` | `string` | UUID of the current operation. |

---

## Priority system

Handlers are executed in ascending priority order within a pipeline point. Lower numbers run first.

| Priority range | Intended use |
|---|---|
| 1 – 9 | Pre-processing — runs before built-in framework handlers. |
| 10 | Built-in framework handlers (reserved). |
| 11 – 99 | Post-processing — runs after built-in framework handlers. |

Every pipeline point named `build_context` has a built-in framework handler at priority 10 that stamps a fresh `CorrelationID`. If you attach a handler at a priority lower than 10 on a `build_context` point, it will run before the `CorrelationID` has been set. If you need a valid `CorrelationID`, attach at priority 11 or higher.

---

## `GlobalContext` contributions

The `vdb.context.create.contribute` point is the **only** place where values can be written into the `GlobalContext`. The payload at this point is a `*framework.GlobalContextBuilder`. Once the `vdb.context.create.seal` point completes, the context is immutable for the rest of the process lifetime.

### Writing a value during startup

```go
app.Attach("vdb.context.create.contribute", 5, func(ctx any, payload any) (any, any, error) {
    b := payload.(*framework.GlobalContextBuilder)
    b.Set("my-service-config", loadConfig())
    return ctx, payload, nil
})
```

- The key is an arbitrary string. Use a name that is unlikely to collide with other contributors.
- The value can be any type. Use a concrete pointer type for safe type-assertion on the read side.

### Reading a contributed value in any subsequent handler

```go
app.Attach("vdb.query.received.intercept", 20, func(ctx any, payload any) (any, any, error) {
    hctx := ctx.(framework.HandlerContext)
    cfg := hctx.Global.Get("my-service-config").(*MyConfig)
    if cfg == nil {
        return ctx, payload, nil // key was not contributed; handle gracefully
    }
    // use cfg ...
    return ctx, payload, nil
})
```

`Get` returns `nil` if the key was never set. Always guard against nil before type-asserting when the contribution is optional or provided by another component.

---

## Common handler patterns

The payload is a struct of `internal/payloads` (fields in [pipelines.md](pipelines.md)). Code outside `vdb-core` cannot
name those types, so it reads the payload by field name and may return it as a `map[string]any` with the same field
names; the core decodes either form, as it does for out-of-process plugins. The helper below is used in the examples:

```go
// fields returns the payload's fields by name.
func fields(payload any) map[string]any {
    b, _ := json.Marshal(payload)
    m := map[string]any{}
    _ = json.Unmarshal(b, &m)
    return m
}
```

### 1. Query rewriting

Attach to `vdb.query.received.intercept` to rewrite a statement before the engine runs it.

```go
app.Attach("vdb.query.received.intercept", 5, func(ctx any, payload any) (any, any, error) {
    p := fields(payload)
    p["Query"] = strings.ReplaceAll(p["Query"].(string), "legacy_orders", "orders")
    return ctx, p, nil
})
```

### 2. Blocking a transaction

Attach to `vdb.transaction.begin.authorize` and return an error to refuse the transaction.

```go
app.Attach("vdb.transaction.begin.authorize", 5, func(ctx any, payload any) (any, any, error) {
    hctx := ctx.(framework.HandlerContext)
    if policy, _ := hctx.Global.Get("tx-policy").(*TxPolicy); policy != nil && policy.ReadOnly {
        return ctx, payload, errors.New("transactions are disabled in read-only mode")
    }
    return ctx, payload, nil
})
```

The error is returned to the engine and the transaction is not opened.

### 3. Persisting committed changes

Attach to `vdb.transaction.commit.apply` below 10. The payload's `Change` is the transaction's change set; an error
fails the commit.

```go
app.Attach("vdb.transaction.commit.apply", 5, func(ctx any, payload any) (any, any, error) {
    if err := store.Append(fields(payload)["Change"]); err != nil {
        return ctx, payload, err
    }
    return ctx, payload, nil
})
```

### 4. Emitting a custom event from a handler

```go
app.Attach("vdb.write.insert.emit", 50, func(ctx any, payload any) (any, any, error) {
    hctx := ctx.(framework.HandlerContext)
    hctx.Global.Bus().Emit("my-app.rows.inserted", fields(payload)["Table"])
    return ctx, payload, nil
})
```

Subscribers of `my-app.rows.inserted` run before `Emit` returns; their errors are logged and do not affect the
handler.

---

## Error handling behaviour summary

| Pipeline | Handler error behaviour |
|---|---|
| `vdb.connection.opened.*` | The connection is refused; the error goes to the engine |
| `vdb.query.received.*` | The statement fails with the error |
| `vdb.transaction.begin.*` | The transaction is not opened |
| `vdb.transaction.commit.*` | The commit fails |
| `vdb.write.*` | The write fails |
| `vdb.records.source.*`, `vdb.records.merged.*` | The read fails |
| `vdb.transaction.rollback.*` | The rollback still happens; the error is returned to the engine |
| `vdb.connection.closed.*` | Ignored; the connection is released |
| All event subscribers | Logged; the other subscribers of the event still run |
