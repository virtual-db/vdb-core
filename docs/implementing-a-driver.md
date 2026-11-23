# Implementing an engine on the core

An engine makes a database speak to the core. [`vdb-mysql-engine`](https://github.com/virtual-db/vdb-mysql-engine) is
the reference: one `Driver` built from its `Config` and the core's `DriverAPI`.

## What the engine supplies

| Interface | Purpose |
|---|---|
| `Server` | `Run` starts the engine and blocks; `Stop` stops it. Register with `UseDriver`. |
| `SourceProvider` (optional) | `Source()` returns the `Source`: `SourceSnapshot` and `SourceCursor` read the Source inside a consistent snapshot (paged scans, key lookups, existence checks, the count, rows by column values). Detected by `UseDriver`. |
| `MemoryProber` (optional) | `MemoryProbe()` returns the resident memory of every process to count against the memory ceiling. Detected by `UseDriver`. |
| `RowProvider` | Rows of a provided table (VDB for MySQL's `vdb` schema), registered with `Catalog().Provide`. |
| Lock modes | `Locks().Define(LockModes)`: the engine's lock kinds, their compatibility and coverage (InnoDB's record, gap, next-key and insert-intention locks in the MySQL engine). |

## What the engine calls

1. Fill the catalog of record with `Catalog().Load`: tables with their namespace, columns, indexes and identity, plus
   the engine's own definitions as opaque bytes, and objects.
2. Open one `Session` per database connection with `DriverAPI().OpenSession`.
3. Per statement: `StmtBegin` (with the snapshot timing and read view the engine's isolation rules ask for), then
   reads (`Scan`, `Lookup`, `ByColumns`, `Count`) and writes (`Insert`, `Update`, `Delete`, `Truncate`, and `DeleteAll` for a
   DELETE without WHERE), then `StmtEnd`
   or `StmtRollback`; per transaction `Commit` or `Rollback`; `Savepoint`, `RollbackTo`, `Release`; `Snapshot` for a
   consistent snapshot now.
4. Take the locks its rules require with `Locks().Acquire(session, resource, mode, wait)`, passing a `ReadLocks` to a
   read to lock rows as they are returned; release a statement's locks with `ReleaseSince(session.LockMark())`.
5. Send every key with its sort image, every row with every index's image, and every update or delete with the
   `RowRef` the read returned and the whole old row.
6. Stage DDL with `ApplyDDL` (client) or `ApplySourceDDL` (from the Source); convert Delta rows on a layout change
   with `AlterRows` and `AlterPut`.
7. `End` when the connection closes; `DriverAPI().Kill` from any goroutine to end a wait.

Errors are `*core.Error` with one `ErrorCode` each, so the engine maps them with one switch. The core never retries a
Source request inside a snapshot and never holds a lock across one except the snapshot pair's opening.
