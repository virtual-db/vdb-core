# Implementing an engine on the core

An engine makes a database speak to the core. VDB for MySQL (`vdb-mysql`) is the reference.

## What the engine supplies

| Interface | Purpose |
|---|---|
| `Server` | `Run` starts the engine and blocks; `Stop` stops it. Register with `UseDriver`. |
| `Source`, `SourceSnapshot`, `SourceCursor` | Reads of the Source inside a consistent snapshot: paged scans, key lookups, existence checks, the count, and rows by column values. Register with `UseSource`. |
| `MemoryProbe` | Resident memory of every process to count, in `Config`. |
| `RowProvider` | Rows of a provided table (VDB for MySQL's `vdb` schema), registered with `Catalog().Provide`. |
| `ImageFunc`, `WaitFunc` | Per call, through `Session.SetCallbacks`: sort images for rows a cascade writes (D-86), and the holder of a lock before a wait. |

## What the engine calls

1. Fill the catalog of record with `Catalog().Load` (tables with their indexes, identity, foreign keys and which
   collations are proven) and objects.
2. Open one `Session` per database connection with `DriverAPI().OpenSession`.
3. Per statement: `StmtBegin`, then reads (`Scan`, `Lookup`, `Count`) and writes (`Insert`, `Update`, `Delete`,
   `Truncate`, `AutoIncrement`), then `StmtEnd` or `StmtRollback`; per transaction `Commit` or `Rollback`; `Savepoint`,
   `RollbackTo`, `Release`; `Snapshot` for a consistent snapshot now.
4. Send every key with its sort image, every row with every index's image, and every update or delete with the
   `RowRef` the read returned and the whole old row.
5. Stage DDL with `ApplyDDL` (client) or `ApplySourceDDL` (from the Source); convert Delta rows on a layout change
   with `AlterRows` and `AlterPut`.
6. `End` when the connection closes; `Kill` from any goroutine to end a wait.

Errors are `*core.Error` with one `ErrorCode` each, so the engine maps them with one switch. The core never retries a
Source request inside a snapshot and never holds a lock across one except the snapshot pair's opening.
