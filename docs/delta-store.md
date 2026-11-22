# The Delta

The Delta is everything an environment changed, kept in memory by `internal/delta` (implementation plan doc 04 §1).

## Per table

| Tree | Ordered by | Holds |
|---|---|---|
| rows | identity image | Delta rows: every column, origin (Source or Delta), the Source key it replaced, the image of every index |
| touched | Source key bytes | Source rows updated or deleted; for tables without a usable key, a count of identical rows gone (D-10) |
| touched by image | identity image | The same, when the engine supplied the image |
| index *i* | (index image, identity image) | One entry per Delta row per index |

Images are the engine's sort images (MySQL's `Field::make_sort_key`); the core compares them as bytes and never
builds one. Range bounds compare as prefixes, so a bound on fewer key parts covers every longer key.

## Snapshots and transactions

Every tree is a `tidwall/btree` B-tree whose `Copy()` is copy-on-write. The committed state is a `Root` of trees that
are never mutated after publication. A transaction writes into a `Draft` forked from its snapshot root and records
each change as an op; a commit re-applies the ops to the current trees under the commit lock, so commits of other
rows in between are kept. Savepoints are op counts: restoring replays the earlier ops over the base. A cursor works on
its own copy of the trees, so it never sees writes made after it opened.

## The merge rule

visible(T) = Source rows whose key is neither touched nor held by a Delta row, plus the Delta rows. Unordered scans
filter Source pages by touched key bytes; ordered scans give the engine two cursors to interleave; lookups answer from
the Delta first; `COUNT(*)` is the Source count minus touched keys still on the Source plus the Delta's rows.

## Memory

Each tree entry is accounted in its table's state; the memory guard decides with the resident memory of every
process, keeping `MemoryReserve` free, and refuses a growing write with `ErrDeltaFull`, which the engine maps to its own error (1114 in the MySQL engine).
