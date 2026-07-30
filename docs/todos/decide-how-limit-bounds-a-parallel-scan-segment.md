---
title: Decide how Limit bounds a parallel-scan segment
type: todo
summary: "Open question: fix the Limit + TotalSegments divergence by modeling segments at the storage layer, by over-fetching and post-filtering, or by documenting it as a permanent limitation."
tags: [parallel-scan, known-limitation, storage-model, open-question]
created: 2026-07-30
priority: medium
status: open
cites: "[[parallel-scan-limit-interaction-gap]]"
depends-on: "[[parity-benchmarks-project]]"
---

# Decide how Limit bounds a parallel-scan segment

A `ScanRequest` carrying both `TotalSegments` and `Limit` applies `Limit` before the segment filter, so a segment can return fewer items than `Limit` while more of its items remain unscanned. Real DynamoDB bounds `Limit` within the segment. The divergence and its root cause are in [parallel-scan-limit-interaction-gap](../notes/parallel-scan-limit-interaction-gap.md).

Three fixes are open, and none is chosen. One of them changes the storage schema, so settle it before implementing.

## Current state

Unimplemented, verified 2026-07-30. `pk_hash` appears nowhere in `src/` or `tests/`. The only column-migration helper is `EnsureTtlEpochColumn` (`SqliteStore.cs:157`). Both scan paths still filter in C# after the store call: `DynamoDbClient.Scan.cs:47` (`ScanAsync`) and `:147` (`ScanIndexAsync`), with `SegmentOf` at `:312`.

## Candidates

**1. Model segments at the storage layer.** Add `pk_hash INTEGER` to `items` and to every per-GSI/LSI index table, populate at write time from the partition key with the existing FNV-1a function, and push `pk_hash % @totalSegments = @segment` into the WHERE clause so `LIMIT` applies within the segment. Matches real DynamoDB exactly, and the basic parallel-scan case gets faster because SQLite narrows the scan instead of C# discarding rows. Costs a schema migration on `items` and every index table, a one-shot backfill `UPDATE` on first open for legacy file DBs, and one hash per insert per affected table. Store the raw hash and not a `segment_n` value, because `TotalSegments` is supplied per-scan. This is the approach worked out in the note.

**2. Over-fetch, then post-filter.** Ask the store for `Limit × TotalSegments` rows and keep filtering in C#. No schema change, no migration, no write-path cost. It narrows the divergence without closing it: skewed hashing still under-fills a segment, and every segmented scan reads `TotalSegments` times the rows it returns. Not considered in the note.

**3. Document it as a permanent limitation.** `Limit` combined with `TotalSegments` is a narrow combination, and the no-`Limit` parallel-scan case is already correct. Costs nothing and keeps the divergence, which contradicts the drop-in premise in [`decisions/behavioral-fidelity.md`](../decisions/behavioral-fidelity.md).

## What settles it

- A before number for the parallel-scan path from [parity-benchmarks-project](../notes/parity-benchmarks-project.md), so the read win in candidate 1 and the read cost in candidate 2 are measured rather than argued.
- Whether a schema migration is acceptable for this, given it touches every index table.

Add the parity test whichever candidate wins: scan a large table with `TotalSegments=2` and `Limit=5` per segment, and assert each segment returns `Limit` items when more than `Limit` items hash to it.
