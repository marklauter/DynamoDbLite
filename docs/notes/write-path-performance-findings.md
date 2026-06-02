---
title: Write-path performance findings
summary: SQLite write-path costs (single + batch) and the levers found — prepared statements win, multi-row VALUES regressed, batch-25 saturates throughput, default-journal is a 166x cliff.
document:
  tags: [performance, write-path, batch, prepared-statements, wal, bench]
  created: 2026-06-02
---

# Write-path performance findings

What the SQLite write path costs (single and batch) and the levers found while optimizing it. Numbers are a snapshot at commit `9ca0b2f`, one machine, median-of-5, ~2 KB items, ~10k-item workloads. Treat the ratios and conclusions as durable; the absolute milliseconds are point-in-time.

## The win: prepare the write statement once per batch

`BatchUpsertItemsAsync` / `BatchDeleteItemsAsync` build one `DbCommand`, `Prepare()` it before the loop, then rebind parameter values and `ExecuteNonQuery` per row — replacing Dapper's `ExecuteAsync(constSql, op, tx)` per item, which recompiles the statement on every call.

At `9ca0b2f`, batch500 came in ~18–41% under the pre-effort baseline across configs (in-memory 113 → 67 ms; WAL ac=1000 265 → 210). Single-write timings stayed flat — the control that proves it's a batch-path change, not the machine warming.

Mechanism (settled after getting it wrong twice):

- A compiled statement lives on the **`DbCommand` object**, not the connection. Microsoft.Data.Sqlite has **no connection-level statement cache** — which is exactly why Dapper's fresh-command-per-call recompiles.
- Preparing once and reusing the command across the loop compiles the SQL a single time. Interleaved `oldJson`/index Dapper calls don't evict it: separate commands hold separate statements, SQLite keeps many prepared statements alive per connection, and the index ops are DML (no `SQLITE_SCHEMA` invalidation).

## Dead end: multi-row `VALUES` upsert

Collapsing N puts into one `INSERT … VALUES (…),(…) ON CONFLICT … DO UPDATE SET … = excluded.*` **regressed** batches 2.5–3× and was reverted (`7e4a9a0`). A unique large statement per chunk defeats reuse and re-pays an expensive compile, and `DynamicParameters` binding of hundreds of named params is heavy. The tell: even a single 25-row statement (no chunking at all) was slower than 25 reused single-row writes. The cost was **recompilation, not round-trips** — which is why prepare-once won and multi-row lost. Don't re-attempt this.

## Batch size: 25 already saturates throughput

Per-item cost barely moves from batch25 to batch500 once the per-call fixed cost (connection + transaction + compile) is amortized — and 25 items amortizes it. At the default WAL checkpoint (ac=1000), batch25 and batch500 are tied (21.4 vs 21.0 µs/op). batch500 only pulls ahead where **commit count** matters:

- **ac=0** (no checkpoints): 10.4 vs 12.9 µs/op — fewer `BEGIN`/`COMMIT` + WAL frames.
- **ac=100** (frequent checkpoints): 22.6 vs 45.5 µs/op (2×) — batch25's 20× more commits trigger far more checkpoint work.
- **ac=1000**: gap collapses — same total bytes written → same checkpoint count regardless of batch size.

So `MaxBatchWriteItems` > 25 buys **fewer commits**, not throughput. It's a niche tuning knob for WAL file stores under an aggressive checkpoint interval (or ac=0), and a no-op under defaults. Keep 25 as the default; treat the override as a specialized tool, not a "go faster" button.

## The default-journal cliff (file stores)

`UseWriteAheadLog` defaults **off**, so a default file store uses rollback-journal (DELETE) mode. Single writes there cost ~2.8 ms/op (~28 s for 10k) — **166× slower than WAL** — because each commit fsyncs both the journal and the db file. WAL + the library's `synchronous=NORMAL` skips the per-commit fsync entirely (it syncs the WAL at checkpoint instead).

Action candidate: default WAL on for file stores, or document loudly that write-heavy file workloads must opt into `WithWriteAheadLog`.

## Open levers

- **Single-write `SELECT`-old gate.** `PutItemCoreAsync` / `DeleteItemCoreAsync` always read the old row before writing (~2 statements/op — the ~90 µs in-memory single floor). That read is only needed for index upkeep or `ReturnValues`. Gating it away for non-indexed + no-`ReturnValues` puts drops it to one statement. `TransactWriteItems` shares this path. See [performance-pass.md](performance-pass.md).
- **Index-maintenance aggregation.** `MaintainIndexesAsync` still runs unprepared Dapper DELETE/UPSERTs per indexed op. Aggregate them out of the core write loop and apply grouped by `idx_<table>_<index>`, with a prepared DELETE and UPSERT per index table reused across the batch — the same prepare-once trick, applied to the index statements. Only helps indexed-table batches, so it needs an indexed-table bench config to measure (the current bench is non-indexed).
- **In-memory is at the provider floor** (~6.7 µs/op batched = bind + interop + single-row B-tree insert). Further gains would mean leaving Microsoft.Data.Sqlite for raw `sqlite3` P/Invoke — not worth it for a test/dev library.

## See also

- [performance-pass.md](performance-pass.md) — raw benchmark sweep, plus the metadata-read and SELECT-old analysis.
- [dynamodblite-write-path-is-slower-than-read-path.md](dynamodblite-write-path-is-slower-than-read-path.md) — read-vs-write asymmetry against dynamodb-local (parser hypothesis).
- [parity-benchmarks-project.md](parity-benchmarks-project.md) — the benchmark harness.
