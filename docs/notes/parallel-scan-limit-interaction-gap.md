# Parallel scan + Limit interaction gap

Tags: release, v1.1, known-limitation, parallel-scan
ScanRequest with both TotalSegments and Limit applies Limit before the segment filter; real DynamoDB applies Limit within the segment.


## Observation

The parallel-scan fix in commit `0e435bd` post-filters rows by segment hash AFTER the SQLite store applies `Limit`. With `Limit=10` and `TotalSegments=2`, the store returns up to 10 rows; the segment filter then keeps only the ~half hashing to the requested segment. Result: a single segment can return fewer items than `Limit` even when more items in that segment exist beyond what was sampled.

Real DynamoDB partitions items by hash *before* applying `Limit`, so `Limit` bounds the result count within the segment, not the pre-partition pool.

No parity test exercises both knobs simultaneously — the gap is unmonitored.

## Interpretation

The fix shipped because the parallel-scan parity test (no Limit) needed it and the contract for the no-Limit case is correct. For consumers who use `Limit` + `TotalSegments` together, behavior diverges from real DynamoDB silently. Visibility is poor: tests pass, code looks reasonable, only users with the specific combination see the wrong count.

The root cause is that **DynamoDbLite does not model segments at the storage layer**. The hash is computed at scan time as a stateless C# function over the partition key (`SegmentOf` in `DynamoDbClient.Query.cs`) and applied as a post-filter on rows the store has already truncated to `Limit`. By the time the filter runs, the rows we'd want for the requested segment may already be gone.

Real DynamoDB models segments at the partition level: items live in physical partitions, parallel scan opens N readers, and `Limit` bounds the count per reader.

## Proposed fix

Model segmentation by adding a stable per-row partition hash column to the storage schema and pushing the segment filter into SQLite. Outline:

**Schema change.** Add `pk_hash INTEGER` to `items` and to every per-GSI/LSI index table. Populate at write time from the partition key using the existing FNV-1a function (the same one currently in `DynamoDbClient.Query.cs`). Backfill via a one-shot UPDATE on first open after upgrade — the same `EnsureXColumn` migration pattern already used for `ttl_epoch`.

**Scan path.** When `TotalSegments` is set, push the modulus into the WHERE clause:

```sql
SELECT pk, sk, item_json, sk_num, ttl_epoch
FROM items
WHERE table_name = @table
  AND (@totalSegments IS NULL OR (pk_hash % @totalSegments) = @segment)
  AND (ttl_epoch IS NULL OR ttl_epoch >= @nowEpoch)
ORDER BY pk, sk
LIMIT @limit;
```

`LIMIT` now applies *within* the segment, matching real DynamoDB. Same shape for the index-table scan path.

**Hash storage choice.** Store the raw hash, not a `segment_n` value, because `TotalSegments` is supplied per-scan, not at write time — only the raw hash is reusable across different `TotalSegments` values. INTEGER (rather than REAL) keeps `%` arithmetic exact in SQLite.

**Indexing.** A composite index `(table_name, pk_hash)` would let SQLite seek directly to the segment's rows. Whether to add it depends on bench numbers — defer until [parity-benchmarks-project](parity-benchmarks-project.md) gives us a "before" measurement.

## Trade-offs

- **Schema migration:** new column on `items` and on every per-GSI/LSI index table; new `EnsureSkHashColumn`-style helper for legacy file DBs (mirrors `EnsureTtlEpochColumn`).
- **Write cost:** one hash per insert per affected table — negligible compared to the parser cost we know dominates the write path (see [dynamodblite-write-path-is-slower-than-read-path](dynamodblite-write-path-is-slower-than-read-path.md)).
- **Read win:** parallel scan becomes correct under `Limit`; the basic parallel-scan case also gets faster (SQL-level filter narrows the SQLite scan instead of the post-filter scanning the full table per segment).
- **Splash area:** schema, write path, scan path (table + index), and migration helper. Larger than a typical fix; touches the storage contract.
- **Backfill cost on legacy file DBs:** one-shot UPDATE on first open after upgrade. For a multi-million-row table this is a noticeable startup pause — but it's a one-time cost and DynamoDbLite isn't aimed at multi-million-row scenarios anyway.

## Next

- Add a parity test: scan a large table with `TotalSegments=2`, `Limit=5` on each segment. Assert each segment returns `Limit` items when more than `Limit` items hash to it, and that `LastEvaluatedKey` is set correctly.
- Sequence the schema change after [parity-benchmarks-project](parity-benchmarks-project.md) so we have a "before" number for the parallel-scan path.
- Implement: schema column + write-path hash + scan-path WHERE clause + `EnsureSkHashColumn` helper. `DynamoDbClient.Query.cs` (`ScanAsync` and `ScanIndexAsync`); `SqliteStore.cs` (schema bootstrap + write path); per-table index DDL.
- Cross-link from [`parity-with-dynamodb-local.md`](parity-with-dynamodb-local.md) (under a "Known limitations" subsection or in the Covered entry for `ScanParityTests`) until fixed; remove from CHANGELOG known-limitations when closed.

Listed under [v1-0-changelog-and-release-notes](v1-0-changelog-and-release-notes.md) "Known limitations" in the meantime.
