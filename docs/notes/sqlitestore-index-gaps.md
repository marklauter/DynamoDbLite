# SqliteStore index gaps

Tags: performance, read-path, sqlite, indexes, bench-needed
Three missing indexes in SqliteStore: items.ttl_epoch, items.sk_num, per-GSI/LSI table sk_num.


## Observation

Schema definitions in `src/DynamoDbLite/SqliteStores/SqliteStore.cs` carry primary keys but no auxiliary indexes:

- `items` table (lines 56–64): `PRIMARY KEY (table_name, pk, sk)`. No index on `ttl_epoch`. No index on `sk_num`.
- Per-GSI/LSI index tables (created in `CreateIndexTableAsync`, around line 821): `PRIMARY KEY (pk, sk, table_pk, table_sk)`. Same `sk_num` gap.

## Interpretation

- **TTL sweeps full-scan the items table.** The background TTL cleanup filters `WHERE ttl_epoch < ?`. Without an index on `ttl_epoch`, every row gets scanned per sweep — cost grows linearly with table size.
- **Numeric-sort-key range queries don't use an index for the range.** `Query` / `Scan` with `BETWEEN` or `<=` on a numeric sort key hits the PK for the partition lookup, then linearly scans that partition filtering on `sk_num`. A partial index `(table_name, pk, sk_num) WHERE sk_num IS NOT NULL` would convert the filter to an index range scan.
- **Same gap on index tables.** GSI/LSI numeric sort-key range queries have the same shape and the same problem.

These are read-path optimizations, separate from the suspected write-side parser bottleneck. See [dynamodblite-write-path-is-slower-than-read-path](dynamodblite-write-path-is-slower-than-read-path.md) for that thread.

## Next

Validate before adding indexes. Every auxiliary index has a real write-side cost — each `Put` updates each index — so the bench numbers need to show the read win outweighs the write cost. The parity benchmarks project (see `docs/parity.md`, "Parity benchmarks" under Next) should include:

- TTL-heavy workload (large table with TTL-expiring rows, repeated sweep cost).
- Numeric-sort-key range scans against varying partition sizes.
- GSI numeric range queries.

Add indexes only when measurements confirm the win.
