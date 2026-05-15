# Time To Live

DynamoDbLite implements TTL the same way the user proposed: each item carries an absolute expiration epoch, reads filter expired rows out, and a throttled background sweep deletes them. Tracks Phase 7 in [`architecture-decisions.md`](architecture-decisions.md).

## Model

Two pieces of state.

**Per-table config** — a `ttl_config` table maps `table_name → attribute_name`. Presence of a row means TTL is enabled; the attribute name tells the engine which item attribute holds the expiration epoch. This mirrors DynamoDB, where `UpdateTimeToLive` nominates one attribute per table and `DescribeTimeToLive` reports it.

```sql
CREATE TABLE ttl_config (
    table_name      TEXT PRIMARY KEY,
    attribute_name  TEXT NOT NULL
);
```

**Per-item epoch** — every row in `items` (and every row in each `idx_{tableName}_{indexName}` table) has a `ttl_epoch REAL` column. It holds the parsed numeric value of the item's TTL attribute at write time, or `NULL` if TTL is disabled / the attribute is missing / the attribute isn't a parseable number. Storing the epoch as a denormalized column is what makes the read-time filter a simple `WHERE` clause instead of a JSON parse per row.

`NowEpoch()` is `DateTimeOffset.UtcNow.ToUnixTimeSeconds()` — seconds since the Unix epoch, matching DynamoDB's TTL semantics.

## Write path

On every `PutItem` / `UpdateItem` / batch / transact write, the client calls `store.GetTtlAttributeNameAsync(tableName)` once per table to find the configured attribute name, then `TtlEpochParser.TryParse` extracts it from the item:

```csharp
internal static bool TryParse(
    Dictionary<string, AttributeValue> item,
    string ttlAttributeName,
    out double epoch)
{
    epoch = default;
    return item.TryGetValue(ttlAttributeName, out var attr)
        && attr.N is not null
        && double.TryParse(attr.N, NumberStyles.Any, CultureInfo.InvariantCulture, out epoch);
}
```

Only `N`-typed attributes parse — anything else stores `NULL` and behaves as never-expiring, matching DynamoDB's "items with non-numeric or missing TTL attributes are not deleted" rule.

The parsed value lands in `ttl_epoch` on the upsert. Index writes propagate the same value into every secondary-index row for that item, so index queries get the same filter without joining back to `items`.

## Read path

Every read SQL fragment carries the same predicate:

```sql
WHERE ... AND (ttl_epoch IS NULL OR ttl_epoch >= @nowEpoch)
```

This appears in `GetItem`, `Query`, `Scan`, the conditional-write existence checks in `UpdateItem` / `DeleteItem` / transact, and the index-side `Query` / `Scan`. `@nowEpoch` is computed once per call from `NowEpoch()`.

Effect: an expired item is invisible to every API surface the moment its epoch passes, regardless of whether the row has been swept yet. This is stricter than real DynamoDB, where expired-but-not-yet-deleted items can still appear in scans for up to 48h. Stricter is fine — tests written against real DynamoDB's documented contract pass against DynamoDbLite.

## Background cleanup

`SqliteStore.CleanupExpiredItemsAsync(tableName)` deletes rows where `ttl_epoch IS NOT NULL AND ttl_epoch < @nowEpoch` from `items` and from every index table for that table, then refreshes `item_count` / `table_size_bytes` on the `tables` row. The whole thing runs in a single write transaction.

Triggering is fire-and-forget from `DynamoDbClient.TriggerBackgroundCleanup`, called after writes:

```csharp
private void TriggerBackgroundCleanup(string tableName) =>
    _ = CleanupExpiredItemsSafeAsync(tableName);
```

Throttled to **once per 30 seconds per table** via an in-memory `lastCleanupByTable` dictionary inside the store. The throttle exists because cleanup takes a write lock and would otherwise contend with the write that just triggered it on every burst. Failures are logged but do not propagate (`CA1031` suppression is intentional — a fire-and-forget background task that crashes the process on a transient SQLite error would be worse than a missed sweep).

There is no timer or background thread. Cleanup happens opportunistically on the next write after the throttle window expires. If a table goes idle, expired rows stay on disk indefinitely — but they remain invisible to readers, so the only cost is storage. This matches the "lite" positioning: no thread overhead, correctness on read, eventual storage reclamation.

## Enable / disable

`UpdateTimeToLiveAsync(tableName, { Enabled = true, AttributeName = "ttl" })`:

1. Insert into `ttl_config`.
2. **Backfill** — scan all existing items, parse the attribute, write `ttl_epoch` per row. Repeat for each secondary-index table via `BackfillIndexTtlEpochAsync`. Without backfill, items written before TTL was enabled would never expire.
3. Returns the `TimeToLiveSpecification` echoed back, status `OK`.

Errors match DynamoDB:

- Already enabled → `AmazonDynamoDBException("TimeToLive is already enabled on the table")`
- Missing `Enabled` → invalid-parameter exception
- Missing `AttributeName` when enabling → argument exception

`UpdateTimeToLiveAsync(..., { Enabled = false })`:

1. Delete the `ttl_config` row.
2. `UPDATE items SET ttl_epoch = NULL WHERE table_name = @tableName`, and the same for each index table.

Setting epochs back to `NULL` is what makes the read filter pass for previously-expiring items — disable means stop expiring, not delete-everything-that-was-marked.

`DescribeTimeToLiveAsync` returns `ENABLED` + the attribute name if a config row exists, `DISABLED` + null otherwise. No `ENABLING` / `DISABLING` transient states — the operations are synchronous from the caller's perspective.

## Why this design

Three properties fall out of putting the epoch in a dedicated indexed column rather than parsing JSON on read:

1. **Correctness on read is unconditional.** No race between "item expired" and "sweeper hasn't run yet" — the SQL filter is the source of truth.
2. **Cleanup is decoupled from correctness.** The sweep is a storage-reclamation optimization, not a correctness mechanism. It can be throttled aggressively, fail silently, or skip idle tables without affecting query results.
3. **Index parity is automatic.** Because `ttl_epoch` lives on each index row too, GSI/LSI queries get the same filter with the same predicate — no special-case index-side logic.

The cost is the backfill on enable and one extra column-write per item. Both are negligible at lite-emulator scale.

## Testing

TTL is **excluded from the parity suite** ([`parity.md`](parity.md)). `amazon/dynamodb-local` only runs its TTL sweep on a long cron, so a parity assertion of the form "write item with past epoch → read returns nothing" can't complete in CI-friendly time against the container. The behavioral claims in this doc — read-time invisibility, NULL-on-non-numeric, backfill semantics, enable/disable error parity — rest on the [AWS API reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTimeToLive.html), not on cross-backend tests. The unit tests in `DynamoDbLite.Tests` cover the in-process behavior end-to-end.
