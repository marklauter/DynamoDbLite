# Architecture Decisions

## Storage Model

Single SQLite table with dedicated columns for hash key, sort key, and JSON payload — keeps DynamoDB's schemaless nature while allowing efficient key lookups.

```sql
CREATE TABLE items (
    table_name  TEXT NOT NULL,
    pk          TEXT NOT NULL,
    sk          TEXT NOT NULL DEFAULT '',
    item_json   TEXT NOT NULL,
    PRIMARY KEY (table_name, pk, sk)
);
```

## SQLite Lifetime

Configurable via connection string; defaults to in-memory.

## Concurrency

Connection pooling with thread-safe access across concurrent async calls.

## Behavioral Fidelity

Full fidelity with DynamoDB semantics:

- Parse and evaluate `ConditionExpression`, `FilterExpression`, `ProjectionExpression`
- Support `UpdateExpression` (`SET`, `REMOVE`, `ADD`, `DELETE`)
- Enforce key schema validation

## Implementation Phases

1. **Phase 1 — Table management (complete):** `CreateTableAsync`, `DeleteTableAsync`, `DescribeTableAsync`, `ListTablesAsync`
2. **Phase 2 — Item CRUD (complete):** `PutItemAsync`, `GetItemAsync`, `DeleteItemAsync`, `UpdateItemAsync`
3. **Phase 3 — Querying (complete):** `QueryAsync`, `ScanAsync`
4. **Phase 4 — Batch operations (complete):** `BatchGetItemAsync`, `BatchWriteItemAsync`
5. **Phase 5 — Secondary indexes (complete):** GSI/LSI creation, index maintenance on writes, `QueryAsync`/`ScanAsync` with `IndexName`
6. **Phase 6 — Transactions** · effort: medium · value: high — `TransactGetItemsAsync`, `TransactWriteItemsAsync`
7. **Phase 7 — TTL** · effort: low · value: medium — `DescribeTimeToLiveAsync`, `UpdateTimeToLiveAsync` (metadata-only, no background expiry)
8. **Phase 8 — Tags** · effort: low · value: low — `TagResourceAsync`, `UntagResourceAsync`, `ListTagsOfResourceAsync`
9. **Phase 9 — Capacity & limits (mock)** · effort: low · value: low — `DescribeLimitsAsync`, `DescribeEndpointsAsync` — return sensible default/mock values
10. **Phase 10 — Export & import** · effort: medium · value: medium — Local file-based export/import using directories as S3 bucket analogs

### Out of scope

These operations are not meaningful for a local embedded emulator and will remain as `NotImplementedException` stubs:

- **Backup & restore:** `CreateBackup`, `DeleteBackup`, `RestoreTableFromBackup`, PITR
- **Global tables & replication:** `CreateGlobalTable`, replica management
- **Kinesis streaming:** `EnableKinesisStreamingDestination` and related
- **PartiQL:** `ExecuteStatement`, `BatchExecuteStatement`, `ExecuteTransaction`
- **Contributor insights / resource policies**
