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
6. **Phase 6 — Transactions (complete):** `TransactGetItemsAsync`, `TransactWriteItemsAsync`
7. **Phase 7 — TTL (complete):** `DescribeTimeToLiveAsync`, `UpdateTimeToLiveAsync`, SQL-level read filtering, background cleanup with 30s throttle
8. **Phase 8 — Tags (complete)** · effort: low · value: low — `TagResourceAsync`, `UntagResourceAsync`, `ListTagsOfResourceAsync`
9. **Phase 9 — Capacity & limits (complete)** · effort: low · value: low — `DescribeLimitsAsync`, `DescribeEndpointsAsync`, `DetermineServiceOperationEndpoint` — return sensible default/mock values
10. **Phase 10 — Export & import (complete)** · effort: medium · value: medium — Local file-based export/import using directories as S3 bucket analogs; `DYNAMODB_JSON` format only; background `Task.Run` execution; `AWSDynamoDB/{exportId}/` file layout
11. **Phase 11 — Split SqliteStore by mode (complete)** · effort: medium · value: medium — Extract interface, implement file-based (WAL) and in-memory (`SemaphoreSlim`) stores so callers get correct concurrency regardless of connection string. See [`docs/tech-debt/003-split-sqlitestore-by-mode.md`](tech-debt/003-split-sqlitestore-by-mode.md).
12. **Phase 12 — DynamoDbContext ORM tests (complete)** · effort: high · value: high — Exercise DynamoDbLite with real-world usage patterns through the `DynamoDBContext` high-level ORM. 50+ tests across InMemory and FileBased stores covering CRUD with simple/composite keys, type mapping (primitives, DateTime, enums, collections, nullables), optimistic locking via `[DynamoDBVersion]`, GSI queries and scans, batch get/write (single and multi-table), pagination, sort key ordering, and attribute mapping (`[DynamoDBProperty]`, `[DynamoDBIgnore]`). Seven model classes and tables exercise distinct key schemas and index configurations.
13. **Phase 13 — Tech debt cleanup** · effort: medium · value: medium — Address accumulated tech debt items tracked in [`docs/tech-debt/`](tech-debt/)
14. **Phase 14 — Parity tests** · effort: medium · value: high — Integration tests that run against both DynamoDbLite and real DynamoDB (via Testcontainers + Podman on WSL) to confirm behavioral parity. `DynamoDbFixture` spins up `amazon/dynamodb-local:latest` container; requires Podman as the container runtime running under WSL (daemonless, no Docker needed). Tests verify identical responses from both implementations.

### Out of scope

These operations are not meaningful for a local embedded emulator and will remain as `NotImplementedException` stubs:

- **Backup & restore:** `CreateBackup`, `DeleteBackup`, `RestoreTableFromBackup`, PITR
- **Global tables & replication:** `CreateGlobalTable`, replica management
- **Kinesis streaming:** `EnableKinesisStreamingDestination` and related
- **PartiQL:** `ExecuteStatement`, `BatchExecuteStatement`, `ExecuteTransaction`
- **Contributor insights / resource policies**
