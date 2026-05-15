# Architecture Decisions

## Storage Model

All data lives in SQLite. The schema centres on an `items` table with `(table_name, pk, sk)` as primary key and a JSON payload column, plus a denormalized numeric sort-key column (`sk_num`) for ordered range queries and a `ttl_epoch` column for read-time TTL filtering. Table metadata, TTL config, tags, exports, imports, and per-index tables are separate.

For the full schema (column-by-column, plus the per-GSI/LSI `idx_{tableName}_{indexName}` tables), see [Storage Architecture](https://github.com/marklauter/DynamoDbLite/wiki/Storage-Architecture) in the wiki.

## SQLite Lifetime

Configurable via connection string; defaults to in-memory.

## Concurrency

Concurrency strategy depends on the connection mode (Phase 11 split):

- **In-memory (`InMemorySqliteStore`)**: a sentinel connection keeps the database alive, and an `AsyncReaderWriterLock` (see `SqliteStores/AsyncReaderWriterLock.cs`) serializes writes while allowing concurrent reads.
- **File-based (`FileSqliteStore`)**: SQLite WAL mode handles concurrency; no in-process lock is needed.

Callers get correct behaviour regardless of which connection string they pass.

## Behavioral Fidelity

Full fidelity with DynamoDB semantics:

- Parse and evaluate `ConditionExpression`, `FilterExpression`, `KeyConditionExpression`, `ProjectionExpression`
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
11. **Phase 11 — Split SqliteStore by mode (complete)** · effort: medium · value: medium — Extract interface, implement file-based (WAL) and in-memory (`AsyncReaderWriterLock`) stores so callers get correct concurrency regardless of connection string.
12. **Phase 12 — DynamoDbContext ORM tests (complete)** · effort: high · value: high — Exercise DynamoDbLite with real-world usage patterns through the `DynamoDBContext` high-level ORM. 50+ tests across InMemory and FileBased stores covering CRUD with simple/composite keys, type mapping (primitives, DateTime, enums, collections, nullables), optimistic locking via `[DynamoDBVersion]`, GSI queries and scans, batch get/write (single and multi-table), pagination, sort key ordering, and attribute mapping (`[DynamoDBProperty]`, `[DynamoDBIgnore]`). Seven model classes and tables exercise distinct key schemas and index configurations.
13. **Phase 13 — Tech debt cleanup** · effort: medium · value: medium — Address accumulated tech debt items tracked as GitHub issues.
14. **Phase 14 — Parity tests (in progress)** · effort: medium · value: high — Integration tests that run against `DynamoDbClient` (in-memory and file-based SQLite) and `amazon/dynamodb-local` (Testcontainers + Podman) to confirm behavioral parity. The parity project at `src/DynamoDbLite.Parity.Tests/` hosts the three-backend collection fixture and the first 14 scenarios (42 test executions per run) covering CRUD, condition expressions, update expressions, query, scan, transactions, batch, and GSI. Design and coverage: [`parity.md`](parity.md).

### Out of scope

These operations are not meaningful for a local embedded emulator and will remain as `NotImplementedException` stubs:

- **Backup & restore:** `CreateBackup`, `DeleteBackup`, `RestoreTableFromBackup`, PITR
- **Global tables & replication:** `CreateGlobalTable`, replica management
- **Kinesis streaming:** `EnableKinesisStreamingDestination` and related
- **PartiQL:** `ExecuteStatement`, `BatchExecuteStatement`, `ExecuteTransaction`
- **Contributor insights / resource policies**
