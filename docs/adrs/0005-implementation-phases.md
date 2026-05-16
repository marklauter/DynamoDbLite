# ADR 0005 — Implementation Phases

Status: Informational (status log, not a decision)

Phased delivery of the public API surface. Each phase below is complete unless flagged otherwise.

1. **Phase 1 — Table management:** `CreateTableAsync`, `DeleteTableAsync`, `DescribeTableAsync`, `ListTablesAsync`
2. **Phase 2 — Item CRUD:** `PutItemAsync`, `GetItemAsync`, `DeleteItemAsync`, `UpdateItemAsync`
3. **Phase 3 — Querying:** `QueryAsync`, `ScanAsync`
4. **Phase 4 — Batch operations:** `BatchGetItemAsync`, `BatchWriteItemAsync`
5. **Phase 5 — Secondary indexes:** GSI/LSI creation, index maintenance on writes, `QueryAsync`/`ScanAsync` with `IndexName`
6. **Phase 6 — Transactions:** `TransactGetItemsAsync`, `TransactWriteItemsAsync`
7. **Phase 7 — TTL:** `DescribeTimeToLiveAsync`, `UpdateTimeToLiveAsync`, SQL-level read filtering, background cleanup with 30s throttle. Design: [`ttl.md`](../notes/ttl.md).
8. **Phase 8 — Tags** · effort: low · value: low — `TagResourceAsync`, `UntagResourceAsync`, `ListTagsOfResourceAsync`
9. **Phase 9 — Capacity & limits** · effort: low · value: low — `DescribeLimitsAsync`, `DescribeEndpointsAsync`, `DetermineServiceOperationEndpoint` — return sensible default/mock values
10. **Phase 10 — Export & import** · effort: medium · value: medium — Local file-based export/import using directories as S3 bucket analogs; `DYNAMODB_JSON` format only; background `Task.Run` execution; `AWSDynamoDB/{exportId}/` file layout
11. **Phase 11 — Split SqliteStore by mode** · effort: medium · value: medium — Extract interface, implement file-based (WAL) and in-memory (`AsyncReaderWriterLock`) stores so callers get correct concurrency regardless of connection string. See [ADR 0003](0003-concurrency-strategy.md).
12. **Phase 12 — DynamoDbContext ORM tests** · effort: high · value: high — Exercise DynamoDbLite with real-world usage patterns through the `DynamoDBContext` high-level ORM. 50+ tests across InMemory and FileBased stores covering CRUD with simple/composite keys, type mapping (primitives, DateTime, enums, collections, nullables), optimistic locking via `[DynamoDBVersion]`, GSI queries and scans, batch get/write (single and multi-table), pagination, sort key ordering, and attribute mapping (`[DynamoDBProperty]`, `[DynamoDBIgnore]`). Seven model classes and tables exercise distinct key schemas and index configurations.
13. **Phase 13 — Tech debt cleanup** · effort: medium · value: medium — Address accumulated tech debt items tracked as GitHub issues.
14. **Phase 14 — Parity tests** · effort: medium · value: high — Integration tests that run against `DynamoDbClient` (in-memory and file-based SQLite) and `amazon/dynamodb-local` (Testcontainers + Podman) to confirm behavioral parity. The parity project at `tests/DynamoDbLite.Parity.Tests/` hosts the three-backend collection fixture and asserts CRUD, condition expressions, update expressions, query, scan, transactions, batch, and GSI/LSI parity. Design, full coverage list, and out-of-scope decisions: [`notes/parity-with-dynamodb-local.md`](../notes/parity-with-dynamodb-local.md).
