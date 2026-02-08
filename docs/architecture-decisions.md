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

## Secondary Indexes

Out of scope for now.

## Behavioral Fidelity

Full fidelity with DynamoDB semantics:

- Parse and evaluate `ConditionExpression`, `FilterExpression`, `ProjectionExpression`
- Support `UpdateExpression` (`SET`, `REMOVE`, `ADD`, `DELETE`)
- Enforce key schema validation

## Implementation Phases

1. **Phase 1 — Table management (complete):** `CreateTableAsync`, `DeleteTableAsync`, `DescribeTableAsync`, `ListTablesAsync`
2. **Phase 2 — Item CRUD (complete):** `PutItemAsync`, `GetItemAsync`, `DeleteItemAsync`, `UpdateItemAsync`
3. **Phase 3 — Querying:** `QueryAsync`, `ScanAsync`
4. **Phase 4 — Secondary Indexes:** GSI/LSI creation and querying
