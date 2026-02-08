# Architecture Decisions

## 1. Storage Model

**Question:** Should each DynamoDB table map to its own SQLite table, or should there be a single SQLite table that stores all items as serialized blobs (e.g., JSON)?

- **Single-table-with-JSON** — simpler, more faithful to DynamoDB's schemaless nature
- **Per-table DDL** — more relational, but harder to reconcile with DynamoDB's flexible attributes

**Decision:** Single SQLite table with dedicated columns for hash key, sort key, and JSON payload.

```sql
CREATE TABLE items (
    table_name  TEXT NOT NULL,
    pk          TEXT NOT NULL,
    sk          TEXT NOT NULL DEFAULT '',
    item_json   TEXT NOT NULL,
    PRIMARY KEY (table_name, pk, sk)
);
```

This keeps the schemaless nature of DynamoDB while allowing efficient lookups on the key columns.

## 2. SQLite Lifetime

**Question:** Should the database be purely in-memory (`:memory:`), file-backed, or configurable? Should the constructor accept a connection string or path?

**Decision:** Configurable. Accept a connection string or path; default to in-memory (`:memory:`).

## 3. Scope of Operations

**Question:** Which operations should be implemented first?

- **Table management:** `CreateTableAsync`, `DeleteTableAsync`, `DescribeTableAsync`, `ListTablesAsync`
- **Item CRUD:** `PutItemAsync`, `GetItemAsync`, `DeleteItemAsync`, `UpdateItemAsync`
- **Querying:** `QueryAsync`, `ScanAsync`

Or tackle everything at once?

**Decision:** Incremental, in three phases:

1. **Phase 1 — Table management (complete):** `CreateTableAsync`, `DeleteTableAsync`, `DescribeTableAsync`, `ListTablesAsync`
2. **Phase 2 — Item CRUD:** `PutItemAsync`, `GetItemAsync`, `DeleteItemAsync`, `UpdateItemAsync`
3. **Phase 3 — Querying:** `QueryAsync`, `ScanAsync`

## 4. Behavioral Fidelity

**Question:** How closely should we match DynamoDB semantics?

- Should `ConditionExpression` / `FilterExpression` / `ProjectionExpression` be parsed and evaluated?
- Should `UpdateExpression` (`SET`, `REMOVE`, `ADD`, `DELETE`) be supported?
- Should we enforce key schema validation (rejecting puts with missing partition/sort keys)?

**Decision:** Full fidelity.

- Parse and evaluate `ConditionExpression`, `FilterExpression`, `ProjectionExpression`
- Support `UpdateExpression` (`SET`, `REMOVE`, `ADD`, `DELETE`)
- Enforce key schema validation (reject puts with missing partition/sort keys)

## 5. Secondary Indexes

**Question:** Should GSI/LSI creation and querying be supported, or out of scope for now?

**Decision:** Out of scope for now. GSI/LSI support will be added later.

## 6. Concurrency

**Question:** Single connection or connection pooling? Should we worry about thread safety across concurrent async calls?

**Decision:** Connection pooling with thread-safe access across concurrent async calls.
