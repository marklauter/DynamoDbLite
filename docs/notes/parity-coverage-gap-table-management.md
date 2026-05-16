# Parity coverage gap: table management

Tags: release, v1.0, parity, coverage-gap
Every parity test uses CreateTable, but DescribeTable / UpdateTable / ListTables are never the subject of explicit cross-backend assertions.


## Observation

The parity suite under `test/DynamoDbLite.Parity.Tests/` covers CRUD, queries, scans, transactions, batch, indexes (GSI + LSI), expressions, reserved words, projection variants, return values, parallel scan, and validation order. Each test calls `CreateTableAsync` as setup — but no test asserts the *shape* of what comes back from `DescribeTableAsync`, the behavior of `UpdateTableAsync` (adding a GSI in-place), or the pagination behavior of `ListTablesAsync`.

See [`parity-with-dynamodb-local.md`](parity-with-dynamodb-local.md) for the current Covered list.

## Interpretation

Implicit coverage hides drift in TableDescription field shape, status transitions, and edge-case behavior on the table-management surface. Real DynamoDB returns specific values for `CreationDateTime`, `ItemCount`, `TableSizeBytes`, `TableArn` — DdbLite's may not match in shape or semantics. The "Assertion strategy" section in [`parity-with-dynamodb-local.md`](parity-with-dynamodb-local.md) already calls out that some fields legitimately differ across backends (TableArn, CreationDateTime, ResponseMetadata.RequestId); the gap here is around the fields and behaviors that *should* match and aren't being checked.

## Next

Add `TableManagementParityTests` covering:

- `DescribeTableAsync` returns `ACTIVE` status with the supplied `KeySchema` and `AttributeDefinitions`.
- `ListTablesAsync` includes the newly created table; pagination via `ExclusiveStartTableName` works.
- `UpdateTableAsync` adds a GSI in-place and the GSI is queryable afterward.
- `DeleteTableAsync` removes the table from `ListTablesAsync` and subsequent operations on it throw `ResourceNotFoundException`.

Skip fields that legitimately differ across backends per [`parity-with-dynamodb-local.md`](parity-with-dynamodb-local.md) (TableArn, CreationDateTime, RequestId). Add a Covered entry to that note when the file lands.
