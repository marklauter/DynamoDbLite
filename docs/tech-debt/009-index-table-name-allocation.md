# IndexTableName String Allocation

- **Area:** SqliteStore
- **Priority:** High
- **Status:** Open

## Problem
`SqliteStore.IndexTableName` is called on every index read/write and allocates via interpolation (`$"idx_{tableName}_{indexName}"`).

## Suggested Fix
Use `string.Create(length, (tableName, indexName), ...)` with exact pre-computed length, or cache results.

## Code References
- `src/DynamoDbLite/SqliteStore.cs` — `IndexTableName` method
