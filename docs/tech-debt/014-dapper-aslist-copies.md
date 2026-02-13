# Dapper .AsList() Potential Copies

- **Area:** SqliteStore
- **Type:** Performance
- **Priority:** Medium
- **Status:** Open

## Problem
Multiple query results call `.AsList()` which may copy if the underlying type isn't already a `List<T>`. Dapper's `QueryAsync` already returns a collection that supports `AsList()` without copying in most cases, but this should be verified.

## Suggested Fix
Verify that Dapper's return type is already a `List<T>` in all call sites. If so, cast directly; if not, consider whether the list conversion is necessary.

## Code References
- `src/DynamoDbLite/SqliteStore.cs` — multiple `.AsList()` call sites
