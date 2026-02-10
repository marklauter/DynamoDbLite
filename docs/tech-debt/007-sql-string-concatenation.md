# SQL String Concatenation in Query Hot Paths

- **Area:** SqliteStore
- **Priority:** High
- **Status:** Open

## Problem
`SqliteStore.cs` builds SQL queries using `+=` on strings, which allocates intermediate strings on every query.

## Suggested Fix
Use `StringBuilder` or `DefaultInterpolatedStringHandler` for dynamic SQL assembly.

## Code References
- `src/DynamoDbLite/SqliteStore.cs` — `sql += $" AND {skWhereSql}";` and `sql += $" ORDER BY {orderByColumn} {direction}";`
