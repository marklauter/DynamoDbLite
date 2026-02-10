# LINQ .Select().ToList() for Serialization

- **Area:** SqliteStore
- **Priority:** Medium
- **Status:** Open

## Problem
`SqliteStore.cs` creates anonymous objects via LINQ for JSON serialization, allocating intermediate lists.

## Suggested Fix
Use a `foreach` loop with pre-allocated `List<T>` and a small named struct/record.

## Code References
- `src/DynamoDbLite/SqliteStore.cs` — `JsonSerializer.Serialize(keySchema.Select(k => new { ... }).ToList())`
