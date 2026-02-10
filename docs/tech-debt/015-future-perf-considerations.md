# Future Performance Considerations

- **Area:** Core
- **Priority:** Low
- **Status:** Open

## Problem
Several lower-impact optimization opportunities exist that should be measured before implementing:
- `GetTtlAttributeNameAsync`, `GetKeySchemaAsync` could use `ValueTask` for synchronous fast paths
- Temporary `byte[]` buffers in binary comparisons could use `ArrayPool<T>`
- Integer boxing in string interpolation (`$"#legacyN{i}"`) — `string.Create` avoids it
- High-frequency internal types like `ItemRow`, `IndexItemRow` could be structs instead of records (measure first — Dapper materialization matters)

## Suggested Fix
Profile first, then apply targeted optimizations where measurements show meaningful impact.

## Code References
- `src/DynamoDbLite/SqliteStore.cs` — `GetTtlAttributeNameAsync`, `GetKeySchemaAsync`, `ItemRow`, `IndexItemRow`
- `src/DynamoDbLite/Expressions/ConditionExpressionEvaluator.cs` — binary comparison buffers
- `src/DynamoDbLite/DynamoDbClient.Query.cs` — legacy scan integer interpolation
