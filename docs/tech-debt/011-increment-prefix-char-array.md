# IncrementPrefix Char Array Allocation

- **Area:** KeyConditionSqlBuilder
- **Type:** Performance
- **Priority:** High
- **Status:** Resolved

## Problem
`KeyConditionSqlBuilder.cs` allocates a `char[]` via `ToCharArray()` to increment the last character of a prefix string.

## Suggested Fix
Use `string.Create(prefix.Length, prefix, (span, p) => { p.AsSpan().CopyTo(span); span[^1]++; })`.

## Code References
- `src/DynamoDbLite/KeyConditionSqlBuilder.cs` — `IncrementPrefix` method
