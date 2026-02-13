# LINQ to Loop Replacements in Validation Paths

- **Area:** DynamoDbClient.TableManagement
- **Type:** Performance
- **Priority:** High
- **Status:** Resolved

## Problem
`DynamoDbClient.TableManagement.cs` enumerates key schemas 3 times with separate LINQ queries to get hash keys, range keys, and key attribute names.

## Suggested Fix
Single-pass `foreach` loop classifying into hash/range/names simultaneously.

## Code References
- `src/DynamoDbLite/DynamoDbClient.TableManagement.cs` — `keySchema.Where(...).ToList()` (x2) and `keySchema.Select(...).ToHashSet()`
