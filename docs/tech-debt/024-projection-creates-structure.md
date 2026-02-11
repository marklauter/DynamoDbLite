# Projection evaluator creates structure instead of selecting

- **Area:** Expressions / ProjectionExpressionEvaluator
- **Priority:** Medium
- **Status:** Open

## Problem

`ProjectionExpressionEvaluator` auto-creates nested maps and null-padded lists when projecting paths, instead of only copying existing data. The evaluator reuses `SetAtPath`-style logic to build the result, which creates intermediate containers as a side effect. Projecting `items[5].name` on an item where `items` has only 2 elements produces a result with a 6-element list (indices 0-4 filled with null AttributeValues, index 5 with an empty map). AWS DynamoDB would omit the attribute entirely.

## Suggested Fix

Projection should only copy values that exist in the source item. If a path doesn't resolve (missing key, out-of-bounds index), the attribute should be absent from the result, not synthesized with nulls.

## Code References

- `src/DynamoDbLite/Expressions/ProjectionExpressionEvaluator.cs` — list index handling
