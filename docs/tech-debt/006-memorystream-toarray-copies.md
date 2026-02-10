# MemoryStream .ToArray() — Avoidable Copies

- **Area:** Core / Expressions
- **Priority:** High
- **Status:** Open

## Problem
Multiple files call `.ToArray()` on `MemoryStream`, allocating a full copy of the buffer each time. This is especially costly in hot paths like condition evaluation and binary comparisons.

## Suggested Fix
Use `TryGetBuffer(out ArraySegment<byte> buffer)` to get a span over the existing buffer without copying.

## Code References
- `src/DynamoDbLite/Expressions/ConditionExpressionEvaluator.cs` — `CompareBytes(left.B.ToArray(), right.B.ToArray())`
- `src/DynamoDbLite/Expressions/UpdateExpressionEvaluator.cs` — `.BS.Any(eb => eb.ToArray().AsSpan().SequenceEqual(...))`
- `src/DynamoDbLite/AttributeValueSerializer.cs` — `writer.WriteBase64StringValue(value.B.ToArray())`
- `src/DynamoDbLite/KeyHelper.cs` — `Convert.ToBase64String(value.B.ToArray())`
