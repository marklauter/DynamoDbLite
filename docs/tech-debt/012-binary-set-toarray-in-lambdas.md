# Binary Set Operations — Repeated .ToArray() in LINQ Predicates

- **Area:** Expressions
- **Priority:** Medium
- **Status:** Open

## Problem
`UpdateExpressionEvaluator.cs` calls `.ToArray()` inside `Any`/`RemoveAll` lambdas, meaning every element comparison allocates a new array.

## Suggested Fix
Extract `b.ToArray()` (or better, `TryGetBuffer`) outside the lambda, compare using spans.

## Code References
- `src/DynamoDbLite/Expressions/UpdateExpressionEvaluator.cs` — `existing.BS.Any(eb => eb.ToArray().AsSpan().SequenceEqual(b.ToArray()))`
