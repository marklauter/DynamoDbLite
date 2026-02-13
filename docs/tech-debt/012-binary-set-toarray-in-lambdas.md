# Binary Set Operations — Repeated .ToArray() in LINQ Predicates

- **Area:** Expressions
- **Type:** Performance
- **Priority:** Medium
- **Status:** Resolved

## Problem
`UpdateExpressionEvaluator.cs` calls `.ToArray()` inside `Any`/`RemoveAll` lambdas, meaning every element comparison allocates a new array.

## Resolution
Replaced LINQ `Any`/`RemoveAll` lambdas with `BinarySetContains` and `BinarySetRemoveAll` helpers that use manual loops with `GetSpan` (backed by `TryGetBuffer`). Zero allocations when `TryGetBuffer` succeeds.
