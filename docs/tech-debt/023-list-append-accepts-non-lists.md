# list_append silently accepts non-list operands

- **Area:** Expressions / UpdateExpressionEvaluator
- **Priority:** Medium
- **Status:** Open

## Problem

`list_append()` in UpdateExpression coalesces non-list operands to empty lists instead of throwing a validation error. The `?? []` fallback was a defensive shortcut to avoid null checks. `SET attr = list_append(:val, attr)` where `:val` is a string succeeds silently, treating the string as an empty list. The result is just the existing list contents without the intended prepend.

## Suggested Fix

Throw a validation error when either operand is not a list, matching AWS DynamoDB behavior: "Invalid UpdateExpression: Incorrect operand type for operator or function".

## Code References

- `src/DynamoDbLite/Expressions/UpdateExpressionEvaluator.cs` — `list_append` handling
