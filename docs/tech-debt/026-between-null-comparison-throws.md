# BETWEEN with null operand throws instead of returning false

- **Area:** Expressions / ConditionExpressionEvaluator
- **Type:** Bug
- **Priority:** Low
- **Status:** Open

## Problem

`CompareValues` throws `ArgumentException` on null operands, and `EvaluateBetween` calls it without a null guard. The equality path in `EvaluateComparison` has a null guard that returns false, but this guard was not replicated in `EvaluateBetween`. A condition like `age BETWEEN :low AND :high` where `:low` or the attribute value is null throws `ArgumentException("Cannot compare null attribute values")` instead of returning false.

## Suggested Fix

`EvaluateBetween` should return false when any operand is null, matching the behavior of other comparison operators and AWS DynamoDB semantics.

## Code References

- `src/DynamoDbLite/Expressions/ConditionExpressionEvaluator.cs` — `CompareValues` and `EvaluateBetween` methods

## Notes

Needs verification: Confirm that `EvaluateBetween` actually reaches `CompareValues` without an intermediate null check. If there is a guard earlier in the call chain, this may not be reachable.
