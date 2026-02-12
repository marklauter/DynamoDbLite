# Empty set type detection returns wrong result

- **Area:** Expressions / ConditionExpressionEvaluator
- **Priority:** Medium
- **Status:** Resolved

## Problem

`attribute_type()` condition function fails to identify empty SS, NS, and BS sets because it checks `Count > 0` instead of `is not null`. The pattern matching used `{ SS.Count: > 0 }` to distinguish set types, which excludes empty sets. `attribute_type(attr, :ss)` returns false when `attr` is an empty string set `SS: []`. The attribute falls through to the default `""` case. DynamoDB considers an empty set to still have a type.

## Suggested Fix

Check `{ SS: not null }` (or equivalent) so that empty sets are still correctly identified by their type.

## Code References

- `src/DynamoDbLite/Expressions/ConditionExpressionEvaluator.cs` — `EvaluateAttributeType` method
