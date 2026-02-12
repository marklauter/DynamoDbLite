# Empty containers treated as null in path resolution

- **Area:** Expressions / ExpressionHelper
- **Priority:** Medium
- **Status:** Resolved

## Problem

`ResolvePath` and `SetAtPath` reject empty maps/lists as if they were null, breaking paths that traverse empty containers. The `{ Count: > 0 }` pattern was used as a shorthand for "has a map/list" but it also excludes empty containers. A path like `a.b.c` where `a.b` is an empty map `{}` resolves to null instead of looking up key `c` in the empty map (which would correctly return null/not-found). This affects ConditionExpression, ProjectionExpression, and UpdateExpression evaluation.

## Suggested Fix

Check `is not null` (or `is { }`) instead of `is { Count: > 0 }` so empty containers are valid intermediate path nodes. The path should only return null when the key genuinely doesn't exist.

## Code References

- `src/DynamoDbLite/Expressions/ExpressionHelper.cs` — `ResolvePath` and `SetAtPath` methods
