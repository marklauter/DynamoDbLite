# SetAtPath NullReferenceException on nested list access

- **Area:** Expressions / ExpressionHelper
- **Priority:** Low
- **Status:** Open

## Problem

`SetAtPath` accesses `.L` with null-suppression on an AttributeValue that may not be a list, causing NullReferenceException. The path-walking loop assumes the AttributeValue type matches the path element type (map element vs list index) without validating. An UpdateExpression path like `a.b[0]` where `a.b` exists but is a map (not a list) causes `current!.L` to throw NullReferenceException. AWS DynamoDB would return a validation error about type mismatch.

## Suggested Fix

Check that the AttributeValue actually contains a list before accessing `.L`. Throw a descriptive error when the path element type doesn't match the value type.

## Code References

- `src/DynamoDbLite/Expressions/ExpressionHelper.cs` — `SetAtPath` method

## Notes

Needs verification: Confirm that `.L` is actually null in this scenario with the current AWS SDK v4 `AttributeValue` type. The SDK may initialize `.L` to an empty list by default, in which case this is not reachable.
