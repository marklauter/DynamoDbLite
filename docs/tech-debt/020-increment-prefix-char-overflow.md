# IncrementPrefix char overflow breaks begins_with queries

- **Area:** KeyConditionSqlBuilder
- **Type:** Bug
- **Priority:** High
- **Status:** Resolved

## Problem

Incrementing a char at `\uffff` wraps to `\u0000`, producing an invalid upper bound for `begins_with` SQL range queries. `IncrementPrefix("\uffff")` returns `"\u0000"` instead of a value greater than the input. The SQL range `sk >= @prefix AND sk < @prefixEnd` becomes `sk >= '\uffff' AND sk < '\u0000'`, which matches nothing. The helper was written for the common ASCII case and doesn't handle Unicode boundary characters.

## Suggested Fix

Handle the overflow case — either carry into previous characters or fall back to a prefix-only filter without an upper bound when overflow occurs.

## Code References

- `src/DynamoDbLite/KeyConditionSqlBuilder.cs` — `IncrementPrefix` method
