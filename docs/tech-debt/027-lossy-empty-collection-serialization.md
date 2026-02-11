# Lossy serialization of empty collections

- **Area:** AttributeValueSerializer
- **Priority:** Medium
- **Status:** Open

## Problem

Empty `L` (list) and `M` (map) values are serialized as `{"NULL":true}` instead of `{"L":[]}` / `{"M":{}}`, losing type information on round-trip. AWS SDK v4 initializes `AttributeValue.L` to an empty `List<AttributeValue>` and `AttributeValue.M` to an empty `Dictionary<string, AttributeValue>` (both non-null). The `WriteAttributeValue` method uses `is { Count: > 0 }` checks, so empty collections fall through to the NULL fallback. This was added to unblock DynamoDBContext tests where the SDK creates AttributeValue instances with all collection properties initialized but empty.

## Suggested Fix

Properly detect and serialize empty collections (`{"L":[]}` and `{"M":{}}`). The challenge is distinguishing "intentionally empty list" from "unset attribute with default-initialized L property". Options:
1. Check if `L` is non-null AND no other type property is set — requires a priority/exclusion system
2. Track which property was explicitly set via SDK internals (if accessible)
3. Accept the SDK v4 reality and always serialize empty L as `{"L":[]}` — changes behavior but is more correct

## Code References

- `src/DynamoDbLite/AttributeValueSerializer.cs:WriteAttributeValue` — `is { Count: > 0 }` checks skip empty collections, falling through to NULL

## Notes

Affects DynamoDBContext users storing empty collections; direct low-level API users can work around it.
