# Lossy serialization of empty collections

- **Area:** AttributeValueSerializer
- **Type:** Bug
- **Priority:** Medium
- **Status:** Resolved

## Problem

Empty `L` (list) and `M` (map) values were serialized as `{"NULL":true}` instead of `{"L":[]}` / `{"M":{}}`, losing type information on round-trip. AWS SDK v4 initializes `AttributeValue.L` to an empty `List<AttributeValue>` and `AttributeValue.M` to an empty `Dictionary<string, AttributeValue>` (both non-null). The `WriteAttributeValue` method used `is { Count: > 0 }` checks, so empty collections fell through to the NULL fallback.

## Resolution

Added `IsAlwaysSend<T>()` helper that detects the SDK's `AlwaysSendDictionary` / `AlwaysSendList` marker subtypes by comparing `value.GetType() != typeof(T)`. When the SDK intentionally sends an empty collection, it wraps it in these subtypes. The serializer now checks `Count > 0 || IsAlwaysSend(value.L/M)` to preserve intentionally-empty collections while still treating default-initialized empty collections as unset.

## Code References

- `src/DynamoDbLite/AttributeValueSerializer.cs:IsAlwaysSend` — generic type check for SDK marker subtypes
- `src/DynamoDbLite/AttributeValueSerializer.cs:WriteAttributeValue` — L and M branches now use `IsAlwaysSend` fallback
