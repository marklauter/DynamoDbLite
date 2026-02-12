# List/Dictionary Pre-sizing

- **Area:** Core / SqliteStore
- **Priority:** High
- **Status:** Resolved

## Problem
Throughout the codebase, collections are created without capacity hints, causing unnecessary resizing and allocations.

## Resolution

All collection pre-sizing has been applied:

- **Transactions** (`DynamoDbClient.Transactions.cs`) — `new HashSet<(string, string, string)>(actions.Count)` and `new List<TransactWriteOperation>(resolvedActions.Count)`
- **Legacy scan conversion** (`DynamoDbClient.Query.cs`) — `new List<string>(conditions.Count)`, `new Dictionary<string, string>(conditions.Count)`, `new Dictionary<string, AttributeValue>(conditions.Count)`
- **JSON deserialization** (`SqliteStore.cs`) — `DeserializeKeySchema`, `DeserializeAttributeDefinitions`, `DeserializeIndexDefinitions` all use `root.GetArrayLength()` to pre-allocate (O(1) lookup into `JsonDocument` metadata). Inner lists (KeySchema, NonKeyAttributes) also pre-sized.
- **AttributeValueSerializer** — `ReadStringList`, `ReadBinaryList`, `ReadAttributeValueList` all use `element.GetArrayLength()` to pre-allocate

## Code References
- `src/DynamoDbLite/SqliteStores/SqliteStore.cs` — `DeserializeKeySchema`, `DeserializeAttributeDefinitions`, `DeserializeIndexDefinitions`
- `src/DynamoDbLite/AttributeValueSerializer.cs` — `ReadStringList`, `ReadBinaryList`, `ReadAttributeValueList`
- `src/DynamoDbLite/DynamoDbClient.Transactions.cs` — transaction uniqueness set
- `src/DynamoDbLite/DynamoDbClient.Query.cs` — legacy scan conversion
