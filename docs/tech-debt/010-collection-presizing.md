# List/Dictionary Pre-sizing

- **Area:** Core / SqliteStore
- **Priority:** High
- **Status:** Resolved

## Problem
Throughout the codebase, collections are created without capacity hints, causing unnecessary resizing and allocations.

## Suggested Fix
- JSON deserialization loops (`DeserializeKeySchema`, `DeserializeAttributeDefinitions`, `DeserializeIndexDefinitions`) — use `GetArrayLength()` to pre-allocate
- `HashSet<(string,string,string)>` in transactions — pass `actions.Count` as capacity
- `Dictionary<string, string>` in legacy scan conversion — pass `conditions.Count`

## Code References
- `src/DynamoDbLite/AttributeValueSerializer.cs` — deserialization methods
- `src/DynamoDbLite/DynamoDbClient.Transactions.cs` — transaction uniqueness set
- `src/DynamoDbLite/DynamoDbClient.Query.cs` — legacy scan conversion
