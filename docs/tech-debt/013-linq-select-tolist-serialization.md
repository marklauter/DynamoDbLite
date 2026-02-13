# LINQ .Select().ToList() for Serialization

- **Area:** SqliteStore, DataPipeline
- **Type:** Performance
- **Priority:** Medium
- **Status:** Resolved

## Problem
`SqliteStore.cs` and `DynamoDbClient.DataPipeline.cs` created anonymous objects via LINQ for JSON serialization, allocating intermediate lists.

## Resolution
- Created 7 `Wire`-suffixed records in `DynamoDbLite.Serialization` namespace (one file each)
- Replaced `.Select().ToList()` with pre-allocated arrays and `for` loops
- `KeySchemaElementExtensions.ToKeySchemas()` — shared extension with `AggressiveInlining`
- `SerializerExtensions.ToJson()` — extension methods on `TableCreationParameters` and `List<IndexDefinition>`
- Wire suffix eliminates name collisions with AWS SDK types — no aliases needed
