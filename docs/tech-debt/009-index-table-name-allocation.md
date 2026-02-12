# IndexTableName String Allocation

- **Area:** SqliteStore
- **Priority:** High
- **Status:** Resolved

## Problem
`SqliteStore.IndexTableName` is called on every index read/write and allocates via interpolation (`$"idx_{tableName}_{indexName}"`). Similarly, inline ARN strings (`$"arn:aws:dynamodb:local:000000000000:table/{...}"`) are repeatedly built for the same small set of table/index names.

## Resolution
Added `static ConcurrentDictionary` caches with `GetOrAdd` for three deterministic string-building helpers:

- **`IndexTableName`** — `ConcurrentDictionary<(string, string), string>` eliminates allocation after first call per unique (table, index) pair
- **`TableArn`** — `ConcurrentDictionary<string, string>` caches `arn:aws:dynamodb:local:000000000000:table/{name}` (replaced 8 inline usages across SqliteStore + DataPipeline)
- **`IndexArn`** — `ConcurrentDictionary<(string, string), string>` caches `...table/{name}/index/{index}` (replaced 2 inline usages in GSI/LSI descriptions)

## Code References
- `src/DynamoDbLite/SqliteStores/SqliteStore.cs` — `IndexTableName`, `TableArn`, `IndexArn` methods
- `src/DynamoDbLite/DynamoDbClient.DataPipeline.cs` — callers of `SqliteStore.TableArn`
