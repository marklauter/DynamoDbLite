# Architecture

All projects live under `src/`; add new ones to the solution.

- `DynamoDbClient.cs` — partial class, core + disposal; split by feature into:
  - `DynamoDbClient.TableManagement.cs`, `.Crud.cs`, `.Query.cs`, `.Batch.cs`, `.Transactions.cs`
  - `.Admin.cs`, `.Backup.cs`, `.GlobalTables.cs`, `.Streams.cs`, `.DataPipeline.cs` (stubs)
- `SqliteStore.cs` — abstract `SqliteStoreBase`, all DB access; two sealed implementations:
  - `InMemorySqliteStore.cs` — shared-cache + `AsyncReaderWriterLock` for concurrency
  - `FileSqliteStore.cs` — WAL mode, no additional locking needed
- `Expressions/` — Superpower-based parsers and evaluators for DynamoDB expressions
  - `DynamoDbTokenizer.cs` → `Ast.cs` → `*Parser.cs` → `*Evaluator.cs`
- `KeyHelper.cs` — PK/SK extraction and key type validation
- `KeyConditionSqlBuilder.cs` — translates KeyCondition AST to SQL fragments
- `AttributeValueSerializer.cs` — DynamoDB JSON ↔ `Dictionary<string, AttributeValue>`
- `DynamoDbLiteOptions.cs` — configuration record
- `DynamoDbService.cs` — DI registration
