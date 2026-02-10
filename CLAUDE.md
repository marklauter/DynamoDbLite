# DynamoDbLite

In-process IAmazonDynamoDB backed by SQLite for local dev/testing.

## Quick Reference
- Solution file: src/DynamoDbLite.slnx
- Build: `dotnet build "src/DynamoDbLite.slnx"`
- Test: `dotnet test "src/DynamoDbLite.slnx"`
- All projects live under `src/`; add new ones to the solution

## Code Style
Read `.claude/style.md` before writing any code.

## Testing
Read `.claude/testing.md` before creating test projects.

## Architecture
- `DynamoDbClient.cs` — partial class, core + disposal; split by feature into:
  - `DynamoDbClient.TableManagement.cs`, `.Crud.cs`, `.Query.cs`, `.Batch.cs`, `.Transactions.cs`
  - `.Admin.cs`, `.Backup.cs`, `.GlobalTables.cs`, `.Streams.cs`, `.DataPipeline.cs` (stubs)
- `SqliteStore.cs` — internal SQLite layer, all DB access; sentinel connection keeps in-memory DB alive
- `Expressions/` — Superpower-based parsers and evaluators for DynamoDB expressions
  - `DynamoDbTokenizer.cs` → `Ast.cs` → `*Parser.cs` → `*Evaluator.cs`
- `KeyHelper.cs` — PK/SK extraction and key type validation
- `KeyConditionSqlBuilder.cs` — translates KeyCondition AST to SQL fragments
- `AttributeValueSerializer.cs` — DynamoDB JSON ↔ `Dictionary<string, AttributeValue>`
- `DynamoDbLiteOptions.cs` — configuration record
- `DynamoDbService.cs` — DI registration

## Tech Stack
Read `.claude/tech-stack.md` for platform and dependency info.

## Gotchas
Read `.claude/gotchas.md` before debugging build or runtime issues.
