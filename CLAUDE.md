# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

In-process `IAmazonDynamoDB` backed by `SQLite` for local dev/testing, or mobile apps.

## Quick Reference
- Solution file: `src/DynamoDbLite.slnx`
- Build: `dotnet build "src/DynamoDbLite.slnx"`
- Test: `dotnet test "src/DynamoDbLite.slnx"`
- Single test: `dotnet test "src/DynamoDbLite.slnx" --filter "FullyQualifiedClassName~MethodName"`
- Format: `dotnet format "src/DynamoDbLite.slnx" --verbosity normal` (whitespace, style, analyzers via `.editorconfig`)
- All projects live under `src/`; add new ones to the solution

## Code Style
Read `.claude/code-style.md` before writing any code. The `.editorconfig` is enforced by `dotnet format` — run `dotnet format "src/DynamoDbLite.slnx" --verbosity normal` after writing code and fix any violations before committing.

## Testing
Read `.claude/testing.md` before creating test projects and before writing tests.

## Architecture
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

## Tech Stack
Read `.claude/tech-stack.md` for platform and dependency info.

## Tech Debt
Read `docs/tech-debt/readme.md` for tech-debt template before creating new tech-debt records.
Read `docs/tech-debt/index.md` for tech-debt record listing and status.

## Gotchas
Read `.claude/gotchas.md` before debugging build or runtime issues.
