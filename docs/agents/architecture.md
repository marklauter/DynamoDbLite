# Architecture

All projects under `src/`; add new ones to `DynamoDbLite.slnx`.

## Conventions

- `DynamoDbClient.cs` is a partial class — core + disposal here, one file per feature area:
  - `.TableManagement.cs`, `.Crud.cs`, `.Query.cs`, `.Batch.cs`, `.Transactions.cs`
  - `.DataPipeline.cs` — export/import (Phase 10, complete)
  - `.Admin.cs`, `.Backup.cs`, `.GlobalTables.cs`, `.Streams.cs` — out-of-scope APIs that throw `NotImplementedException`
- `SqliteStores/` — abstract `SqliteStoreBase` with two sealed implementations:
  - `InMemorySqliteStore` — sentinel connection keeps the DB alive; `AsyncReaderWriterLock` serializes writes
  - `FileSqliteStore` — WAL mode handles concurrency; no in-process lock needed
- `Expressions/` — Superpower-based pipeline: `DynamoDbTokenizer` → `Ast` → `*Parser` → `*Evaluator`

For storage schema and design rationale, see [`../architecture-decisions.md`](../architecture-decisions.md) and the [Storage Architecture](https://github.com/marklauter/DynamoDbLite/wiki/Storage-Architecture) wiki page.
