---
title: Architecture
summary: Where library code, tests, and stores live, and the conventions binding them.
document:
  tags: [architecture, agents]
---

# Architecture

Library code under `src/`, tests under `tests/`; add new projects to `DynamoDbLite.slnx` at the repo root.

## Conventions

- `DynamoDbClient.cs` is a partial class — core + disposal here, one file per AWS operation family (`.TableManagement.cs`, `.Crud.cs`, `.Query.cs`, `.Scan.cs`, `.Batch.cs`, `.Transactions.cs`, `.Export.cs`, `.Import.cs`, `.Tags.cs`, `.Ttl.cs`, `.Service.cs`).
  - `.Unsupported.cs` — out-of-scope APIs that throw `NotSupportedException`, grouped by `#region` matching [ADR 0006](../adrs/0006-out-of-scope-operations.md) / [ADR 0007](../adrs/0007-not-supported-exception-for-out-of-scope.md).
- `SqliteStores/` — abstract `SqliteStore` with two sealed implementations:
  - `InMemorySqliteStore` — sentinel connection keeps the shared in-memory DB alive; concurrency is left to SQLite and the Microsoft.Data.Sqlite driver, no in-process lock (see [ADR 0008](../adrs/0008-in-memory-needs-no-app-lock.md))
  - `FileSqliteStore` — WAL mode handles concurrency; no in-process lock needed
- `Expressions/` — Superpower-based pipeline: `DynamoDbTokenizer` → `Ast` → `*Parser` → `*Evaluator`

For storage schema and design rationale, see [`../adrs/index.md`](../adrs/index.md) and the [Storage Architecture](https://github.com/marklauter/DynamoDbLite/wiki/Storage-Architecture) wiki page.
