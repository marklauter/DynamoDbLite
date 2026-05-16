# ADR 0003 — Concurrency Strategy

Status: Accepted

Concurrency strategy depends on the connection mode (Phase 11 split — see [ADR 0005](0005-implementation-phases.md)):

- **In-memory (`InMemorySqliteStore`)**: a sentinel connection keeps the database alive, and an `AsyncReaderWriterLock` (see `SqliteStores/AsyncReaderWriterLock.cs`) serializes writes while allowing concurrent reads.
- **File-based (`FileSqliteStore`)**: SQLite WAL mode handles concurrency; no in-process lock is needed.

Callers get correct behaviour regardless of which connection string they pass.
