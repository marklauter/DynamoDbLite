---
title: Concurrency Strategy
type: decision
summary: "Concurrency strategy depends on the connection mode: an in-process reader-writer lock for the in-memory store, WAL for file-based. Superseded on the in-memory half only — that store no longer holds an in-process lock."
tags: [concurrency, sqlite]
created: 2026-05-16
status: superseded
superseded-by: "[[in-memory-needs-no-app-lock]]"
---

# Concurrency Strategy

Superseded by [In-memory store needs no in-process lock](in-memory-needs-no-app-lock.md). The file-based half below still stands; only the in-memory store changed, dropping its `AsyncReaderWriterLock`.

Concurrency strategy depends on the connection mode (see the Phase 11 split in [Implementation Phases](implementation-phases.md)):

- **In-memory (`InMemorySqliteStore`)**: a sentinel connection keeps the database alive, and an `AsyncReaderWriterLock` (see `SqliteStores/AsyncReaderWriterLock.cs`) serializes writes while allowing concurrent reads.
- **File-based (`FileSqliteStore`)**: SQLite WAL mode handles concurrency; no in-process lock is needed.

Callers get correct behavior regardless of which connection string they pass.
