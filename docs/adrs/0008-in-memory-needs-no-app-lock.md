---
title: In-memory store needs no in-process lock
summary: The in-memory store drops the AsyncReaderWriterLock; SQLite plus the Microsoft.Data.Sqlite retry serialize concurrent writers. Supersedes ADR 0003.
document:
  tags: [adr, concurrency, sqlite]
  status: accepted
edge.supersedes: [docs/adrs/0003-concurrency-strategy.md]
---

# ADR 0008 — In-memory store needs no in-process lock

Status: Accepted (supersedes [ADR 0003](0003-concurrency-strategy.md))

The in-memory store no longer holds an application-level lock. `InMemorySqliteStore` previously wrapped every store call in an `AsyncReaderWriterLock` to serialize writes. Experiments showed the lock is redundant.

## Why

With the store's connection-per-operation model and Microsoft.Data.Sqlite's default `BEGIN IMMEDIATE` transactions, concurrent writers to the shared in-memory database serialize through the driver's own retry loop, bounded by `CommandTimeout` (default 30 s). A run of 800 concurrent writes across 16 tasks completed with zero failures and no lock.

`busy_timeout` is inert for this store: sharing one in-memory database across connections requires shared cache, and shared-cache conflicts raise `SQLITE_LOCKED_SHAREDCACHE`, which the busy handler ignores. The driver retry, not the pragma, clears contention.

## Consequence

Both store modes rely on SQLite plus the driver for concurrency; neither holds an in-process lock. The trade-off: the driver serializes by a blocking retry rather than an async wait, and a single write held past `CommandTimeout` fails rather than queuing — outside the target workload of local development, tests, and mobile.

See [the journal entry](../journal/2026-06-01-1753-in-memory-store-needs-no-app-level-write-lock.md) for the experiments.
