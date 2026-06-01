---
title: The in-memory store needs no app-level write lock
summary: Microsoft.Data.Sqlite's own retry serializes concurrent writers to the shared in-memory database, so the AsyncReaderWriterLock is redundant; busy_timeout is inert for shared-cache locks.
document:
  tags: [journal, sqlite, concurrency, performance, decision]
  created: 2026-06-01
edge.supersedes: [docs/adrs/0003-concurrency-strategy.md]
---

# The in-memory store needs no app-level write lock

Microsoft.Data.Sqlite's own retry serializes concurrent writers to the shared in-memory database, so the `AsyncReaderWriterLock` is redundant; `busy_timeout` is inert for shared-cache locks.

## Context

The investigation opened as an efficiency review of `DynamoDbClient.BatchWriteItemAsync`. Costing each store call exposed that `InMemorySqliteStore` wraps every call in an `AsyncReaderWriterLock` per [[docs/adrs/0003-concurrency-strategy.md]]. That raised the question: does the in-memory store need an application-level lock, or do SQLite and the Microsoft.Data.Sqlite driver already serialize concurrent writers? Two things were open going in — whether concurrent writers fail without the lock, and whether `busy_timeout` governs that behavior.

## Attempted

I built a standalone repro on Microsoft.Data.Sqlite 10.0.2 (the library's pinned version) against a true shared in-memory database — `Data Source=...;Mode=Memory;Cache=Shared`, kept alive by a sentinel connection. Three experiments:

1. Two connections, a transaction open on each, overlapping.
2. A holder that takes the write lock and releases after 1.5 s, with a second writer set first to `busy_timeout=0` and then to `busy_timeout=20000`.
3. 16 tasks running 50 connection-per-op writes each, using the default `BeginTransactionAsync` — the same call `SqliteStore` makes — with no application lock.

Hypothesis under test: the lock exists to stop concurrent writers from hitting `SQLITE_LOCKED`.

## Outcome

Observation: concurrent write conflicts surface as `SQLITE_LOCKED_SHAREDCACHE` (primary 6, extended 262). Sharing one in-memory database across connections requires shared-cache, and shared-cache uses table-level locks.

Observation: `busy_timeout` changes nothing for that lock class. With the holder releasing at 1.5 s, the waiter cleared at ~1.4 s under both `busy_timeout=0` and `busy_timeout=20000`. Inference: the busy handler ignores shared-cache table locks, so the pragma stays inert for the in-memory store. The thing serializing the writers is Microsoft.Data.Sqlite's own retry loop, bounded by `CommandTimeout` (default 30 s).

Observation: 800 concurrent writes (16 tasks × 50), connection-per-op, default `BEGIN IMMEDIATE` transactions, no application lock — zero failures, slowest single write 2 ms, 26 ms wall. The writers serialize themselves.

Observation: Microsoft.Data.Sqlite's default `BeginTransactionAsync()` runs at `Serializable`, which it implements as `BEGIN IMMEDIATE` — it takes the write lock at BEGIN, not at first write. `SqliteStore` calls that default, so two concurrent write transactions collide the instant the second one starts, and the loser retries until it wins or times out.

Correction: an earlier claim in the same session — "one connection cannot overlap transactions, so the lock is needed" — conflated single-connection nested transactions with the two-connection model the library uses. Two connections each open a deferred transaction without conflict; the collision is a write-lock contest, not a nesting error.

Related finding, separate cycle: the `SqliteStore` constructor overwrote a caller's `Mode=Memory` with `Mode=ReadWriteCreate`, so the in-memory store ran file-backed and left `Test_*` and `Parity_*` database files in the test output directories. Fixed by collapsing the builder to `builder.ForeignKeys ??= true;`. See [[ghost/mode-memory-clobber-made-in-memory-file-backed]].

## Decision

Remove the `AsyncReaderWriterLock` from `InMemorySqliteStore` and rely on SQLite plus the driver retry, matching `FileSqliteStore`. The lock is redundant for correctness — 800 concurrent writes pass without it. The only behavior it added over driver-retry was asynchronous waiting in place of a blocking retry, and avoiding a hard throw when a single operation exceeds `CommandTimeout`. At the target write concurrency — local development, tests, mobile — both are negligible. This supersedes [[docs/adrs/0003-concurrency-strategy.md]].

Trade-off accepted: driver-retry serializes by blocking the calling thread during the retry rather than awaiting it. A single write that holds the lock past the 30 s `CommandTimeout` hard-fails rather than queuing — a pathological case, outside the target workload.

## Next

Rip out the `rwLock` field, the two `Acquire*LockAsync` overrides, and their disposal from `InMemorySqliteStore`; update [[docs/adrs/0003-concurrency-strategy.md]]; run the batch and parity tests. `busy_timeout` belongs on the file path if anywhere, since file-lock conflicts return `SQLITE_BUSY` (5), which the busy handler does honor.
