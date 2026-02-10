# Split SqliteStore into in-memory and file-based implementations

- **Area:** SqliteStore
- **Priority:** Medium
- **Status:** Done

## Problem

`SqliteStore` serves both in-memory (`Mode=Memory;Cache=Shared`) and file-based databases but they have fundamentally different concurrency characteristics. File-based uses WAL for concurrent readers/writers; in-memory shared-cache uses table-level locking where a write transaction blocks reads from other connections. SQLite does not support WAL on in-memory databases.

Today this is papered over with a gotcha doc entry ("don't open a new connection while a transaction is active on another") instead of being handled by the store itself. Callers shouldn't need to know how the store implements concurrency.

## Suggested Fix

Introduce an interface (or abstract base class) with two implementations:

- **File-based store** — uses WAL, no additional synchronization needed
- **In-memory store** — uses a `SemaphoreSlim` to serialize concurrent access, making it safe without WAL

A factory method or constructor on `DynamoDbClient` picks the right implementation based on the connection string. Callers get identical behavior regardless of mode.

## Code References

- `src/DynamoDbLite/SqliteStore.cs` — constructor with `isMemory` branching
- `src/DynamoDbLite/SqliteStore.cs` — WAL skipped for in-memory
- `src/DynamoDbLite/SqliteStore.cs` — WAL guard in `OpenConnectionAsync`
- `.claude/gotchas.md` — workaround guidance that should become unnecessary

## Notes

Low urgency — current usage is single-threaded async in tests, so the locking issue hasn't manifested. Becomes important if the store is used with concurrent access.
