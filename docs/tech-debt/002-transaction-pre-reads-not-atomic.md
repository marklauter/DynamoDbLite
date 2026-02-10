# Transaction pre-reads are not in the same SQLite transaction as writes

- **Area:** DynamoDbClient.Transactions / SqliteStore
- **Priority:** Medium
- **Status:** Won't Fix

## Problem

`TransactWriteItemsAsync` reads existing items (for condition evaluation and update expressions) via `store.GetItemAsync()`, which opens its own connection. The actual writes happen later in a separate SQLite transaction via `store.TransactWriteItemsAsync()`. A concurrent write between the pre-read and write phases could make condition checks stale, violating DynamoDB's serializable isolation guarantees.

## Suggested Fix

Move pre-reads into the same SQLite connection/transaction as writes. This would require `SqliteStore` to expose a method that opens a connection and transaction, then allows the caller to perform reads and writes on it before committing. The existing architecture avoids holding open transactions across async reads due to SQLite schema lock concerns (see MEMORY.md build notes).

## Code References

- `src/DynamoDbLite/DynamoDbClient.Transactions.cs:96` — pre-read phase uses `store.GetItemAsync()` (separate connections)
- `src/DynamoDbLite/DynamoDbClient.Transactions.cs:230` — write phase uses `store.TransactWriteItemsAsync()` (single transaction)
- `src/DynamoDbLite/SqliteStore.cs:252` — `GetItemAsync` opens its own connection

## Notes

Marked "Won't Fix" because:
1. For a local test emulator, concurrent transaction conflicts are rare — most tests run sequentially against a single client.
2. SQLite's WAL mode serializes writes at the database level, so the window for stale reads is narrow.
3. Fixing this requires rearchitecting how `SqliteStore` manages connection/transaction lifetimes, which would ripple across the entire codebase. The cost outweighs the benefit for the project's use case.
