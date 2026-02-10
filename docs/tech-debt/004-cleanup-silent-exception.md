# Background TTL cleanup silently swallows exceptions

- **Area:** DynamoDbClient / TTL cleanup
- **Priority:** Medium
- **Status:** Open

## Problem

`TriggerBackgroundCleanup` launches a fire-and-forget `Task.Run` that catches and discards all exceptions. If cleanup fails (e.g., SQLite corruption, connection pool exhaustion), the failure is completely invisible. This makes diagnosing production issues difficult because expired items silently accumulate without any indication that cleanup has stopped working.

## Suggested Fix

Inject an `ILogger<DynamoDbClient>` and log caught exceptions at `Warning` level inside the catch block. This preserves the fire-and-forget pattern while making failures observable.

```csharp
private void TriggerBackgroundCleanup(string tableName) =>
    _ = Task.Run(async () =>
    {
        try { await store.CleanupExpiredItemsAsync(tableName); }
        catch (Exception ex) { logger.LogWarning(ex, "TTL cleanup failed for table {TableName}", tableName); }
    });
```

This requires adding `ILogger` to `DynamoDbClient`'s constructor and updating `DynamoDbService` DI registration.

## Code References

- `src/DynamoDbLite/DynamoDbClient.cs` — `TriggerBackgroundCleanup` method with empty catch block
- `src/DynamoDbLite/SqliteStore.cs` — `CleanupExpiredItemsAsync` method that performs the actual cleanup
- `src/DynamoDbLite/DynamoDbService.cs` — DI registration that would need `ILogger` wiring

## Notes

Low urgency for a local dev/testing tool, but important for diagnosing unexpected behavior when expired items remain visible.
