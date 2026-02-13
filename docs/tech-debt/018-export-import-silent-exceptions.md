# Export/import background tasks swallow exceptions silently

- **Area:** Export & Import (DynamoDbClient.DataPipeline.cs)
- **Type:** Observability
- **Priority:** Low
- **Status:** Open

## Problem

`ExecuteExportAsync` and `ExecuteImportAsync` run via fire-and-forget `Task.Run`. Exceptions are caught and the status is updated to `FAILED` in the database, but no logging or notification occurs. If the status update itself fails (e.g., database connection issue), the exception is completely lost. This is the same pattern documented in 004 for TTL background cleanup.

## Suggested Fix

1. Add logging in the catch blocks so failures are observable without polling
2. Consider wrapping the outer `Task.Run` lambda in a second try-catch to handle failures in the status update itself
3. Could be addressed alongside 004 as part of a broader "add logging to fire-and-forget tasks" effort

## Code References

- `src/DynamoDbLite/DynamoDbClient.DataPipeline.cs:37` — `Task.Run(() => ExecuteExportAsync(...))`
- `src/DynamoDbLite/DynamoDbClient.DataPipeline.cs:77-82` — export catch block updates DB but no logging
- `src/DynamoDbLite/DynamoDbClient.DataPipeline.cs:176` — `Task.Run(() => ExecuteImportAsync(...))`
- `src/DynamoDbLite/DynamoDbClient.DataPipeline.cs:242-247` — import catch block updates DB but no logging
- `docs/tech-debt/004-cleanup-silent-exception.md` — same pattern in TTL cleanup

## Notes

Low priority since users can poll `DescribeExportAsync`/`DescribeImportAsync` to discover failures. Best fixed together with 004 when a logging strategy is adopted for the project.
