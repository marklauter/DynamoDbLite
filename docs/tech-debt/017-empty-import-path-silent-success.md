# Empty import path silently succeeds

- **Area:** Export & Import (DynamoDbClient.DataPipeline.cs)
- **Priority:** Medium
- **Status:** Open

## Problem

When `ImportTableAsync` launches the background import task, `ExecuteImportAsync` calls `ExportHelper.FindDataFiles` to locate data files. If the provided S3 bucket path does not exist or contains no data files, `FindDataFiles` returns an empty list. The import then completes with `importedCount = 0` and status `COMPLETED`, giving no indication that something went wrong. A user who points at the wrong directory will see a successful import with zero items and no error.

## Suggested Fix

1. After `FindDataFiles` returns, check if the result is empty
2. If empty, set import status to `FAILED` with failure message: "No data files found at the specified S3 location"
3. Optionally, validate that the base path directory exists before returning `IN_PROGRESS` from `ImportTableAsync` to fail fast on obviously wrong paths

## Code References

- `src/DynamoDbLite/DynamoDbClient.DataPipeline.cs:204` — `FindDataFiles` call with no empty-result check
- `src/DynamoDbLite/ExportHelper.cs` — `FindDataFiles` returns empty list for missing/empty paths

## Notes

Real DynamoDB would fail the import if the S3 location contains no valid data files. Matching that behavior improves developer experience when debugging import issues.
