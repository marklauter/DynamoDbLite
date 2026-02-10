# Import compression format stored but not applied

- **Area:** Export & Import (DynamoDbClient.DataPipeline.cs, SqliteStore.cs)
- **Priority:** Low
- **Status:** Open

## Problem

`ImportTableAsync` captures the `InputCompressionType` (GZIP, ZSTD) from the request and persists it to the `imports` SQLite table, but `ExecuteImportAsync` never reads or applies it. All imported files are assumed to be uncompressed JSON lines. Providing a compressed file would cause JSON parse errors with no indication that the compression type was ignored.

## Suggested Fix

1. In `ExecuteImportAsync`, read the stored compression type before processing data files
2. For GZIP: wrap `File.OpenRead` in `GZipStream` before reading lines
3. For ZSTD: add a ZStandard decompression dependency or reject with a clear error
4. If a compression type is unsupported, fail the import with an explicit message rather than a parse error

## Code References

- `src/DynamoDbLite/DynamoDbClient.DataPipeline.cs:154` — compression type captured from request
- `src/DynamoDbLite/DynamoDbClient.DataPipeline.cs:195-248` — `ExecuteImportAsync` ignores compression
- `src/DynamoDbLite/SqliteStore.cs:111` — `input_compression` column in imports table

## Notes

Similar in spirit to 005 (missing ION format). Both are "stored but not implemented" feature gaps. GZIP is the most common compression type and would be the highest-value addition.
