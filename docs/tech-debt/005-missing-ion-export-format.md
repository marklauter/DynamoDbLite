# Missing ION export/import format

- **Area:** Export & Import (DynamoDbClient.DataPipeline.cs, ExportHelper.cs)
- **Type:** Feature Gap
- **Priority:** Low
- **Status:** Open

## Problem

DynamoDB supports two export formats: `DYNAMODB_JSON` and `ION`. The current implementation only supports `DYNAMODB_JSON`. Requests using `ION` format are rejected with an exception. This limits compatibility with workflows that use Amazon Ion as their serialization format.

## Suggested Fix

1. Add an Amazon Ion serialization library (e.g., `Amazon.IonDotnet`)
2. Implement `WriteDataFilesIonAsync` in `ExportHelper.cs` that serializes items in Ion text format
3. Implement a corresponding `ReadIonDataFile` method for import
4. Update `ExecuteExportAsync` and `ExecuteImportAsync` to branch on the format parameter
5. Remove the format validation rejections in `ExportTableToPointInTimeAsync` and `ImportTableAsync`

## Code References

- `src/DynamoDbLite/DynamoDbClient.DataPipeline.cs` — format validation and export/import execution
- `src/DynamoDbLite/ExportHelper.cs` — file I/O utilities for data files

## Notes

Low priority since `DYNAMODB_JSON` is the most commonly used format for local development and testing. Ion support is primarily needed for compatibility with AWS Athena and other analytics services.
