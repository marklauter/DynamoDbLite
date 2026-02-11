namespace DynamoDbLite.SqlteStores.Models;

internal sealed record ImportRow(
    string ImportArn,
    string TableName,
    string Status,
    string InputFormat,
    string InputCompression,
    string S3Bucket,
    string S3KeyPrefix,
    string TableCreationJson,
    long? ImportedCount,
    long? ProcessedCount,
    long? ProcessedBytes,
    long? ErrorCount,
    string StartTime,
    string? EndTime,
    string? FailureCode,
    string? FailureMessage,
    string? ClientToken);
