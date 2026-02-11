namespace DynamoDbLite.SqlteStores.Models;

internal sealed record ExportRow(
    string ExportArn,
    string TableName,
    string Status,
    string ExportFormat,
    string S3Bucket,
    string S3Prefix,
    string? ExportManifest,
    long? ItemCount,
    long? BilledSize,
    string StartTime,
    string? EndTime,
    string? FailureCode,
    string? FailureMessage,
    string? ClientToken);
