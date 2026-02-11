namespace DynamoDbLite.SqliteStores.Models;

internal sealed record ImportSummaryRow(
    string ImportArn,
    string TableName,
    string Status,
    string S3Bucket,
    string S3KeyPrefix,
    string InputFormat,
    string StartTime,
    string? EndTime);
