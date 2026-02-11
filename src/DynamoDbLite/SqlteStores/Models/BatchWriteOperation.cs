namespace DynamoDbLite.SqlteStores.Models;

internal sealed record BatchWriteOperation(
    string TableName,
    string Pk,
    string Sk,
    double? SkNum,
    double? TtlEpoch,
    string? ItemJson);
