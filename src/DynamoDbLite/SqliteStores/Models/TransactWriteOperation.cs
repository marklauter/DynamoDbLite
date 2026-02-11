namespace DynamoDbLite.SqliteStores.Models;

internal sealed record TransactWriteOperation(
    string TableName,
    string Pk,
    string Sk,
    double? SkNum,
    double? TtlEpoch,
    string? ItemJson,
    bool IsDelete);
