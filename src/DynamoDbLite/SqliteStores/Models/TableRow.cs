namespace DynamoDbLite.SqliteStores.Models;

internal sealed record TableRow(
    string TableName,
    string KeySchemaJson,
    string AttributeDefinitionsJson,
    string ProvisionedThroughputJson,
    string GlobalSecondaryIndexesJson,
    string LocalSecondaryIndexesJson,
    string CreatedAt,
    string Status,
    long ItemCount,
    long TableSizeBytes);
