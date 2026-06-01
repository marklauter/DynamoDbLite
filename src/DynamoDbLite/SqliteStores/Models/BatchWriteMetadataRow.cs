namespace DynamoDbLite.SqliteStores.Models;

internal sealed record BatchWriteMetadataRow(
    string KeySchemaJson,
    string AttributeDefinitionsJson,
    string GlobalSecondaryIndexesJson,
    string LocalSecondaryIndexesJson,
    string? TtlAttributeName);
