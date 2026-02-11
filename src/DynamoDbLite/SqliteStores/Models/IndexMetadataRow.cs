namespace DynamoDbLite.SqliteStores.Models;

internal sealed record IndexMetadataRow(
    string GlobalSecondaryIndexesJson,
    string LocalSecondaryIndexesJson);
