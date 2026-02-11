namespace DynamoDbLite.SqlteStores.Models;

internal sealed record IndexMetadataRow(
    string GlobalSecondaryIndexesJson,
    string LocalSecondaryIndexesJson);
