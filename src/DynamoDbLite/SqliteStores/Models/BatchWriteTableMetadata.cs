namespace DynamoDbLite.SqliteStores.Models;

internal sealed record BatchWriteTableMetadata(
    KeySchemaInfo KeyInfo,
    string? TtlAttributeName,
    List<IndexDefinition> Indexes);
