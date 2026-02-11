namespace DynamoDbLite.SqliteStores.Models;

internal sealed record KeySchemaRow(
    string KeySchemaJson,
    string AttributeDefinitionsJson);
