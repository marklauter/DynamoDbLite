namespace DynamoDbLite.SqlteStores.Models;

internal sealed record KeySchemaRow(
    string KeySchemaJson,
    string AttributeDefinitionsJson);
