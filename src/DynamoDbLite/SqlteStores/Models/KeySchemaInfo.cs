using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite.SqlteStores.Models;

internal sealed record KeySchemaInfo(
    List<KeySchemaElement> KeySchema,
    List<AttributeDefinition> AttributeDefinitions);
