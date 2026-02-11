using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite.SqlteStores.Models;

internal sealed record IndexDefinition(
    string IndexName,
    bool IsGlobal,
    List<KeySchemaElement> KeySchema,
    string ProjectionType,
    List<string>? NonKeyAttributes);
