namespace DynamoDbLite.Serialization;

internal sealed record IndexDefinitionWire(
    string IndexName,
    bool IsGlobal,
    KeySchemaWire[] KeySchema,
    string ProjectionType,
    List<string>? NonKeyAttributes);
