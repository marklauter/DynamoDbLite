namespace DynamoDbLite.Serialization;

internal sealed record GsiWire(
    string IndexName,
    KeySchemaWire[] KeySchema,
    ProjectionWire? Projection);
