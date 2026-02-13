namespace DynamoDbLite.Serialization;

internal sealed record ProjectionWire(
    string? ProjectionType,
    List<string>? NonKeyAttributes);
