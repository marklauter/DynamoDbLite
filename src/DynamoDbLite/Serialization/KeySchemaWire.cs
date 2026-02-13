namespace DynamoDbLite.Serialization;

internal sealed record KeySchemaWire(
    string AttributeName,
    string KeyType);
