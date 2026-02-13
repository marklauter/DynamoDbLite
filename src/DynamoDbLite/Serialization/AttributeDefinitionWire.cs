namespace DynamoDbLite.Serialization;

internal sealed record AttributeDefinitionWire(
    string AttributeName,
    string AttributeType);
