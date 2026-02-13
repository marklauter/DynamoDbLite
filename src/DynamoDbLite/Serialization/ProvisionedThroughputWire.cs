namespace DynamoDbLite.Serialization;

internal sealed record ProvisionedThroughputWire(
    long? ReadCapacityUnits,
    long? WriteCapacityUnits);
