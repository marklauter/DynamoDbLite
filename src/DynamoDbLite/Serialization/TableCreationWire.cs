namespace DynamoDbLite.Serialization;

internal sealed record TableCreationWire(
    string TableName,
    KeySchemaWire[] KeySchema,
    AttributeDefinitionWire[] AttributeDefinitions,
    ProvisionedThroughputWire? ProvisionedThroughput,
    GsiWire[]? GlobalSecondaryIndexes);
