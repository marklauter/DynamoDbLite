namespace DynamoDbLite.Tests.Fixtures;

internal sealed class InMemoryDynamoFactory
    : DynamoDbContextFactory
{
    protected override DynamoDbClient CreateClient() =>
        new(new DynamoDbLiteOptions($"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));
}
