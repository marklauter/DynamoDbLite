namespace DynamoDbLite.Tests.Fixtures;

internal sealed class InMemoryDynamoFactory
    : DynamoDbContextFactory
{
    public InMemoryDynamoFactory()
        : base(() => new DynamoDbClient(new DynamoDbLiteOptions($"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared")))
    {
    }
}
