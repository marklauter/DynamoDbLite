namespace DynamoDbLite.Tests.DynamoDbContext;

public sealed class InMemoryDynamoDbContextCollectionTests
    : DynamoDbContextCollectionTests
{
    protected override DynamoDbClient CreateClient() =>
        new(new DynamoDbLiteOptions($"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));
}
