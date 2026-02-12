namespace DynamoDbLite.Tests.DynamoDbContext;

public sealed class InMemoryDynamoDbContextBatchGetTests
    : DynamoDbContextBatchGetTests
{
    protected override DynamoDbClient CreateClient() =>
        new(new DynamoDbLiteOptions($"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));
}
