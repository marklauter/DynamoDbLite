using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using System.Diagnostics.CodeAnalysis;

namespace DynamoDbLite.Tests.Fixtures;

[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "required for test")]
[SuppressMessage("Design", "CA1062:Validate arguments of public methods", Justification = "test helpers")]
public class DynamoDbClientFixture
    : IAsyncLifetime
{
    private readonly InMemoryDynamoFactory imf = new();
    private readonly FileBasedDynamoFactory fbf = new();

    public DynamoDbClient Client(StoreType st) =>
        st switch
        {
            StoreType.FileBased => fbf.Client,
            StoreType.MemoryBased => imf.Client,
            _ => throw new ArgumentOutOfRangeException(nameof(st), st, null),
        };

    protected virtual ValueTask SetupAsync(CancellationToken ct) => ValueTask.CompletedTask;

    public ValueTask InitializeAsync() => SetupAsync(TestContext.Current.CancellationToken);

    public virtual ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        imf.Dispose();
        fbf.Dispose();
        return ValueTask.CompletedTask;
    }

    protected static async Task CreateTestTableAsync(DynamoDbClient client, CancellationToken ct) =>
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "TestTable",
            KeySchema =
            [
                new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S }
            ]
        }, ct);

    protected static async Task CreateHashOnlyTableAsync(DynamoDbClient client, string tableName, CancellationToken ct) =>
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = tableName,
            KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
            AttributeDefinitions = [new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }]
        }, ct);

    protected static async Task CreateNumericSortKeyTableAsync(DynamoDbClient client, string tableName, CancellationToken ct) =>
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = tableName,
            KeySchema =
            [
                new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.N }
            ]
        }, ct);
}
