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

    // GUID-suffixed per-fixture-instance, which (because xUnit instantiates one fixture per [Fact])
    // means a unique name per test method. The DB itself is already isolated per fixture; the unique
    // table name is defense in depth and removes "did this leak from somewhere" as a debug question.
    protected string TestTableName { get; } = $"TestTable_{Guid.NewGuid():N}";
    protected string SecondTableName { get; } = $"SecondTable_{Guid.NewGuid():N}";

    public DynamoDbClient Client(StoreType st) =>
        st switch
        {
            StoreType.DdbLiteFile => fbf.Client,
            StoreType.DdbLite => imf.Client,
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

    protected async Task CreateTestTableAsync(DynamoDbClient client, CancellationToken ct) =>
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = TestTableName,
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

    protected async Task CreateSecondTestTableAsync(DynamoDbClient client, CancellationToken ct) =>
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = SecondTableName,
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
