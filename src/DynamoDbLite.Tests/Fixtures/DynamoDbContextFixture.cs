using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using System.Diagnostics.CodeAnalysis;

namespace DynamoDbLite.Tests.Fixtures;

[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "required for test")]
public class DynamoDbContextFixture
    : IAsyncLifetime
{
    private readonly InMemoryDynamoFactory imf = new();
    private readonly FileBasedDynamoFactory fbf = new();

    public DynamoDbClient Client(StoreType st)
        => st switch
        {
            StoreType.FileBased => fbf.Client,
            StoreType.MemoryBased => imf.Client,
            _ => throw new ArgumentOutOfRangeException(nameof(st), st, null)
        };

    public DynamoDBContext Context(StoreType st)
        => st switch
        {
            StoreType.FileBased => fbf.Context,
            StoreType.MemoryBased => imf.Context,
            _ => throw new ArgumentOutOfRangeException(nameof(st), st, null)
        };

    public async ValueTask InitializeAsync()
    {
        await CreateTables(imf.Client, TestContext.Current.CancellationToken);
        await CreateTables(fbf.Client, TestContext.Current.CancellationToken);
    }

    private static async Task CreateTables(DynamoDbClient client, CancellationToken ct)
    {
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "SimpleItems",
            KeySchema = [new KeySchemaElement("Id", KeyType.HASH)],
            AttributeDefinitions = [new AttributeDefinition("Id", ScalarAttributeType.S)],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        }, ct);

        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "CompositeItems",
            KeySchema =
            [
                new KeySchemaElement("PK", KeyType.HASH),
                new KeySchemaElement("SK", KeyType.RANGE),
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition("PK", ScalarAttributeType.S),
                new AttributeDefinition("SK", ScalarAttributeType.S),
            ],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        }, ct);

        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "NumericKeyItems",
            KeySchema =
            [
                new KeySchemaElement("Category", KeyType.HASH),
                new KeySchemaElement("OrderNumber", KeyType.RANGE),
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition("Category", ScalarAttributeType.S),
                new AttributeDefinition("OrderNumber", ScalarAttributeType.N),
            ],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        }, ct);

        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "CollectionItems",
            KeySchema = [new KeySchemaElement("Id", KeyType.HASH)],
            AttributeDefinitions = [new AttributeDefinition("Id", ScalarAttributeType.S)],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        }, ct);

        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "GsiItems",
            KeySchema =
            [
                new KeySchemaElement("PK", KeyType.HASH),
                new KeySchemaElement("SK", KeyType.RANGE),
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition("PK", ScalarAttributeType.S),
                new AttributeDefinition("SK", ScalarAttributeType.S),
                new AttributeDefinition("GsiPK", ScalarAttributeType.S),
                new AttributeDefinition("GsiSK", ScalarAttributeType.S),
            ],
            GlobalSecondaryIndexes =
            [
                new GlobalSecondaryIndex
                {
                    IndexName = "GsiIndex",
                    KeySchema =
                    [
                        new KeySchemaElement("GsiPK", KeyType.HASH),
                        new KeySchemaElement("GsiSK", KeyType.RANGE),
                    ],
                    Projection = new Projection { ProjectionType = ProjectionType.ALL },
                },
            ],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        }, ct);

        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "VersionedItems",
            KeySchema = [new KeySchemaElement("Id", KeyType.HASH)],
            AttributeDefinitions = [new AttributeDefinition("Id", ScalarAttributeType.S)],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        }, ct);

        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "EnumItems",
            KeySchema = [new KeySchemaElement("Id", KeyType.HASH)],
            AttributeDefinitions = [new AttributeDefinition("Id", ScalarAttributeType.S)],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        }, ct);
    }

    public virtual ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        imf.Dispose();
        fbf.Dispose();
        return ValueTask.CompletedTask;
    }
}
