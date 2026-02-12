using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite.Tests.Fixtures;

public abstract class DynamoDbContextFixture
    : IAsyncLifetime
{
    protected readonly DynamoDbClient client;
    protected readonly DynamoDBContext context;

    public DynamoDbContextFixture()
    {
        client = CreateClient();
        context = CreateContext(client);
    }

    protected abstract DynamoDbClient CreateClient();

    protected virtual DynamoDBContext CreateContext(DynamoDbClient c) =>
        new DynamoDBContextBuilder()
            .ConfigureContext(cfg => cfg.DisableFetchingTableMetadata = true)
            .WithDynamoDBClient(() => c)
            .Build();

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;

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
        context.Dispose();
        client.Dispose();
        return ValueTask.CompletedTask;
    }
}
