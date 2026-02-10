using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Testcontainers.DynamoDb;

namespace DynamoDbLite.Tests.Fixtures;

public class DynamoDbFixture
    : IAsyncLifetime
{
    private readonly DynamoDbContainer container =
        new DynamoDbBuilder("amazon/dynamodb-local:latest")
            .WithAutoRemove(true)
            .WithCleanUp(true)
            .Build();

    private AmazonDynamoDBClient? client;

    public IAmazonDynamoDB Client => client ?? throw new InvalidOperationException("Fixture not initialized.");

    protected virtual CreateTableRequest CreateTableRequest() => new()
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
        ],
        BillingMode = BillingMode.PAY_PER_REQUEST
    };

    public async ValueTask InitializeAsync()
    {
        await container.StartAsync();

        var credentials = new BasicAWSCredentials("test", "test");
        var config = new AmazonDynamoDBConfig { ServiceURL = container.GetConnectionString() };
        client?.Dispose();
        client = new AmazonDynamoDBClient(credentials, config);

        var request = CreateTableRequest();
        _ = await Client.CreateTableAsync(request);
        await WaitForTableActiveAsync(request.TableName);
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeAsyncCore();
        GC.SuppressFinalize(this);
    }

    protected virtual async ValueTask DisposeAsyncCore()
    {
        client?.Dispose();
        await container.DisposeAsync();
    }

    private async Task WaitForTableActiveAsync(string tableName)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        while (!cts.Token.IsCancellationRequested)
        {
            var response = await Client.DescribeTableAsync(tableName, cts.Token);
            if (response.Table.TableStatus == TableStatus.ACTIVE)
                return;

            await Task.Delay(200, cts.Token);
        }
    }
}
