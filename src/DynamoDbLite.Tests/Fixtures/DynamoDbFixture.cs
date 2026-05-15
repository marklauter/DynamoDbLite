using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using System.Diagnostics.CodeAnalysis;
using Testcontainers.DynamoDb;

namespace DynamoDbLite.Tests.Fixtures;

[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "required for xUnit collection fixture injection")]
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

        _ = await Client.CreateTableAsync(CreateTableRequest());
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
}
