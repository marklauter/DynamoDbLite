using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.Extensions.DependencyInjection;
using System.Net;

namespace DynamoDbLite.Tests;

public sealed class BatchWriteLimitTests
{
    private const string TableName = "BatchLimitTable";

    private static string MemoryConnectionString() =>
        $"Data Source=batchlimit_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";

    // ── Option / builder configuration ───────────────────────────────────

    [Fact]
    public void MaxBatchWriteItems_Defaults_To_25()
    {
        var options = new DynamoDbLiteOptions(MemoryConnectionString());

        Assert.Equal(25, options.MaxBatchWriteItems);
    }

    [Fact]
    public void WithMaxBatchWriteItems_Returns_Builder_For_Chaining()
    {
        var builder = new DynamoDbLiteOptionsBuilder();

        var result = builder.WithMaxBatchWriteItems(50);

        Assert.Same(builder, result);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public void WithMaxBatchWriteItems_Throws_When_Less_Than_One(int value)
    {
        var builder = new DynamoDbLiteOptionsBuilder();

        _ = Assert.Throws<DynamoDbLiteConfigurationException>(() => builder.WithMaxBatchWriteItems(value));
    }

    // ── Behavior: the configured limit drives BatchWriteItem validation ──

    // Builder path (exercises WithMaxBatchWriteItems + Build): a 26-item batch that the
    // default limit of 25 would reject succeeds once the limit is raised.
    [Fact]
    public async Task Raised_Limit_Allows_Batch_Larger_Than_Default()
    {
        var ct = TestContext.Current.CancellationToken;
        using var provider = new ServiceCollection()
            .AddDynamoDbLite(o => o
                .WithConnectionString(MemoryConnectionString())
                .WithMaxBatchWriteItems(30))
            .BuildServiceProvider();
        var client = provider.GetRequiredService<IAmazonDynamoDB>();
        await CreateTableAsync(client, ct);

        var response = await client.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>> { [TableName] = Puts(26) }
        }, ct);

        Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
    }

    // Direct construction with a lowered limit: a 6-item batch the default would accept now throws.
    [Fact]
    public async Task Lowered_Limit_Rejects_Batch_Within_Default()
    {
        var ct = TestContext.Current.CancellationToken;
        var options = new DynamoDbLiteOptions(MemoryConnectionString()) { MaxBatchWriteItems = 5 };
        using var client = new DynamoDbClient(options);
        await CreateTableAsync(client, ct);

        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.BatchWriteItemAsync(new BatchWriteItemRequest
            {
                RequestItems = new Dictionary<string, List<WriteRequest>> { [TableName] = Puts(6) }
            }, ct));

        Assert.Contains("Too many items", ex.Message);
    }

    private static async Task CreateTableAsync(IAmazonDynamoDB client, CancellationToken ct) =>
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = TableName,
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

    private static List<WriteRequest> Puts(int count) =>
        [.. Enumerable.Range(1, count).Select(static i => new WriteRequest
        {
            PutRequest = new PutRequest
            {
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = $"USER#{i}" },
                    ["SK"] = new() { S = "PROFILE" }
                }
            }
        })];
}
