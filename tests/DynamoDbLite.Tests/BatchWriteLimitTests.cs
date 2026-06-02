using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.SqliteStores;
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

    // Multi-chunk bulk-upsert path: a batch larger than MaxUpsertRowsPerChunk is split into several
    // multi-row INSERTs. Verifies rows on both sides of the chunk boundary persist (in-memory only —
    // the chunk logic is store-agnostic and the small batch tests already cover the file-backed store).
    [Fact]
    public async Task Batch_Spanning_Multiple_Upsert_Chunks_Persists_All_Rows()
    {
        var ct = TestContext.Current.CancellationToken;
        var count = SqliteStore.MaxUpsertRowsPerChunk + 5;

        using var provider = new ServiceCollection()
            .AddDynamoDbLite(o => o
                .WithConnectionString(MemoryConnectionString())
                .WithMaxBatchWriteItems(count))
            .BuildServiceProvider();
        var client = provider.GetRequiredService<IAmazonDynamoDB>();
        await CreateTableAsync(client, ct);

        var response = await client.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>> { [TableName] = Puts(count) }
        }, ct);

        Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);

        // First/last of chunk 0 and first/last of chunk 1 — proves the split wrote both chunks.
        foreach (var i in new[] { 1, SqliteStore.MaxUpsertRowsPerChunk, SqliteStore.MaxUpsertRowsPerChunk + 1, count })
        {
            var get = await client.GetItemAsync(new GetItemRequest
            {
                TableName = TableName,
                Key = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = $"USER#{i}" },
                    ["SK"] = new() { S = "PROFILE" }
                }
            }, ct);

            Assert.True(get.IsItemSet, $"row USER#{i} should have persisted");
        }
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
