using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

// The TTL sweep runs on the read path, awaited. Reclamation is never load-bearing — reads filter
// expired rows by ttl_epoch whether or not the sweep ran — so a sweep that fails must not fail the
// read that triggered it. These pin that, using a store that fails only the sweep.
public sealed class TtlSweepFailureTests
{
    private const string TableName = "SweepFailureTable";

    private static Task<DynamoDbClient> CreateClientWithFailingSweepAsync() =>
        CreateClientWithFailingSweepAsync(null);

    private static async Task<DynamoDbClient> CreateClientWithFailingSweepAsync(Func<Exception>? sweepFailure)
    {
        var options = new DynamoDbLiteOptions($"Data Source=sweepfail_{Guid.NewGuid():N};Mode=Memory;Cache=Shared");
        var client = new DynamoDbClient(o => new CleanupFailingStore(o, sweepFailure), options);

        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = TableName,
            KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
            AttributeDefinitions = [new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }]
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = TableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["Data"] = new() { S = "value1" }
            }
        }, TestContext.Current.CancellationToken);

        return client;
    }

    [Fact]
    public async Task GetItem_Succeeds_When_Ttl_Sweep_Fails()
    {
        using var client = await CreateClientWithFailingSweepAsync();

        var response = await client.GetItemAsync(TableName,
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "pk1" } },
            TestContext.Current.CancellationToken);

        Assert.True(response.IsItemSet);
        Assert.Equal("value1", response.Item["Data"].S);
    }

    [Fact]
    public async Task Scan_Succeeds_When_Ttl_Sweep_Fails()
    {
        using var client = await CreateClientWithFailingSweepAsync();

        var response = await client.ScanAsync(
            new ScanRequest { TableName = TableName }, TestContext.Current.CancellationToken);

        Assert.Equal(1, response.Count);
    }

    [Fact]
    public async Task Query_Succeeds_When_Ttl_Sweep_Fails()
    {
        using var client = await CreateClientWithFailingSweepAsync();

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = TableName,
            KeyConditionExpression = "PK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = "pk1" } }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(1, response.Count);
    }

    // Cancellation is the one sweep exception that must not be swallowed. The sweep runs inside the
    // caller's operation and shares its token, so a cancelled sweep means a cancelled request — the
    // filter on the catch exists to keep that distinct from a sweep that genuinely failed.
    [Fact]
    public async Task GetItem_Propagates_Cancellation_From_The_Sweep()
    {
        using var client = await CreateClientWithFailingSweepAsync(
            static () => new OperationCanceledException("sweep observed cancellation"));

        _ = await Assert.ThrowsAsync<OperationCanceledException>(() =>
            client.GetItemAsync(TableName,
                new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "pk1" } },
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Query_Propagates_Cancellation_From_The_Sweep()
    {
        using var client = await CreateClientWithFailingSweepAsync(
            static () => new OperationCanceledException("sweep observed cancellation"));

        _ = await Assert.ThrowsAsync<OperationCanceledException>(() =>
            client.QueryAsync(new QueryRequest
            {
                TableName = TableName,
                KeyConditionExpression = "PK = :pk",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = "pk1" } }
            }, TestContext.Current.CancellationToken));
    }
}
