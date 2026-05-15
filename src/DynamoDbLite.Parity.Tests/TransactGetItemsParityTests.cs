using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class TransactGetItemsParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task TransactGetItems_returns_items_across_two_tables_in_request_order(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableA = TestTables.UniqueName("tget_a");
        var tableB = TestTables.UniqueName("tget_b");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableA), ct);
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableB), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableA,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "a1" },
                ["label"] = new() { S = "from-a" },
            },
        }, ct);
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableB,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "b1" },
                ["label"] = new() { S = "from-b" },
            },
        }, ct);
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableA,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "a2" },
                ["label"] = new() { S = "from-a-2" },
            },
        }, ct);

        var response = await client.TransactGetItemsAsync(new TransactGetItemsRequest
        {
            TransactItems =
            [
                new TransactGetItem
                {
                    Get = new Get
                    {
                        TableName = tableA,
                        Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "a1" } },
                    },
                },
                new TransactGetItem
                {
                    Get = new Get
                    {
                        TableName = tableB,
                        Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "b1" } },
                    },
                },
                new TransactGetItem
                {
                    Get = new Get
                    {
                        TableName = tableA,
                        Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "a2" } },
                    },
                },
            ],
        }, ct);

        Assert.Equal(3, response.Responses.Count);
        Assert.Equal("from-a", response.Responses[0].Item["label"].S);
        Assert.Equal("from-b", response.Responses[1].Item["label"].S);
        Assert.Equal("from-a-2", response.Responses[2].Item["label"].S);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task TransactGetItems_with_missing_key_returns_empty_item_at_that_index(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("tget_miss");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "exists" },
                ["label"] = new() { S = "found" },
            },
        }, ct);

        var response = await client.TransactGetItemsAsync(new TransactGetItemsRequest
        {
            TransactItems =
            [
                new TransactGetItem
                {
                    Get = new Get
                    {
                        TableName = tableName,
                        Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "exists" } },
                    },
                },
                new TransactGetItem
                {
                    Get = new Get
                    {
                        TableName = tableName,
                        Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "ghost" } },
                    },
                },
            ],
        }, ct);

        Assert.Equal(2, response.Responses.Count);
        Assert.Equal("found", response.Responses[0].Item["label"].S);
        Assert.True(response.Responses[1].Item == null || response.Responses[1].Item.Count == 0);
    }
}
