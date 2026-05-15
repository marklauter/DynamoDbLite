using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class BatchParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [BackendData]
    public async Task BatchGetItem_returns_requested_items_for_existing_keys(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("batch_get");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        foreach (var pk in new[] { "user-1", "user-2" })
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = pk },
                    ["name"] = new() { S = $"name-{pk}" },
                },
            }, ct);
        }

        var response = await client.BatchGetItemAsync(new BatchGetItemRequest
        {
            RequestItems = new Dictionary<string, KeysAndAttributes>
            {
                [tableName] = new()
                {
                    Keys =
                    [
                        new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
                        new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-2" } },
                    ],
                },
            },
        }, ct);

        Assert.Equal(2, response.Responses[tableName].Count);
        var byPk = response.Responses[tableName].ToDictionary(r => r["PK"].S, r => r["name"].S);
        Assert.Equal("name-user-1", byPk["user-1"]);
        Assert.Equal("name-user-2", byPk["user-2"]);
    }

    [Theory]
    [BackendData]
    public async Task BatchWriteItem_with_put_and_delete_in_one_batch_applies_both(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("batch_putdel");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "old-1" },
                ["name"] = new() { S = "remove-me" },
            },
        }, ct);

        _ = await client.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>
            {
                [tableName] =
                [
                    new WriteRequest
                    {
                        PutRequest = new PutRequest
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                ["PK"] = new() { S = "new-1" },
                                ["name"] = new() { S = "inserted" },
                            },
                        },
                    },
                    new WriteRequest
                    {
                        DeleteRequest = new DeleteRequest
                        {
                            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "old-1" } },
                        },
                    },
                ],
            },
        }, ct);

        var deleted = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "old-1" } },
        }, ct);
        Assert.False(deleted.IsItemSet);

        var inserted = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "new-1" } },
        }, ct);
        Assert.True(inserted.IsItemSet);
        Assert.Equal("inserted", inserted.Item["name"].S);
    }

    [Theory]
    [BackendData]
    public async Task BatchWriteItem_across_two_tables_applies_each_per_table(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableA = TestTables.UniqueName("batch_a");
        var tableB = TestTables.UniqueName("batch_b");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableA), ct);
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableB), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableB,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "to-delete" },
                ["name"] = new() { S = "gone" },
            },
        }, ct);

        _ = await client.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>
            {
                [tableA] =
                [
                    new WriteRequest
                    {
                        PutRequest = new PutRequest
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                ["PK"] = new() { S = "added" },
                                ["name"] = new() { S = "in-a" },
                            },
                        },
                    },
                ],
                [tableB] =
                [
                    new WriteRequest
                    {
                        DeleteRequest = new DeleteRequest
                        {
                            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "to-delete" } },
                        },
                    },
                ],
            },
        }, ct);

        var inA = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableA,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "added" } },
        }, ct);
        Assert.True(inA.IsItemSet);
        Assert.Equal("in-a", inA.Item["name"].S);

        var inB = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableB,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "to-delete" } },
        }, ct);
        Assert.False(inB.IsItemSet);
    }
}
