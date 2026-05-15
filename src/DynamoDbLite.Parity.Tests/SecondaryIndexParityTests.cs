using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class SecondaryIndexParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [BackendData]
    public async Task Query_on_GSI_with_INCLUDE_projection_returns_projected_attributes_only(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("gsi_include");
        const string indexName = "GsiIndex";
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyStringSortKeyStringWithGsi(tableName, indexName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "X" },
                ["GsiPK"] = new() { S = "GROUP#A" },
                ["GsiSK"] = new() { S = "1" },
                ["projected"] = new() { S = "visible" },
                ["secret"] = new() { S = "hidden" },
            },
        }, ct);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = tableName,
            IndexName = indexName,
            KeyConditionExpression = "GsiPK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = "GROUP#A" } },
        }, ct);

        Assert.Equal(1, response.Count);
        var item = response.Items[0];
        Assert.True(item.ContainsKey("PK"));
        Assert.True(item.ContainsKey("SK"));
        Assert.True(item.ContainsKey("GsiPK"));
        Assert.True(item.ContainsKey("GsiSK"));
        Assert.True(item.ContainsKey("projected"));
        Assert.Equal("visible", item["projected"].S);
        Assert.False(item.ContainsKey("secret"));
    }

    [Theory]
    [BackendData]
    public async Task Query_on_GSI_with_KEYS_ONLY_projection_returns_only_table_and_index_keys(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("gsi_keys");
        const string indexName = "GsiIndex";
        await TestTables.CreateAndWaitAsync(
            client,
            TestTables.HashKeyStringSortKeyStringWithGsiProjection(tableName, indexName, ProjectionType.KEYS_ONLY),
            ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "X" },
                ["GsiPK"] = new() { S = "GROUP#A" },
                ["GsiSK"] = new() { S = "1" },
                ["projected"] = new() { S = "visible" },
                ["secret"] = new() { S = "hidden" },
            },
        }, ct);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = tableName,
            IndexName = indexName,
            KeyConditionExpression = "GsiPK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = "GROUP#A" } },
        }, ct);

        Assert.Equal(1, response.Count);
        var item = response.Items[0];
        Assert.True(item.ContainsKey("PK"));
        Assert.True(item.ContainsKey("SK"));
        Assert.True(item.ContainsKey("GsiPK"));
        Assert.True(item.ContainsKey("GsiSK"));
        Assert.False(item.ContainsKey("projected"));
        Assert.False(item.ContainsKey("secret"));
    }

    [Theory]
    [BackendData]
    public async Task Query_on_GSI_with_ALL_projection_returns_every_attribute(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("gsi_all");
        const string indexName = "GsiIndex";
        await TestTables.CreateAndWaitAsync(
            client,
            TestTables.HashKeyStringSortKeyStringWithGsiProjection(tableName, indexName, ProjectionType.ALL),
            ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "X" },
                ["GsiPK"] = new() { S = "GROUP#A" },
                ["GsiSK"] = new() { S = "1" },
                ["projected"] = new() { S = "visible" },
                ["secret"] = new() { S = "alsoVisible" },
            },
        }, ct);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = tableName,
            IndexName = indexName,
            KeyConditionExpression = "GsiPK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = "GROUP#A" } },
        }, ct);

        Assert.Equal(1, response.Count);
        var item = response.Items[0];
        Assert.Equal("visible", item["projected"].S);
        Assert.Equal("alsoVisible", item["secret"].S);
    }
}
