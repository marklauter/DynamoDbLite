using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class LocalSecondaryIndexParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [BackendData]
    public async Task Query_on_LSI_with_begins_with_on_alternate_sort_key_returns_matching_items(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("lsi_begins");
        const string indexName = "LsiIndex";
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyStringSortKeyStringWithLsi(tableName, indexName), ct);

        foreach (var (sk, lsiSk) in new[] { ("a", "alpha"), ("b", "alphabet"), ("c", "beta") })
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = "USER#1" },
                    ["SK"] = new() { S = sk },
                    ["LsiSK"] = new() { S = lsiSk },
                },
            }, ct);
        }

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = tableName,
            IndexName = indexName,
            KeyConditionExpression = "PK = :pk AND begins_with(LsiSK, :prefix)",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "USER#1" },
                [":prefix"] = new() { S = "alpha" },
            },
        }, ct);

        Assert.Equal(2, response.Count);
        var sortedLsiSks = response.Items.Select(i => i["LsiSK"].S).OrderBy(s => s, StringComparer.Ordinal).ToList();
        Assert.Equal("alpha", sortedLsiSks[0]);
        Assert.Equal("alphabet", sortedLsiSks[1]);
    }

    [Theory]
    [BackendData]
    public async Task Query_on_LSI_with_INCLUDE_projection_returns_projected_attributes_only(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("lsi_include");
        const string indexName = "LsiIndex";
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyStringSortKeyStringWithLsi(tableName, indexName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "X" },
                ["LsiSK"] = new() { S = "alt" },
                ["projected"] = new() { S = "visible" },
                ["secret"] = new() { S = "hidden" },
            },
        }, ct);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = tableName,
            IndexName = indexName,
            KeyConditionExpression = "PK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = "USER#1" } },
        }, ct);

        Assert.Equal(1, response.Count);
        var item = response.Items[0];
        Assert.True(item.ContainsKey("PK"));
        Assert.True(item.ContainsKey("SK"));
        Assert.True(item.ContainsKey("LsiSK"));
        Assert.True(item.ContainsKey("projected"));
        Assert.Equal("visible", item["projected"].S);
        Assert.False(item.ContainsKey("secret"));
    }
}
