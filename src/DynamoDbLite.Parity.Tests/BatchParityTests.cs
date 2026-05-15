using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class BatchParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
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
}
