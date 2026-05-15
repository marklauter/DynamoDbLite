using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

// Real DynamoDB historically rejected empty-string scalar values; since May 2020 it accepts them.
// Lock that behavior — empty strings round-trip cleanly on all three backends.
[Collection("DynamoDbFixtureCollection")]
public sealed class EmptyStringParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task PutItem_then_GetItem_roundtrips_empty_string_scalar(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("empty_str");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["note"] = new() { S = "" },
            },
        }, ct);

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
        }, ct);

        Assert.True(response.IsItemSet);
        Assert.True(response.Item.ContainsKey("note"));
        Assert.Equal("", response.Item["note"].S);
    }
}
