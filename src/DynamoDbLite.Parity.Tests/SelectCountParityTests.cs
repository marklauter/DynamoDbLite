using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class SelectCountParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task Query_with_Select_COUNT_returns_count_without_items(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("sel_q");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyStringSortKeyString(tableName), ct);

        foreach (var sk in new[] { "a", "b", "c" })
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = "USER#1" },
                    ["SK"] = new() { S = sk },
                },
            }, ct);
        }

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = tableName,
            KeyConditionExpression = "PK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = "USER#1" } },
            Select = Select.COUNT,
        }, ct);

        Assert.Equal(3, response.Count);
        Assert.Equal(3, response.ScannedCount);
        Assert.True(response.Items == null || response.Items.Count == 0);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task Scan_with_Select_COUNT_and_filter_returns_count_without_items(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("sel_s");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        foreach (var (pk, matches) in new[] { ("a", true), ("b", false), ("c", true), ("d", true) })
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = pk },
                    ["matches"] = new() { BOOL = matches },
                },
            }, ct);
        }

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = tableName,
            FilterExpression = "#m = :true",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#m"] = "matches" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":true"] = new() { BOOL = true } },
            Select = Select.COUNT,
        }, ct);

        Assert.Equal(3, response.Count);
        Assert.Equal(4, response.ScannedCount);
        Assert.True(response.Items == null || response.Items.Count == 0);
    }
}
