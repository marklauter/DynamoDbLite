using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class ScanParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task Scan_with_FilterExpression_returns_matching_items_and_correct_ScannedCount(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("scan_filter");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        foreach (var (pk, matches) in new[] { ("a", true), ("b", false), ("c", true) })
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
        }, ct);

        Assert.Equal(2, response.Count);
        Assert.Equal(3, response.ScannedCount);
    }
}
