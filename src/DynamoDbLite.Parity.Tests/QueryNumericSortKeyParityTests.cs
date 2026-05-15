using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class QueryNumericSortKeyParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task Query_with_BETWEEN_on_numeric_sort_key_returns_inclusive_range_ascending(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("query_nsk_btwn");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyStringSortKeyNumber(tableName), ct);

        foreach (var sk in new[] { "1", "5", "10", "15", "20" })
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = "USER#1" },
                    ["SK"] = new() { N = sk },
                },
            }, ct);
        }

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = tableName,
            KeyConditionExpression = "PK = :pk AND SK BETWEEN :lo AND :hi",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "USER#1" },
                [":lo"] = new() { N = "5" },
                [":hi"] = new() { N = "15" },
            },
        }, ct);

        Assert.Equal(3, response.Count);
        Assert.Equal("5", response.Items[0]["SK"].N);
        Assert.Equal("10", response.Items[1]["SK"].N);
        Assert.Equal("15", response.Items[2]["SK"].N);
    }
}
