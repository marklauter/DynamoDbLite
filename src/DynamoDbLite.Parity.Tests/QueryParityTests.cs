using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class QueryParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task Query_with_KeyConditionExpression_returns_matching_items(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("query_keycond");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyStringSortKeyString(tableName), ct);

        await SeedThreeAsync(client, tableName, "USER#1", ["A", "B", "C"], ct);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = tableName,
            KeyConditionExpression = "PK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = "USER#1" } },
        }, ct);

        Assert.Equal(3, response.Count);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task Query_with_ScanIndexForward_false_returns_descending(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("query_descending");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyStringSortKeyString(tableName), ct);

        await SeedThreeAsync(client, tableName, "USER#1", ["A", "B", "C"], ct);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = tableName,
            KeyConditionExpression = "PK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = "USER#1" } },
            ScanIndexForward = false,
        }, ct);

        Assert.Equal(3, response.Count);
        Assert.Equal("C", response.Items[0]["SK"].S);
        Assert.Equal("B", response.Items[1]["SK"].S);
        Assert.Equal("A", response.Items[2]["SK"].S);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task Query_with_Limit_paginates_via_LastEvaluatedKey(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("query_paginate");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyStringSortKeyString(tableName), ct);

        await SeedThreeAsync(client, tableName, "USER#1", ["A", "B", "C"], ct);

        var page1 = await client.QueryAsync(new QueryRequest
        {
            TableName = tableName,
            KeyConditionExpression = "PK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = "USER#1" } },
            Limit = 2,
        }, ct);

        Assert.Equal(2, page1.Count);
        Assert.NotNull(page1.LastEvaluatedKey);
        Assert.NotEmpty(page1.LastEvaluatedKey);

        var page2 = await client.QueryAsync(new QueryRequest
        {
            TableName = tableName,
            KeyConditionExpression = "PK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = "USER#1" } },
            Limit = 2,
            ExclusiveStartKey = page1.LastEvaluatedKey,
        }, ct);

        Assert.Equal(1, page2.Count);
        Assert.True(page2.LastEvaluatedKey is null || page2.LastEvaluatedKey.Count == 0);
    }

    private static async Task SeedThreeAsync(IAmazonDynamoDB client, string tableName, string pk, string[] sks, CancellationToken ct)
    {
        foreach (var sk in sks)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = pk },
                    ["SK"] = new() { S = sk },
                },
            }, ct);
        }
    }
}
