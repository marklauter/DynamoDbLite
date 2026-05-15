using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class SizeOperatorParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task UpdateItem_with_size_condition_succeeds_when_list_size_passes(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("size_pass");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["tags"] = new() { L = [new AttributeValue { S = "a" }, new AttributeValue { S = "b" }, new AttributeValue { S = "c" }] },
            },
        }, ct);

        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
            UpdateExpression = "SET #s = :s",
            ConditionExpression = "size(#t) > :min",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#t"] = "tags",
                ["#s"] = "status",
            },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":min"] = new() { N = "2" },
                [":s"] = new() { S = "ok" },
            },
        }, ct);

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
        }, ct);

        Assert.Equal("ok", response.Item["status"].S);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task UpdateItem_with_size_condition_throws_when_list_too_small(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("size_fail");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["tags"] = new() { L = [new AttributeValue { S = "a" }] },
            },
        }, ct);

        _ = await Assert.ThrowsAsync<ConditionalCheckFailedException>(() => client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
            UpdateExpression = "SET #s = :s",
            ConditionExpression = "size(#t) > :min",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#t"] = "tags",
                ["#s"] = "status",
            },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":min"] = new() { N = "5" },
                [":s"] = new() { S = "ok" },
            },
        }, ct));
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task Scan_with_size_filter_on_string_set_returns_matching_items(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("size_scan");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["permissions"] = new() { SS = ["admin", "owner"] },
            },
        }, ct);
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-2" },
                ["permissions"] = new() { SS = ["viewer"] },
            },
        }, ct);
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-3" },
                ["permissions"] = new() { SS = ["a", "b", "c"] },
            },
        }, ct);

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = tableName,
            FilterExpression = "size(#p) = :n",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#p"] = "permissions" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":n"] = new() { N = "2" } },
        }, ct);

        Assert.Equal(1, response.Count);
        Assert.Equal("user-1", response.Items[0]["PK"].S);
    }
}
