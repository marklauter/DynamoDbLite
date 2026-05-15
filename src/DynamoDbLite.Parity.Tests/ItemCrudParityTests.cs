using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class ItemCrudParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task PutItem_then_GetItem_returns_identical_item(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("crud_roundtrip");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "user-1" },
            ["name"] = new() { S = "Alice" },
            ["age"] = new() { N = "30" },
            ["active"] = new() { BOOL = true },
            ["tags"] = new() { L = [new AttributeValue { S = "admin" }, new AttributeValue { S = "owner" }] },
            ["address"] = new() { M = new Dictionary<string, AttributeValue> { ["city"] = new() { S = "Seattle" } } },
        };
        _ = await client.PutItemAsync(new PutItemRequest { TableName = tableName, Item = item }, ct);

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
        }, ct);

        Assert.True(response.IsItemSet);
        Assert.Equal("Alice", response.Item["name"].S);
        Assert.Equal("30", response.Item["age"].N);
        Assert.True(response.Item["active"].BOOL);
        Assert.Equal(2, response.Item["tags"].L.Count);
        Assert.Equal("admin", response.Item["tags"].L[0].S);
        Assert.Equal("Seattle", response.Item["address"].M["city"].S);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task PutItem_with_attribute_not_exists_succeeds_for_new_key(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("crud_putnew");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var response = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
            ConditionExpression = "attribute_not_exists(PK)",
        }, ct);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.HttpStatusCode);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task PutItem_with_attribute_not_exists_throws_for_existing_key(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("crud_putexists");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
        }, ct);

        _ = await Assert.ThrowsAsync<ConditionalCheckFailedException>(() => client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
            ConditionExpression = "attribute_not_exists(PK)",
        }, ct));
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task DeleteItem_with_attribute_exists_condition_throws_for_missing_key(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("crud_delete");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await Assert.ThrowsAsync<ConditionalCheckFailedException>(() => client.DeleteItemAsync(new DeleteItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "missing" } },
            ConditionExpression = "attribute_exists(PK)",
        }, ct));
    }
}
