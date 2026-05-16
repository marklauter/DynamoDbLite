using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class ReturnValuesParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [BackendData]
    public async Task PutItem_with_ALL_OLD_returns_prior_item_on_overwrite(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("rv_put_old");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["label"] = new() { S = "before" },
            },
        }, ct);

        var response = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["label"] = new() { S = "after" },
            },
            ReturnValues = ReturnValue.ALL_OLD,
        }, ct);

        Assert.Equal("before", response.Attributes["label"].S);
    }

    [Theory]
    [BackendData]
    public async Task PutItem_with_NONE_returns_empty_Attributes(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("rv_put_none");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["label"] = new() { S = "before" },
            },
        }, ct);

        var response = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["label"] = new() { S = "after" },
            },
            ReturnValues = ReturnValue.NONE,
        }, ct);

        Assert.True(response.Attributes == null || response.Attributes.Count == 0);
    }

    [Theory]
    [BackendData]
    public async Task UpdateItem_with_ALL_OLD_returns_full_prior_item(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("rv_upd_aold");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["label"] = new() { S = "before" },
                ["other"] = new() { S = "keep" },
            },
        }, ct);

        var response = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
            UpdateExpression = "SET label = :v",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":v"] = new() { S = "after" } },
            ReturnValues = ReturnValue.ALL_OLD,
        }, ct);

        Assert.Equal("before", response.Attributes["label"].S);
        Assert.Equal("keep", response.Attributes["other"].S);
    }

    [Theory]
    [BackendData]
    public async Task UpdateItem_with_UPDATED_OLD_returns_only_modified_attribute_prior_values(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("rv_upd_uold");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["label"] = new() { S = "before" },
                ["other"] = new() { S = "keep" },
            },
        }, ct);

        var response = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
            UpdateExpression = "SET label = :v",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":v"] = new() { S = "after" } },
            ReturnValues = ReturnValue.UPDATED_OLD,
        }, ct);

        Assert.Equal("before", response.Attributes["label"].S);
        Assert.False(response.Attributes.ContainsKey("other"));
    }

    [Theory]
    [BackendData]
    public async Task UpdateItem_with_ALL_NEW_returns_full_post_update_item(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("rv_upd_anew");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["label"] = new() { S = "before" },
                ["other"] = new() { S = "keep" },
            },
        }, ct);

        var response = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
            UpdateExpression = "SET label = :v",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":v"] = new() { S = "after" } },
            ReturnValues = ReturnValue.ALL_NEW,
        }, ct);

        Assert.Equal("after", response.Attributes["label"].S);
        Assert.Equal("keep", response.Attributes["other"].S);
    }

    [Theory]
    [BackendData]
    public async Task UpdateItem_with_UPDATED_NEW_returns_only_modified_attribute_new_values(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("rv_upd_unew");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["label"] = new() { S = "before" },
                ["other"] = new() { S = "keep" },
            },
        }, ct);

        var response = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
            UpdateExpression = "SET label = :v",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":v"] = new() { S = "after" } },
            ReturnValues = ReturnValue.UPDATED_NEW,
        }, ct);

        Assert.Equal("after", response.Attributes["label"].S);
        Assert.False(response.Attributes.ContainsKey("other"));
    }

    [Theory]
    [BackendData]
    public async Task DeleteItem_with_ALL_OLD_returns_deleted_item(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("rv_del_aold");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["label"] = new() { S = "gone" },
            },
        }, ct);

        var response = await client.DeleteItemAsync(new DeleteItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
            ReturnValues = ReturnValue.ALL_OLD,
        }, ct);

        Assert.Equal("gone", response.Attributes["label"].S);
    }
}
