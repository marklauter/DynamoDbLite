using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class UpdateExpressionParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task UpdateItem_SET_with_if_not_exists_preserves_existing_value(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("update_ifnotexists");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["name"] = new() { S = "Alice" },
            },
        }, ct);

        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
            UpdateExpression = "SET #n = if_not_exists(#n, :new)",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#n"] = "name" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":new"] = new() { S = "Bob" } },
        }, ct);

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
        }, ct);

        Assert.Equal("Alice", response.Item["name"].S);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task UpdateItem_ADD_increments_number_attribute(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("update_add");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["counter"] = new() { N = "5" },
            },
        }, ct);

        // `counter` is a DynamoDB reserved keyword — must be escaped via ExpressionAttributeNames.
        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
            UpdateExpression = "ADD #c :inc",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#c"] = "counter" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":inc"] = new() { N = "3" } },
        }, ct);

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
        }, ct);

        Assert.Equal("8", response.Item["counter"].N);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task UpdateItem_REMOVE_strips_attribute_from_item(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("update_remove");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["name"] = new() { S = "Alice" },
                ["extra"] = new() { S = "bye" },
            },
        }, ct);

        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
            UpdateExpression = "REMOVE extra",
        }, ct);

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
        }, ct);

        Assert.Equal("Alice", response.Item["name"].S);
        Assert.False(response.Item.ContainsKey("extra"));
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task UpdateItem_SET_with_list_append_appends_to_existing_list(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("update_lstapp");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["tags"] = new() { L = [new AttributeValue { S = "a" }, new AttributeValue { S = "b" }] },
            },
        }, ct);

        // `tags` is a DynamoDB reserved keyword — must be escaped via ExpressionAttributeNames.
        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
            UpdateExpression = "SET #t = list_append(#t, :new)",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#t"] = "tags" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":new"] = new() { L = [new AttributeValue { S = "c" }, new AttributeValue { S = "d" }] },
            },
        }, ct);

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
        }, ct);

        Assert.Equal(4, response.Item["tags"].L.Count);
        Assert.Equal("a", response.Item["tags"].L[0].S);
        Assert.Equal("b", response.Item["tags"].L[1].S);
        Assert.Equal("c", response.Item["tags"].L[2].S);
        Assert.Equal("d", response.Item["tags"].L[3].S);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task UpdateItem_DELETE_removes_elements_from_string_set(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("update_delset");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["permissions"] = new() { SS = ["admin", "owner", "viewer"] },
            },
        }, ct);

        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
            UpdateExpression = "DELETE #p :rm",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#p"] = "permissions" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":rm"] = new() { SS = ["owner"] },
            },
        }, ct);

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
        }, ct);

        var sorted = response.Item["permissions"].SS.OrderBy(s => s, StringComparer.Ordinal).ToList();
        Assert.Equal(2, sorted.Count);
        Assert.Equal("admin", sorted[0]);
        Assert.Equal("viewer", sorted[1]);
    }
}
