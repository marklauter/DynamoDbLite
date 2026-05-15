using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class ItemCrudParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [BackendData]
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
    [BackendData]
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
    [BackendData]
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
    [BackendData]
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

    [Theory]
    [BackendData]
    public async Task PutItem_then_GetItem_round_trips_binary_attribute(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("crud_binary");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var expected = new byte[] { 0x01, 0x02, 0x03, 0xFF };
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["payload"] = new() { B = new MemoryStream(expected) },
            },
        }, ct);

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
        }, ct);

        Assert.True(response.IsItemSet);
        Assert.Equal(expected, response.Item["payload"].B.ToArray());
    }

    [Theory]
    [BackendData]
    public async Task PutItem_then_GetItem_round_trips_null_attribute(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("crud_null");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["deleted_at"] = new() { NULL = true },
            },
        }, ct);

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
        }, ct);

        Assert.True(response.IsItemSet);
        Assert.True(response.Item["deleted_at"].NULL);
    }

    [Theory]
    [BackendData]
    public async Task PutItem_then_GetItem_round_trips_string_set_attribute(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("crud_ss");
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

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
        }, ct);

        Assert.True(response.IsItemSet);
        var sorted = response.Item["permissions"].SS.OrderBy(s => s, StringComparer.Ordinal).ToList();
        Assert.Equal(3, sorted.Count);
        Assert.Equal("admin", sorted[0]);
        Assert.Equal("owner", sorted[1]);
        Assert.Equal("viewer", sorted[2]);
    }

    [Theory]
    [BackendData]
    public async Task PutItem_then_GetItem_round_trips_number_set_attribute(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("crud_ns");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["scores"] = new() { NS = ["1", "2", "3"] },
            },
        }, ct);

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
        }, ct);

        Assert.True(response.IsItemSet);
        var sorted = response.Item["scores"].NS.OrderBy(s => s, StringComparer.Ordinal).ToList();
        Assert.Equal(3, sorted.Count);
        Assert.Equal("1", sorted[0]);
        Assert.Equal("2", sorted[1]);
        Assert.Equal("3", sorted[2]);
    }

    [Theory]
    [BackendData]
    public async Task PutItem_then_GetItem_round_trips_binary_set_attribute(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("crud_bs");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var first = new byte[] { 0x01, 0x02 };
        var second = new byte[] { 0x03, 0x04, 0x05 };
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["chunks"] = new() { BS = [new MemoryStream(first), new MemoryStream(second)] },
            },
        }, ct);

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
        }, ct);

        Assert.True(response.IsItemSet);
        var actual = response.Item["chunks"].BS
            .Select(s => Convert.ToBase64String(s.ToArray()))
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
        Assert.Equal(2, actual.Count);
        Assert.Equal(Convert.ToBase64String(first), actual[0]);
        Assert.Equal(Convert.ToBase64String(second), actual[1]);
    }
}
