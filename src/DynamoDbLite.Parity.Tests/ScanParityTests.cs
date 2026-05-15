using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class ScanParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [BackendData]
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

    [Theory]
    [BackendData]
    public async Task Scan_with_contains_on_string_set_returns_matching_items(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("scan_contains");
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

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = tableName,
            FilterExpression = "contains(#p, :v)",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#p"] = "permissions" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":v"] = new() { S = "admin" } },
        }, ct);

        Assert.Equal(1, response.Count);
        Assert.Equal(2, response.ScannedCount);
        Assert.Equal("user-1", response.Items[0]["PK"].S);
    }

    [Theory]
    [BackendData]
    public async Task Scan_with_IN_returns_items_whose_attribute_matches_any_value(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("scan_in");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        foreach (var (pk, category) in new[] { ("a", "alpha"), ("b", "beta"), ("c", "gamma"), ("d", "delta") })
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = pk },
                    ["category"] = new() { S = category },
                },
            }, ct);
        }

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = tableName,
            FilterExpression = "#c IN (:v1, :v2)",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#c"] = "category" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":v1"] = new() { S = "alpha" },
                [":v2"] = new() { S = "gamma" },
            },
        }, ct);

        Assert.Equal(2, response.Count);
        Assert.Equal(4, response.ScannedCount);
    }

    [Theory]
    [BackendData]
    public async Task Scan_with_two_segments_returns_full_set_when_merged(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("scan_segs");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var expected = new HashSet<string>();
        for (var i = 0; i < 20; i++)
        {
            var pk = $"item-{i:D2}";
            _ = expected.Add(pk);
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = pk } },
            }, ct);
        }

        var seg0 = await client.ScanAsync(new ScanRequest
        {
            TableName = tableName,
            TotalSegments = 2,
            Segment = 0,
        }, ct);

        var seg1 = await client.ScanAsync(new ScanRequest
        {
            TableName = tableName,
            TotalSegments = 2,
            Segment = 1,
        }, ct);

        var merged = new HashSet<string>();
        foreach (var item in seg0.Items)
            _ = merged.Add(item["PK"].S);
        foreach (var item in seg1.Items)
            _ = merged.Add(item["PK"].S);

        Assert.Equal(expected, merged);
        Assert.Equal(20, seg0.Count + seg1.Count);
    }
}
