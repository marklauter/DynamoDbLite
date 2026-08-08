using Amazon.DynamoDBv2;
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

    [Theory]
    [BackendData]
    public async Task Scan_with_Limit_paginates_via_LastEvaluatedKey_without_duplicates_or_gaps(ParityBackend backend)
    {
        const int seedCount = 25;
        const int pageSize = 10;

        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("scan_page");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var seeded = await SeedAsync(client, tableName, seedCount, seedCount, ct);

        var pages = await ScanAllPagesAsync(client, cursor => new ScanRequest
        {
            TableName = tableName,
            Limit = pageSize,
            ExclusiveStartKey = cursor,
        }, ct);

        Assert.True(pages.Count > 1, $"{seedCount} items at a page size of {pageSize} must paginate");
        Assert.NotEmpty(pages[0].LastEvaluatedKey);
        Assert.True(pages[^1].LastEvaluatedKey is null or { Count: 0 }, "the terminal page must not carry a cursor");

        foreach (var page in pages)
        {
            Assert.True(page.Items.Count <= pageSize, $"page returned {page.Items.Count} items for Limit {pageSize}");
            Assert.True(page.ScannedCount <= pageSize, $"page scanned {page.ScannedCount} items for Limit {pageSize}");
        }

        // Scan order is unspecified, so the pages are compared as sorted sequences.
        // Equal counts rule out duplicates; equal sequences rule out skips.
        var returned = pages.SelectMany(page => page.Items).Select(item => item["PK"].S).ToList();
        Assert.Equal(seedCount, returned.Count);
        Assert.Equal(seeded.Order(), returned.Order());
    }

    [Theory]
    [BackendData]
    public async Task Scan_with_FilterExpression_and_Limit_bounds_the_pre_filter_window(ParityBackend backend)
    {
        const int seedCount = 25;
        const int keepCount = 10;
        const int pageSize = 5;

        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("scan_page_filt");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var seeded = await SeedAsync(client, tableName, seedCount, keepCount, ct);

        var pages = await ScanAllPagesAsync(client, cursor => new ScanRequest
        {
            TableName = tableName,
            Limit = pageSize,
            ExclusiveStartKey = cursor,
            FilterExpression = "#g = :keep",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#g"] = "group" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":keep"] = new() { S = "keep" } },
        }, ct);

        foreach (var page in pages)
            Assert.True(page.ScannedCount <= pageSize, $"page scanned {page.ScannedCount} items for Limit {pageSize}");

        var returned = pages.SelectMany(page => page.Items).Select(item => item["PK"].S).ToList();
        Assert.Equal(keepCount, returned.Count);
        Assert.Equal(seeded.Take(keepCount).Order(), returned.Order());

        // Limit bounds the window the scan reads before the filter runs, so a page can
        // come back short while rows remain. The pages carrying a cursor hold at most
        // keepCount matches between them, and there are more than keepCount / pageSize
        // of them, so at least one must return fewer than pageSize items.
        Assert.Contains(pages, page => page.LastEvaluatedKey is { Count: > 0 } && page.Items.Count < pageSize);
    }

    // Seeds `count` items keyed item-00..item-NN in insertion order, tagging the first
    // `keepCount` with group=keep so a FilterExpression can select exactly that subset.
    private static async Task<IReadOnlyList<string>> SeedAsync(IAmazonDynamoDB client, string tableName, int count, int keepCount, CancellationToken ct)
    {
        var keys = new List<string>(count);
        for (var i = 0; i < count; i++)
        {
            var pk = $"item-{i:D2}";
            keys.Add(pk);
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = pk },
                    ["group"] = new() { S = i < keepCount ? "keep" : "drop" },
                },
            }, ct);
        }

        return keys;
    }

    // Walks every page of a scan, threading LastEvaluatedKey into the next request.
    // Bounded so a cursor that never clears fails the test instead of hanging the suite.
    private static async Task<IReadOnlyList<ScanResponse>> ScanAllPagesAsync(
        IAmazonDynamoDB client,
        Func<Dictionary<string, AttributeValue>?, ScanRequest> requestFor,
        CancellationToken ct)
    {
        const int maxPages = 20;

        var pages = new List<ScanResponse>();
        Dictionary<string, AttributeValue>? cursor = null;

        while (pages.Count < maxPages)
        {
            var page = await client.ScanAsync(requestFor(cursor), ct);
            pages.Add(page);
            cursor = page.LastEvaluatedKey;
            if (cursor is not { Count: > 0 })
                return pages;
        }

        Assert.Fail($"scan did not exhaust the table within {maxPages} pages");
        return pages;
    }
}
