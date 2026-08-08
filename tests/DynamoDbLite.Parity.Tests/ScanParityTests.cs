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
        const int expectedPages = 3;
        const int terminalPageSize = seedCount - pageSize * (expectedPages - 1);

        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("scan_page");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var seeded = await SeedAsync(client, tableName, seedCount, seedCount, ct);

        var pages = await ScanAllPagesAsync(client, seedCount, cursor => new ScanRequest
        {
            TableName = tableName,
            Limit = pageSize,
            ExclusiveStartKey = cursor,
        }, ct);

        Assert.Equal(expectedPages, pages.Count);

        // Every page short of the last fills the Limit, scans exactly what it returns
        // because no filter runs, and carries a cursor naming its own last item. The
        // terminal page needs no cursor assertion: ScanAllPagesAsync stops only on an
        // empty cursor, so a backend that never cleared it fails there instead.
        foreach (var page in pages.Take(pages.Count - 1))
        {
            Assert.Equal(pageSize, page.Items.Count);
            Assert.Equal(pageSize, page.Count);
            Assert.Equal(pageSize, page.ScannedCount);
            AssertCursorNamesLastItem(page);
        }

        var terminal = pages[^1];
        Assert.Equal(terminalPageSize, terminal.Items.Count);
        Assert.Equal(terminalPageSize, terminal.Count);
        Assert.Equal(terminalPageSize, terminal.ScannedCount);

        // Scan order is unspecified, so the pages are compared as sorted sequences.
        // Equal counts rule out duplicates; equal sequences rule out skips.
        var returned = pages.SelectMany(page => page.Items).Select(item => item["PK"].S).ToList();
        Assert.Equal(seedCount, returned.Count);
        Assert.Equal(seeded.Order(), returned.Order());
    }

    [Theory]
    [BackendData]
    public async Task Scan_with_Limit_dividing_the_table_exactly_ends_on_an_empty_page(ParityBackend backend)
    {
        const int seedCount = 20;
        const int pageSize = 10;

        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("scan_page_exact");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var seeded = await SeedAsync(client, tableName, seedCount, seedCount, ct);

        var pages = await ScanAllPagesAsync(client, seedCount, cursor => new ScanRequest
        {
            TableName = tableName,
            Limit = pageSize,
            ExclusiveStartKey = cursor,
        }, ct);

        // A page that fills the Limit carries a cursor even when it drained the table,
        // so the walk ends on an empty page rather than on the last full one.
        Assert.Equal(seedCount / pageSize + 1, pages.Count);

        foreach (var page in pages.Take(pages.Count - 1))
        {
            Assert.Equal(pageSize, page.Items.Count);
            Assert.Equal(pageSize, page.Count);
            Assert.Equal(pageSize, page.ScannedCount);
            AssertCursorNamesLastItem(page);
        }

        // The table was already drained, so the trailing page reads nothing at all.
        var terminal = pages[^1];
        Assert.Empty(terminal.Items);
        Assert.Equal(0, terminal.Count);
        Assert.Equal(0, terminal.ScannedCount);

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

        var pages = await ScanAllPagesAsync(client, seedCount, cursor => new ScanRequest
        {
            TableName = tableName,
            Limit = pageSize,
            ExclusiveStartKey = cursor,
            FilterExpression = "#g = :keep",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#g"] = "group" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":keep"] = new() { S = "keep" } },
        }, ct);

        // seedCount divides by pageSize, so the last full window still carries a cursor
        // and the walk ends on a page that reads nothing.
        Assert.Equal(seedCount / pageSize + 1, pages.Count);

        foreach (var page in pages)
            Assert.Equal(page.Items.Count, page.Count);

        // A page carrying a cursor stopped because it filled the window, so it scanned
        // exactly Limit rows however many of them survived the filter.
        foreach (var page in pages.Take(pages.Count - 1))
        {
            Assert.Equal(pageSize, page.ScannedCount);
            _ = AssertCursorShape(page);
        }

        var terminal = pages[^1];
        Assert.Empty(terminal.Items);
        Assert.Equal(0, terminal.Count);
        Assert.Equal(0, terminal.ScannedCount);

        var returned = pages.SelectMany(page => page.Items).Select(item => item["PK"].S).ToList();
        Assert.Equal(keepCount, returned.Count);
        Assert.Equal(seeded.Take(keepCount).Order(), returned.Order());

        // Limit bounds the window the scan reads before the filter runs, so a page can
        // come back short while rows remain. Given that, the cursor-carrying pages span
        // more than keepCount / pageSize windows while holding at most keepCount matches
        // between them, so one returns fewer than pageSize items whatever order the
        // backend scans in. A backend applying Limit after the filter fills every page
        // instead and fails here, which is the divergence this case exists to catch.
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

    // These tables are hash-key only, so a cursor holds exactly one attribute. Extra
    // attributes still round-trip inside one backend, so only a direct assertion on the
    // contents catches them. Returns the key the cursor names.
    private static string AssertCursorShape(ScanResponse page)
    {
        var cursor = page.LastEvaluatedKey;
        Assert.NotNull(cursor);
        Assert.Equal("PK", Assert.Single(cursor.Keys));
        return cursor["PK"].S;
    }

    // An unfiltered page returns every row it scanned, so its cursor names the last item
    // in the page. A filtered page's cursor names the last row scanned, which the filter
    // may have dropped, so only AssertCursorShape applies there.
    private static void AssertCursorNamesLastItem(ScanResponse page) =>
        Assert.Equal(page.Items[^1]["PK"].S, AssertCursorShape(page));

    // Walks every page of a scan, threading LastEvaluatedKey into the next request.
    // One page per seeded item is the worst legitimate case; past that the cursor is
    // not advancing and the walk would never end.
    private static async Task<IReadOnlyList<ScanResponse>> ScanAllPagesAsync(
        IAmazonDynamoDB client,
        int seedCount,
        Func<Dictionary<string, AttributeValue>?, ScanRequest> requestFor,
        CancellationToken ct)
    {
        var maxPages = seedCount + 1;

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

        Assert.Fail($"scan returned {maxPages} pages for {seedCount} items, so the cursor is not advancing");
        return pages;
    }
}
