using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

// Pins the Paginators surface against the reference implementation. On the DynamoDbLocal backend
// Paginators is the AWS SDK's own factory, so these tests measure our semantics against AWS rather
// than against a written description of AWS.
//
// Not covered here: ListExports / ListImports are excluded from the parity suite by decision, and
// ListContributorInsights is unsupported. Both live in the unit suite.
[Collection("DynamoDbFixtureCollection")]
public sealed class PaginatorParityTests(DynamoDbFixture fixture)
{
    private static IDynamoDBv2PaginatorFactory Paginators(IAmazonDynamoDB client)
    {
        var factory = client.Paginators;
        Assert.NotNull(factory);
        return factory;
    }

    private static async Task<IReadOnlyList<T>> CollectAsync<T>(IAsyncEnumerable<T> source, CancellationToken ct)
    {
        var collected = new List<T>();
        await foreach (var item in source.WithCancellation(ct))
            collected.Add(item);

        return collected;
    }

    // Begins an enumeration, pulls one element, then disposes without draining.
    private static async Task BeginAndAbandonAsync<T>(IAsyncEnumerable<T> source, CancellationToken ct)
    {
        var enumerator = source.GetAsyncEnumerator(ct);
        await using (enumerator.ConfigureAwait(false))
            _ = await enumerator.MoveNextAsync();
    }

    private static async Task SeedHashKeyItemsAsync(IAmazonDynamoDB client, string tableName, int count, CancellationToken ct)
    {
        for (var i = 0; i < count; i++)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = $"item-{i:D2}" } },
            }, ct);
        }
    }

    // ── Scan ────────────────────────────────────────────────────────

    [Theory]
    [BackendData]
    public async Task Scan_paginator_yields_every_item_exactly_once_in_bounded_pages(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("pg_scan");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);
        await SeedHashKeyItemsAsync(client, tableName, 7, ct);

        var pages = await CollectAsync(
            Paginators(client).Scan(new ScanRequest { TableName = tableName, Limit = 2 }).Responses,
            ct);

        // Page count is not asserted: DynamoDB is free to hand back a continuation token on the
        // last populated page and terminate with an empty one.
        Assert.All(pages, static p => Assert.True(p.Items.Count <= 2));

        var keys = pages.SelectMany(static p => p.Items).Select(static i => i["PK"].S).Order(StringComparer.Ordinal).ToArray();
        Assert.Equal(Enumerable.Range(0, 7).Select(static i => $"item-{i:D2}"), keys);
    }

    [Theory]
    [BackendData]
    public async Task Scan_paginator_without_limit_yields_one_page(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("pg_scan1");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);
        await SeedHashKeyItemsAsync(client, tableName, 3, ct);

        var pages = await CollectAsync(
            Paginators(client).Scan(new ScanRequest { TableName = tableName }).Responses,
            ct);

        _ = Assert.Single(pages);
        Assert.Equal(3, pages[0].Items.Count);
    }

    [Theory]
    [BackendData]
    public async Task Scan_paginator_over_empty_table_yields_one_empty_page(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("pg_scan0");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var pages = await CollectAsync(
            Paginators(client).Scan(new ScanRequest { TableName = tableName }).Responses,
            ct);

        _ = Assert.Single(pages);
        Assert.Empty(pages[0].Items);
    }

    // ── Query ───────────────────────────────────────────────────────

    [Theory]
    [BackendData]
    public async Task Query_paginator_yields_every_item_exactly_once_in_sort_key_order(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("pg_query");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyStringSortKeyString(tableName), ct);

        for (var i = 0; i < 7; i++)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = "p" },
                    ["SK"] = new() { S = $"sk-{i:D2}" },
                },
            }, ct);
        }

        var pages = await CollectAsync(
            Paginators(client).Query(new QueryRequest
            {
                TableName = tableName,
                Limit = 2,
                KeyConditionExpression = "PK = :pk",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":pk"] = new() { S = "p" } },
            }).Responses,
            ct);

        Assert.All(pages, static p => Assert.True(p.Items.Count <= 2));

        var sortKeys = pages.SelectMany(static p => p.Items).Select(static i => i["SK"].S).ToArray();
        Assert.Equal(Enumerable.Range(0, 7).Select(static i => $"sk-{i:D2}"), sortKeys);
    }

    // ── ListTables ──────────────────────────────────────────────────

    // The DynamoDbLocal container is shared across the suite, so the table list is not a fixed set.
    // What the paginator promises regardless is that paging reaches every name exactly once.
    [Theory]
    [BackendData]
    public async Task ListTables_paginator_TableNames_yields_every_created_table_exactly_once(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);

        var created = new List<string>();
        for (var i = 0; i < 3; i++)
        {
            var tableName = TestTables.UniqueName($"pg_lt{i}");
            await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);
            created.Add(tableName);
        }

        var names = await CollectAsync(
            Paginators(client).ListTables(new ListTablesRequest { Limit = 1 }).TableNames,
            ct);

        foreach (var tableName in created)
            _ = Assert.Single(names, n => string.Equals(n, tableName, StringComparison.Ordinal));
    }

    [Theory]
    [BackendData]
    public async Task ListTables_paginator_Responses_bounds_each_page_by_Limit(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("pg_ltb");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var pages = await CollectAsync(
            Paginators(client).ListTables(new ListTablesRequest { Limit = 1 }).Responses,
            ct);

        Assert.NotEmpty(pages);
        Assert.All(pages, static p => Assert.True(p.TableNames.Count <= 1));
    }

    // ── BatchGetItem ────────────────────────────────────────────────

    [Theory]
    [BackendData]
    public async Task BatchGetItem_paginator_yields_one_page_with_every_requested_item(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("pg_bgi");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);
        await SeedHashKeyItemsAsync(client, tableName, 3, ct);

        var pages = await CollectAsync(
            Paginators(client).BatchGetItem(new BatchGetItemRequest
            {
                RequestItems = new Dictionary<string, KeysAndAttributes>
                {
                    [tableName] = new()
                    {
                        Keys =
                        [
                            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "item-00" } },
                            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "item-01" } },
                            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "item-02" } },
                        ],
                    },
                },
            }).Responses,
            ct);

        _ = Assert.Single(pages);

        var keys = pages[0].Responses[tableName].Select(static i => i["PK"].S).Order(StringComparer.Ordinal).ToArray();
        Assert.Equal(["item-00", "item-01", "item-02"], keys);
    }

    // ── Single use ──────────────────────────────────────────────────

    [Theory]
    [BackendData]
    public async Task Paginator_enumerated_twice_throws_InvalidOperationException(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("pg_su1");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);
        await SeedHashKeyItemsAsync(client, tableName, 5, ct);

        var paginator = Paginators(client).Scan(new ScanRequest { TableName = tableName, Limit = 2 });

        _ = await CollectAsync(paginator.Responses, ct);

        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => CollectAsync(paginator.Responses, ct));
    }

    // Consumption is marked when enumeration begins, not when it completes.
    [Theory]
    [BackendData]
    public async Task Paginator_abandoned_partway_is_still_consumed(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("pg_su2");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);
        await SeedHashKeyItemsAsync(client, tableName, 5, ct);

        var paginator = Paginators(client).Scan(new ScanRequest { TableName = tableName, Limit = 2 });

        await BeginAndAbandonAsync(paginator.Responses, ct);

        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => CollectAsync(paginator.Responses, ct));
    }

    [Theory]
    [BackendData]
    public async Task Paginator_Responses_read_without_enumerating_is_not_consumed(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("pg_su3");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);
        await SeedHashKeyItemsAsync(client, tableName, 5, ct);

        var paginator = Paginators(client).Scan(new ScanRequest { TableName = tableName, Limit = 2 });

        _ = paginator.Responses;
        _ = paginator.Responses;

        var pages = await CollectAsync(paginator.Responses, ct);

        Assert.Equal(5, pages.Sum(static p => p.Items.Count));
    }

    [Theory]
    [BackendData]
    public async Task Fresh_paginator_after_a_consumed_one_pages_normally(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("pg_su4");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);
        await SeedHashKeyItemsAsync(client, tableName, 5, ct);

        var consumed = Paginators(client).Scan(new ScanRequest { TableName = tableName, Limit = 2 });
        _ = await CollectAsync(consumed.Responses, ct);

        var fresh = Paginators(client).Scan(new ScanRequest { TableName = tableName, Limit = 2 });
        var pages = await CollectAsync(fresh.Responses, ct);

        Assert.Equal(5, pages.Sum(static p => p.Items.Count));
    }

    // Consumption belongs to the paginator, not to whichever property was enumerated.
    [Theory]
    [BackendData]
    public async Task ListTables_TableNames_after_Responses_enumerated_throws_InvalidOperationException(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("pg_su5");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var paginator = Paginators(client).ListTables(new ListTablesRequest { Limit = 1 });

        _ = await CollectAsync(paginator.Responses, ct);

        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => CollectAsync(paginator.TableNames, ct));
    }
}
