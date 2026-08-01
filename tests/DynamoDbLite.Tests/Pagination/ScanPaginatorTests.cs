using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;
using System.Diagnostics.CodeAnalysis;
using static DynamoDbLite.Tests.Pagination.PaginatorTestSupport;

namespace DynamoDbLite.Tests.Pagination;

// Scan pages on LastEvaluatedKey out / ExclusiveStartKey in, stopping when the key comes back
// absent. Limit bounds a page, never the enumeration.
public sealed class ScanPaginatorTests
    : DynamoDbClientFixture
{
    protected override async ValueTask SetupAsync(CancellationToken ct)
    {
        await CreateTestTableAsync(Client(StoreType.DdbLite), ct);
        await CreateTestTableAsync(Client(StoreType.DdbLiteFile), ct);
    }

    private async Task SeedAsync(DynamoDbClient client, int count)
    {
        for (var i = 0; i < count; i++)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = TestTableName,
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = $"PK#{i}" },
                    ["SK"] = new() { S = $"SK#{i}" },
                }
            }, TestContext.Current.CancellationToken);
        }
    }

    private static IReadOnlyList<string> PartitionKeys(IEnumerable<ScanResponse> pages) =>
        [.. pages.SelectMany(static p => p.Items).Select(static i => i["PK"].S).Order(StringComparer.Ordinal)];

    // ── Laziness ────────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP016:Don't use disposed instance", Justification = "Constructing a paginator on a disposed client is the behavior under test: construction issues no call, so it must not throw.")]
    public void Scan_OnDisposedClient_ConstructsWithoutIssuingCall(StoreType st)
    {
        var client = Client(st);
        client.Dispose();

        var paginator = client.Paginators!.Scan(new ScanRequest { TableName = TestTableName });

        Assert.NotNull(paginator);
    }

    // ── Page shape ──────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Scan_EmptyTable_YieldsOnePageWithNoItems(StoreType st)
    {
        var client = Client(st);

        var pages = await CollectAsync(
            client.Paginators!.Scan(new ScanRequest { TableName = TestTableName }).Responses,
            TestContext.Current.CancellationToken);

        _ = Assert.Single(pages);
        Assert.Empty(pages[0].Items);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Scan_NoLimit_YieldsOnePageWithEveryItem(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 7);

        var pages = await CollectAsync(
            client.Paginators!.Scan(new ScanRequest { TableName = TestTableName }).Responses,
            TestContext.Current.CancellationToken);

        _ = Assert.Single(pages);
        Assert.Equal(7, pages[0].Items.Count);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Scan_LimitAboveItemCount_YieldsOnePageWithEveryItem(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 3);

        var pages = await CollectAsync(
            client.Paginators!.Scan(new ScanRequest { TableName = TestTableName, Limit = 4 }).Responses,
            TestContext.Current.CancellationToken);

        _ = Assert.Single(pages);
        Assert.Equal(3, pages[0].Items.Count);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Scan_LimitEqualToItemCount_YieldsEveryItem(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 3);

        var pages = await CollectAsync(
            client.Paginators!.Scan(new ScanRequest { TableName = TestTableName, Limit = 3 }).Responses,
            TestContext.Current.CancellationToken);

        Assert.Equal(["PK#0", "PK#1", "PK#2"], PartitionKeys(pages));
    }

    // The profile's worked example: seven items at Limit = 2 page as 2, 2, 2, 1.
    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Scan_LimitBelowItemCount_PagesEveryItemInBoundedPages(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 7);

        var pages = await CollectAsync(
            client.Paginators!.Scan(new ScanRequest { TableName = TestTableName, Limit = 2 }).Responses,
            TestContext.Current.CancellationToken);

        Assert.Equal([2, 2, 2, 1], pages.Select(static p => p.Items.Count));
        Assert.Equal(["PK#0", "PK#1", "PK#2", "PK#3", "PK#4", "PK#5", "PK#6"], PartitionKeys(pages));
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Scan_LimitOfOne_YieldsAtMostOneItemPerPage(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 3);

        var pages = await CollectAsync(
            client.Paginators!.Scan(new ScanRequest { TableName = TestTableName, Limit = 1 }).Responses,
            TestContext.Current.CancellationToken);

        Assert.All(pages, static p => Assert.True(p.Items.Count <= 1));
        Assert.Equal(["PK#0", "PK#1", "PK#2"], PartitionKeys(pages));
    }

    // ── Caller-supplied start key ───────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Scan_CallerSuppliedExclusiveStartKey_ResumesAfterIt(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 5);

        var firstPage = await client.ScanAsync(
            new ScanRequest { TableName = TestTableName, Limit = 1 },
            TestContext.Current.CancellationToken);

        var pages = await CollectAsync(
            client.Paginators!.Scan(new ScanRequest
            {
                TableName = TestTableName,
                Limit = 2,
                ExclusiveStartKey = firstPage.LastEvaluatedKey,
            }).Responses,
            TestContext.Current.CancellationToken);

        Assert.Equal(4, PartitionKeys(pages).Count);
        Assert.DoesNotContain(firstPage.Items[0]["PK"].S, PartitionKeys(pages), StringComparer.Ordinal);
    }

    // ── Re-enumeration ──────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Scan_ReEnumerated_RestartsAndYieldsSamePages(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 5);

        var paginator = client.Paginators!.Scan(new ScanRequest { TableName = TestTableName, Limit = 2 });

        var first = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);
        var second = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);

        Assert.Equal(first.Select(static p => p.Items.Count), second.Select(static p => p.Items.Count));
        Assert.Equal(PartitionKeys(first), PartitionKeys(second));
    }

    // ── Cancellation ────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Scan_CancelledToken_ThrowsOperationCanceled(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 3);

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var paginator = client.Paginators!.Scan(new ScanRequest { TableName = TestTableName });

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => CollectAsync(paginator.Responses, cts.Token));
    }

    // ── Disposal ────────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Scan_EnumeratedAfterClientDisposed_ThrowsObjectDisposed(StoreType st)
    {
        var client = Client(st);
        var paginator = client.Paginators!.Scan(new ScanRequest { TableName = TestTableName });
        client.Dispose();

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(
            () => CollectAsync(paginator.Responses, TestContext.Current.CancellationToken));
    }
}
