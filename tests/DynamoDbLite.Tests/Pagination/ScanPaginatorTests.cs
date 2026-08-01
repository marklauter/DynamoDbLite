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

        // An exact sequence, not a count plus a does-not-contain: PartitionKeys sorts without
        // de-duplicating, so those two are jointly satisfied by a resumption that skips one item and
        // duplicates another.
        Assert.Equal("PK#0", firstPage.Items[0]["PK"].S);
        Assert.Equal(["PK#1", "PK#2", "PK#3", "PK#4"], PartitionKeys(pages));
    }

    // ── Single use ──────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Scan_EnumeratedTwice_ThrowsInvalidOperation(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 5);

        var paginator = client.Paginators!.Scan(new ScanRequest { TableName = TestTableName, Limit = 2 });

        _ = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);

        _ = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CollectAsync(paginator.Responses, TestContext.Current.CancellationToken));
    }

    // Consumption is marked when enumeration begins, not when it completes.
    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Scan_FirstEnumerationAbandonedPartway_SecondThrowsInvalidOperation(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 5);

        var paginator = client.Paginators!.Scan(new ScanRequest { TableName = TestTableName, Limit = 2 });

        await BeginAndAbandonAsync(paginator.Responses, TestContext.Current.CancellationToken);

        _ = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CollectAsync(paginator.Responses, TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Scan_ResponsesReadWithoutEnumerating_DoesNotConsume(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 5);

        var paginator = client.Paginators!.Scan(new ScanRequest { TableName = TestTableName, Limit = 2 });

        _ = paginator.Responses;
        _ = paginator.Responses;

        var pages = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);

        Assert.Equal(["PK#0", "PK#1", "PK#2", "PK#3", "PK#4"], PartitionKeys(pages));
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Scan_FreshPaginatorAfterConsumption_PagesNormally(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 5);

        var consumed = client.Paginators!.Scan(new ScanRequest { TableName = TestTableName, Limit = 2 });
        _ = await CollectAsync(consumed.Responses, TestContext.Current.CancellationToken);

        var fresh = client.Paginators!.Scan(new ScanRequest { TableName = TestTableName, Limit = 2 });
        var pages = await CollectAsync(fresh.Responses, TestContext.Current.CancellationToken);

        Assert.Equal([2, 2, 1], pages.Select(static p => p.Items.Count));
        Assert.Equal(["PK#0", "PK#1", "PK#2", "PK#3", "PK#4"], PartitionKeys(pages));
    }

    // ── Cancellation ────────────────────────────────────────────────

    // Pins the SDK wrapper's guard, not token propagation. PaginatedResponse<T> re-checks the token
    // after pulling each page, so this throws whether or not the paginator passes the token down.
    // Propagation is not observable through the AWS-public surface, so no test here pins it.
    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Scan_CancelledToken_SdkWrapperThrowsOperationCanceled(StoreType st)
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
