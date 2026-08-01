using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;
using System.Diagnostics.CodeAnalysis;
using static DynamoDbLite.Tests.Pagination.PaginatorTestSupport;

namespace DynamoDbLite.Tests.Pagination;

// BatchGetItem pages on UnprocessedKeys out / RequestItems in. DynamoDbLite has no throttling to
// report, so it never populates UnprocessedKeys and the enumeration is always exactly one page.
public sealed class BatchGetItemPaginatorTests
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

    private BatchGetItemRequest Request(params int[] indices) =>
        new()
        {
            RequestItems = new Dictionary<string, KeysAndAttributes>
            {
                [TestTableName] = new()
                {
                    Keys = [.. indices.Select(static i => new Dictionary<string, AttributeValue>
                    {
                        ["PK"] = new() { S = $"PK#{i}" },
                        ["SK"] = new() { S = $"SK#{i}" },
                    })],
                }
            }
        };

    private IReadOnlyList<string> PartitionKeys(IEnumerable<BatchGetItemResponse> pages) =>
        [.. pages
            .Where(p => p.Responses.ContainsKey(TestTableName))
            .SelectMany(p => p.Responses[TestTableName])
            .Select(static i => i["PK"].S)
            .Order(StringComparer.Ordinal)];

    // ── Laziness ────────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP016:Don't use disposed instance", Justification = "Constructing a paginator on a disposed client is the behavior under test: construction issues no call, so it must not throw.")]
    public void BatchGetItem_OnDisposedClient_ConstructsWithoutIssuingCall(StoreType st)
    {
        var client = Client(st);
        client.Dispose();

        var paginator = client.Paginators!.BatchGetItem(Request(0));

        Assert.NotNull(paginator);
    }

    // ── Page shape ──────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchGetItem_MatchingKeys_YieldsExactlyOnePageWithEveryItem(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 3);

        var pages = await CollectAsync(
            client.Paginators!.BatchGetItem(Request(0, 1, 2)).Responses,
            TestContext.Current.CancellationToken);

        _ = Assert.Single(pages);
        Assert.Equal(["PK#0", "PK#1", "PK#2"], PartitionKeys(pages));
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchGetItem_SingleKey_YieldsExactlyOnePage(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 3);

        var pages = await CollectAsync(
            client.Paginators!.BatchGetItem(Request(1)).Responses,
            TestContext.Current.CancellationToken);

        _ = Assert.Single(pages);
        Assert.Equal(["PK#1"], PartitionKeys(pages));
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchGetItem_NoMatchingKeys_YieldsExactlyOnePageWithNoItems(StoreType st)
    {
        var client = Client(st);

        var pages = await CollectAsync(
            client.Paginators!.BatchGetItem(Request(0, 1)).Responses,
            TestContext.Current.CancellationToken);

        _ = Assert.Single(pages);
        Assert.Empty(PartitionKeys(pages));
    }

    // ── Re-enumeration ──────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchGetItem_ReEnumerated_RestartsAndYieldsSamePages(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 3);

        var paginator = client.Paginators!.BatchGetItem(Request(0, 1, 2));

        var first = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);
        var second = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);

        Assert.Equal(first.Count, second.Count);
        Assert.Equal(PartitionKeys(first), PartitionKeys(second));
    }

    // ── Cancellation ────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchGetItem_CancelledToken_ThrowsOperationCanceled(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 3);

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var paginator = client.Paginators!.BatchGetItem(Request(0, 1, 2));

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => CollectAsync(paginator.Responses, cts.Token));
    }

    // ── Disposal ────────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchGetItem_EnumeratedAfterClientDisposed_ThrowsObjectDisposed(StoreType st)
    {
        var client = Client(st);
        var paginator = client.Paginators!.BatchGetItem(Request(0));
        client.Dispose();

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(
            () => CollectAsync(paginator.Responses, TestContext.Current.CancellationToken));
    }
}
