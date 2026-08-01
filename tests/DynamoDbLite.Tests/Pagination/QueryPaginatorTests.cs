using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;
using System.Diagnostics.CodeAnalysis;
using static DynamoDbLite.Tests.Pagination.PaginatorTestSupport;

namespace DynamoDbLite.Tests.Pagination;

// Query pages on the same key pair as Scan: LastEvaluatedKey out, ExclusiveStartKey in.
public sealed class QueryPaginatorTests
    : DynamoDbClientFixture
{
    private const string PartitionKey = "USER#1";

    protected override async ValueTask SetupAsync(CancellationToken ct)
    {
        await CreateTestTableAsync(Client(StoreType.DdbLite), ct);
        await CreateTestTableAsync(Client(StoreType.DdbLiteFile), ct);
    }

    private QueryRequest Request(int? limit = null, Dictionary<string, AttributeValue>? startKey = null) =>
        new()
        {
            TableName = TestTableName,
            KeyConditionExpression = "PK = :pk",
            Limit = limit,
            ExclusiveStartKey = startKey,
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = PartitionKey },
            }
        };

    private async Task SeedAsync(DynamoDbClient client, int count)
    {
        for (var i = 0; i < count; i++)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = TestTableName,
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = PartitionKey },
                    ["SK"] = new() { S = $"SK#{i}" },
                }
            }, TestContext.Current.CancellationToken);
        }
    }

    private static IReadOnlyList<string> SortKeys(IEnumerable<QueryResponse> pages) =>
        [.. pages.SelectMany(static p => p.Items).Select(static i => i["SK"].S)];

    // ── Laziness ────────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP016:Don't use disposed instance", Justification = "Constructing a paginator on a disposed client is the behavior under test: construction issues no call, so it must not throw.")]
    public void Query_OnDisposedClient_ConstructsWithoutIssuingCall(StoreType st)
    {
        var client = Client(st);
        client.Dispose();

        var paginator = client.Paginators!.Query(Request());

        Assert.NotNull(paginator);
    }

    // ── Page shape ──────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Query_NoMatchingPartition_YieldsOnePageWithNoItems(StoreType st)
    {
        var client = Client(st);

        var pages = await CollectAsync(
            client.Paginators!.Query(Request()).Responses,
            TestContext.Current.CancellationToken);

        _ = Assert.Single(pages);
        Assert.Empty(pages[0].Items);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Query_NoLimit_YieldsOnePageWithEveryItem(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 5);

        var pages = await CollectAsync(
            client.Paginators!.Query(Request()).Responses,
            TestContext.Current.CancellationToken);

        _ = Assert.Single(pages);
        Assert.Equal(5, pages[0].Items.Count);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Query_LimitAboveItemCount_YieldsOnePageWithEveryItem(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 3);

        var pages = await CollectAsync(
            client.Paginators!.Query(Request(limit: 4)).Responses,
            TestContext.Current.CancellationToken);

        _ = Assert.Single(pages);
        Assert.Equal(3, pages[0].Items.Count);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Query_LimitBelowItemCount_PagesEveryItemInSortKeyOrder(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 7);

        var pages = await CollectAsync(
            client.Paginators!.Query(Request(limit: 2)).Responses,
            TestContext.Current.CancellationToken);

        Assert.Equal([2, 2, 2, 1], pages.Select(static p => p.Items.Count));
        Assert.Equal(["SK#0", "SK#1", "SK#2", "SK#3", "SK#4", "SK#5", "SK#6"], SortKeys(pages));
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Query_LimitOfOne_YieldsAtMostOneItemPerPage(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 3);

        var pages = await CollectAsync(
            client.Paginators!.Query(Request(limit: 1)).Responses,
            TestContext.Current.CancellationToken);

        Assert.All(pages, static p => Assert.True(p.Items.Count <= 1));
        Assert.Equal(["SK#0", "SK#1", "SK#2"], SortKeys(pages));
    }

    // ── Caller-supplied start key ───────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Query_CallerSuppliedExclusiveStartKey_ResumesAfterIt(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 5);

        var firstPage = await client.QueryAsync(Request(limit: 1), TestContext.Current.CancellationToken);

        var pages = await CollectAsync(
            client.Paginators!.Query(Request(limit: 2, startKey: firstPage.LastEvaluatedKey)).Responses,
            TestContext.Current.CancellationToken);

        Assert.Equal(["SK#1", "SK#2", "SK#3", "SK#4"], SortKeys(pages));
    }

    // ── Single use ──────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Query_EnumeratedTwice_ThrowsInvalidOperation(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 5);

        var paginator = client.Paginators!.Query(Request(limit: 2));

        _ = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);

        _ = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CollectAsync(paginator.Responses, TestContext.Current.CancellationToken));
    }

    // Consumption is marked when enumeration begins, not when it completes.
    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Query_FirstEnumerationAbandonedPartway_SecondThrowsInvalidOperation(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 5);

        var paginator = client.Paginators!.Query(Request(limit: 2));

        await BeginAndAbandonAsync(paginator.Responses, TestContext.Current.CancellationToken);

        _ = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CollectAsync(paginator.Responses, TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Query_ResponsesReadWithoutEnumerating_DoesNotConsume(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 5);

        var paginator = client.Paginators!.Query(Request(limit: 2));

        _ = paginator.Responses;
        _ = paginator.Responses;

        var pages = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);

        Assert.Equal(["SK#0", "SK#1", "SK#2", "SK#3", "SK#4"], SortKeys(pages));
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Query_FreshPaginatorAfterConsumption_PagesNormally(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 5);

        var consumed = client.Paginators!.Query(Request(limit: 2));
        _ = await CollectAsync(consumed.Responses, TestContext.Current.CancellationToken);

        var fresh = client.Paginators!.Query(Request(limit: 2));
        var pages = await CollectAsync(fresh.Responses, TestContext.Current.CancellationToken);

        Assert.Equal([2, 2, 1], pages.Select(static p => p.Items.Count));
        Assert.Equal(["SK#0", "SK#1", "SK#2", "SK#3", "SK#4"], SortKeys(pages));
    }

    // ── Cancellation ────────────────────────────────────────────────

    // Pins the SDK wrapper's guard, not token propagation. PaginatedResponse<T> re-checks the token
    // after pulling each page, so this throws whether or not the paginator passes the token down.
    // Propagation is not observable through the AWS-public surface, so no test here pins it.
    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Query_CancelledToken_SdkWrapperThrowsOperationCanceled(StoreType st)
    {
        var client = Client(st);
        await SeedAsync(client, 3);

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var paginator = client.Paginators!.Query(Request());

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => CollectAsync(paginator.Responses, cts.Token));
    }

    // ── Disposal ────────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Query_EnumeratedAfterClientDisposed_ThrowsObjectDisposed(StoreType st)
    {
        var client = Client(st);
        var paginator = client.Paginators!.Query(Request());
        client.Dispose();

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(
            () => CollectAsync(paginator.Responses, TestContext.Current.CancellationToken));
    }
}
