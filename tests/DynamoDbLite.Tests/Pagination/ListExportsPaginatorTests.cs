using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;
using static DynamoDbLite.Tests.Pagination.PaginatorTestSupport;

namespace DynamoDbLite.Tests.Pagination;

// ListExports pages on NextToken out / NextToken in, stopping when the token comes back absent.
public abstract class ListExportsPaginatorTestsBase
    : IAsyncLifetime
{
    private const string TableName = "ExportPagingTable";
    private const string TableArn = "arn:aws:dynamodb:local:000000000000:table/ExportPagingTable";

    private readonly string tempDir = Path.Combine(Path.GetTempPath(), $"dynamo_export_paging_{Guid.NewGuid():N}");

    protected DynamoDbClient client = null!;

    protected abstract DynamoDbClient CreateClient();

    public async ValueTask InitializeAsync()
    {
        client = CreateClient();
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = TableName,
            KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
            AttributeDefinitions = [new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }]
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = TableName,
            Item = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "pk1" } }
        }, TestContext.Current.CancellationToken);
    }

    public virtual ValueTask DisposeAsync()
    {
        client.Dispose();
        try
        {
            if (Directory.Exists(tempDir))
                Directory.Delete(tempDir, true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Best-effort: the export may still hold file handles.
        }

        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    private async Task SeedExportsAsync(int count)
    {
        for (var i = 0; i < count; i++)
        {
            _ = await client.ExportTableToPointInTimeAsync(new ExportTableToPointInTimeRequest
            {
                TableArn = TableArn,
                S3Bucket = tempDir,
                ExportFormat = ExportFormat.DYNAMODB_JSON
            }, TestContext.Current.CancellationToken);
        }
    }

    private static ListExportsRequest Request(int? maxResults = null) =>
        new() { TableArn = TableArn, MaxResults = maxResults };

    private static IEnumerable<string> Arns(ListExportsResponse page) =>
        page.ExportSummaries.Select(static s => s.ExportArn);

    // ── Laziness ────────────────────────────────────────────────────

    [Fact]
    public void ListExports_OnDisposedClient_ConstructsWithoutIssuingCall()
    {
        client.Dispose();

        var paginator = client.Paginators!.ListExports(Request());

        Assert.NotNull(paginator);
    }

    // ── Page shape ──────────────────────────────────────────────────

    [Fact]
    public async Task ListExports_NoExports_YieldsOnePageWithNoSummaries()
    {
        var pages = await CollectAsync(
            client.Paginators!.ListExports(Request()).Responses,
            TestContext.Current.CancellationToken);

        _ = Assert.Single(pages);
        Assert.Empty(pages[0].ExportSummaries);
    }

    [Fact]
    public async Task ListExports_NoMaxResults_YieldsOnePageWithEverySummary()
    {
        await SeedExportsAsync(3);

        var pages = await CollectAsync(
            client.Paginators!.ListExports(Request()).Responses,
            TestContext.Current.CancellationToken);

        _ = Assert.Single(pages);
        Assert.Equal(3, pages[0].ExportSummaries.Count);
    }

    [Fact]
    public async Task ListExports_MaxResultsAboveExportCount_YieldsOnePage()
    {
        await SeedExportsAsync(2);

        var pages = await CollectAsync(
            client.Paginators!.ListExports(Request(maxResults: 3)).Responses,
            TestContext.Current.CancellationToken);

        _ = Assert.Single(pages);
        Assert.Equal(2, pages[0].ExportSummaries.Count);
    }

    // Asserts only what the paging rule promises: paging terminates once NextToken comes back
    // absent, and no page exceeds MaxResults. ListExports' token is row-ordinal based while its
    // ordering is by start time, so which summaries land on which page is the operation's business.
    [Fact]
    public async Task ListExports_MaxResultsBelowExportCount_TerminatesWithBoundedPages()
    {
        await SeedExportsAsync(3);

        var pages = await CollectAsync(
            client.Paginators!.ListExports(Request(maxResults: 2)).Responses,
            TestContext.Current.CancellationToken);

        Assert.True(pages.Count > 1);
        Assert.All(pages, static p => Assert.True(p.ExportSummaries.Count <= 2));
        Assert.True(string.IsNullOrEmpty(pages[^1].NextToken));
    }

    // ── Caller-supplied NextToken ───────────────────────────────────

    // The first request carries whatever token the caller supplied, so a paginator handed the token
    // from page one delivers exactly the remainder: the whole set minus the summaries page one
    // already returned, none re-read and none skipped. Asserted on content rather than on the
    // resumed page merely differing from an unresumed one, which a cursor that both duplicates and
    // skips would also satisfy. Compared as sorted sequences: summaries sharing a start_time have no
    // defined relative order.
    [Fact]
    public async Task ListExports_CallerSuppliedNextToken_ResumesAfterIt()
    {
        await SeedExportsAsync(5);

        var unpaged = await client.ListExportsAsync(Request(), TestContext.Current.CancellationToken);
        var firstPage = await client.ListExportsAsync(Request(maxResults: 2), TestContext.Current.CancellationToken);
        Assert.False(string.IsNullOrEmpty(firstPage.NextToken));

        var expected = Arns(unpaged).Except(Arns(firstPage), StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
        Assert.Equal(3, expected.Length);

        var resumed = await CollectAsync(
            client.Paginators!.ListExports(new ListExportsRequest
            {
                TableArn = TableArn,
                MaxResults = 2,
                NextToken = firstPage.NextToken,
            }).Responses,
            TestContext.Current.CancellationToken);

        var delivered = resumed.SelectMany(Arns).Order(StringComparer.Ordinal).ToArray();

        Assert.Equal(expected, delivered);
        Assert.Equal(delivered.Length, delivered.Distinct(StringComparer.Ordinal).Count());
    }

    // ── Exactly-once delivery ───────────────────────────────────────

    // Enumerating to completion yields every summary the unpaged call returns, each exactly once.
    // Compared as sorted sequences: summaries sharing a start_time have no defined relative order,
    // which is a known limitation and deliberately not asserted against.
    [Fact]
    public async Task ListExports_MaxResultsBelowExportCount_YieldsEverySummaryExactlyOnce()
    {
        await SeedExportsAsync(5);

        var unpaged = await client.ListExportsAsync(Request(), TestContext.Current.CancellationToken);

        var pages = await CollectAsync(
            client.Paginators!.ListExports(Request(maxResults: 2)).Responses,
            TestContext.Current.CancellationToken);

        var paged = pages.SelectMany(Arns).Order(StringComparer.Ordinal).ToArray();

        Assert.Equal(5, unpaged.ExportSummaries.Count);
        Assert.Equal(Arns(unpaged).Order(StringComparer.Ordinal), paged);
        Assert.Equal(paged.Length, paged.Distinct(StringComparer.Ordinal).Count());
    }

    // ── Single use ──────────────────────────────────────────────────

    [Fact]
    public async Task ListExports_EnumeratedTwice_ThrowsInvalidOperation()
    {
        await SeedExportsAsync(3);

        var paginator = client.Paginators!.ListExports(Request(maxResults: 2));

        _ = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);

        _ = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CollectAsync(paginator.Responses, TestContext.Current.CancellationToken));
    }

    // Consumption is marked when enumeration begins, not when it completes.
    [Fact]
    public async Task ListExports_FirstEnumerationAbandonedPartway_SecondThrowsInvalidOperation()
    {
        await SeedExportsAsync(3);

        var paginator = client.Paginators!.ListExports(Request(maxResults: 2));

        await BeginAndAbandonAsync(paginator.Responses, TestContext.Current.CancellationToken);

        _ = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CollectAsync(paginator.Responses, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ListExports_ResponsesReadWithoutEnumerating_DoesNotConsume()
    {
        await SeedExportsAsync(3);

        var paginator = client.Paginators!.ListExports(Request(maxResults: 2));

        _ = paginator.Responses;
        _ = paginator.Responses;

        var pages = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);

        Assert.Equal(3, pages.SelectMany(Arns).Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task ListExports_FreshPaginatorAfterConsumption_PagesNormally()
    {
        await SeedExportsAsync(3);

        var consumed = client.Paginators!.ListExports(Request(maxResults: 2));
        _ = await CollectAsync(consumed.Responses, TestContext.Current.CancellationToken);

        var fresh = client.Paginators!.ListExports(Request(maxResults: 2));
        var pages = await CollectAsync(fresh.Responses, TestContext.Current.CancellationToken);

        Assert.Equal(3, pages.SelectMany(Arns).Distinct(StringComparer.Ordinal).Count());
    }

    // ── Cancellation ────────────────────────────────────────────────

    // Pins the SDK wrapper's guard, not token propagation. PaginatedResponse<T> re-checks the token
    // after pulling each page, so this throws whether or not the paginator passes the token down.
    // Propagation is not observable through the AWS-public surface, so no test here pins it.
    [Fact]
    public async Task ListExports_CancelledToken_SdkWrapperThrowsOperationCanceled()
    {
        await SeedExportsAsync(1);

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var paginator = client.Paginators!.ListExports(Request());

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => CollectAsync(paginator.Responses, cts.Token));
    }

    // ── Disposal ────────────────────────────────────────────────────

    [Fact]
    public async Task ListExports_EnumeratedAfterClientDisposed_ThrowsObjectDisposed()
    {
        var paginator = client.Paginators!.ListExports(Request());
        client.Dispose();

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(
            () => CollectAsync(paginator.Responses, TestContext.Current.CancellationToken));
    }
}

public sealed class InMemoryListExportsPaginatorTests : ListExportsPaginatorTestsBase
{
    protected override DynamoDbClient CreateClient() =>
        new(new DynamoDbLiteOptions($"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));
}

public sealed class FileBasedListExportsPaginatorTests : ListExportsPaginatorTestsBase
{
    private string? dbPath;

    protected override DynamoDbClient CreateClient()
    {
        var (c, path) = FileBasedTestHelper.CreateFileBasedClient();
        dbPath = path;
        return c;
    }

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        FileBasedTestHelper.Cleanup(dbPath);
    }
}
