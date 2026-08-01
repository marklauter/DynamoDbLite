using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;
using static DynamoDbLite.Tests.Pagination.PaginatorTestSupport;

namespace DynamoDbLite.Tests.Pagination;

// ListImports pages on NextToken out / NextToken in, stopping when the token comes back absent.
public abstract class ListImportsPaginatorTestsBase
    : IAsyncLifetime
{
    private const string SourceTable = "ImportPagingSource";
    private const string SourceTableArn = "arn:aws:dynamodb:local:000000000000:table/ImportPagingSource";

    private readonly string tempDir = Path.Combine(Path.GetTempPath(), $"dynamo_import_paging_{Guid.NewGuid():N}");

    protected DynamoDbClient client = null!;

    protected abstract DynamoDbClient CreateClient();

    public async ValueTask InitializeAsync()
    {
        client = CreateClient();

        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = SourceTable,
            KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
            AttributeDefinitions = [new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }]
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = SourceTable,
            Item = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "pk1" } }
        }, TestContext.Current.CancellationToken);

        var export = await client.ExportTableToPointInTimeAsync(new ExportTableToPointInTimeRequest
        {
            TableArn = SourceTableArn,
            S3Bucket = tempDir,
            ExportFormat = ExportFormat.DYNAMODB_JSON
        }, TestContext.Current.CancellationToken);

        // ExportTableToPointInTimeAsync runs the export inline, so the record is already terminal.
        // Assert that rather than polling for it: the import tests all read this export's output, and
        // a FAILED export must fail here loudly instead of becoming their silent input.
        var described = await client.DescribeExportAsync(new DescribeExportRequest
        {
            ExportArn = export.ExportDescription.ExportArn
        }, TestContext.Current.CancellationToken);

        Assert.Equal(ExportStatus.COMPLETED, described.ExportDescription.ExportStatus);
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
            // Best-effort: the import may still hold file handles.
        }

        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    private async Task SeedImportsAsync(int count)
    {
        for (var i = 0; i < count; i++)
        {
            _ = await client.ImportTableAsync(new ImportTableRequest
            {
                S3BucketSource = new S3BucketSource { S3Bucket = tempDir },
                InputFormat = InputFormat.DYNAMODB_JSON,
                TableCreationParameters = new TableCreationParameters
                {
                    TableName = $"ImportPagingTarget{i}",
                    KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
                    AttributeDefinitions = [new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }]
                }
            }, TestContext.Current.CancellationToken);
        }
    }

    private static ListImportsRequest Request(int? pageSize = null) =>
        new() { PageSize = pageSize };

    private static IEnumerable<string> Arns(ListImportsResponse page) =>
        page.ImportSummaryList.Select(static s => s.ImportArn);

    // ── Laziness ────────────────────────────────────────────────────

    [Fact]
    public void ListImports_OnDisposedClient_ConstructsWithoutIssuingCall()
    {
        client.Dispose();

        var paginator = client.Paginators!.ListImports(Request());

        Assert.NotNull(paginator);
    }

    // ── Page shape ──────────────────────────────────────────────────

    [Fact]
    public async Task ListImports_NoImports_YieldsOnePageWithNoSummaries()
    {
        var pages = await CollectAsync(
            client.Paginators!.ListImports(Request()).Responses,
            TestContext.Current.CancellationToken);

        _ = Assert.Single(pages);
        Assert.Empty(pages[0].ImportSummaryList);
    }

    [Fact]
    public async Task ListImports_NoPageSize_YieldsOnePageWithEverySummary()
    {
        await SeedImportsAsync(3);

        var pages = await CollectAsync(
            client.Paginators!.ListImports(Request()).Responses,
            TestContext.Current.CancellationToken);

        _ = Assert.Single(pages);
        Assert.Equal(3, pages[0].ImportSummaryList.Count);
    }

    [Fact]
    public async Task ListImports_PageSizeAboveImportCount_YieldsOnePage()
    {
        await SeedImportsAsync(2);

        var pages = await CollectAsync(
            client.Paginators!.ListImports(Request(pageSize: 3)).Responses,
            TestContext.Current.CancellationToken);

        _ = Assert.Single(pages);
        Assert.Equal(2, pages[0].ImportSummaryList.Count);
    }

    // Asserts only what the paging rule promises: paging terminates once NextToken comes back
    // absent, and no page exceeds PageSize.
    [Fact]
    public async Task ListImports_PageSizeBelowImportCount_TerminatesWithBoundedPages()
    {
        await SeedImportsAsync(3);

        var pages = await CollectAsync(
            client.Paginators!.ListImports(Request(pageSize: 2)).Responses,
            TestContext.Current.CancellationToken);

        Assert.True(pages.Count > 1);
        Assert.All(pages, static p => Assert.True(p.ImportSummaryList.Count <= 2));
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
    public async Task ListImports_CallerSuppliedNextToken_ResumesAfterIt()
    {
        await SeedImportsAsync(5);

        var unpaged = await client.ListImportsAsync(Request(), TestContext.Current.CancellationToken);
        var firstPage = await client.ListImportsAsync(Request(pageSize: 2), TestContext.Current.CancellationToken);
        Assert.False(string.IsNullOrEmpty(firstPage.NextToken));

        var expected = Arns(unpaged).Except(Arns(firstPage), StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
        Assert.Equal(3, expected.Length);

        var resumed = await CollectAsync(
            client.Paginators!.ListImports(new ListImportsRequest
            {
                PageSize = 2,
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
    public async Task ListImports_PageSizeBelowImportCount_YieldsEverySummaryExactlyOnce()
    {
        await SeedImportsAsync(5);

        var unpaged = await client.ListImportsAsync(Request(), TestContext.Current.CancellationToken);

        var pages = await CollectAsync(
            client.Paginators!.ListImports(Request(pageSize: 2)).Responses,
            TestContext.Current.CancellationToken);

        var paged = pages.SelectMany(Arns).Order(StringComparer.Ordinal).ToArray();

        Assert.Equal(5, unpaged.ImportSummaryList.Count);
        Assert.Equal(Arns(unpaged).Order(StringComparer.Ordinal), paged);
        Assert.Equal(paged.Length, paged.Distinct(StringComparer.Ordinal).Count());
    }

    // ── Single use ──────────────────────────────────────────────────

    [Fact]
    public async Task ListImports_EnumeratedTwice_ThrowsInvalidOperation()
    {
        await SeedImportsAsync(3);

        var paginator = client.Paginators!.ListImports(Request(pageSize: 2));

        _ = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);

        _ = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CollectAsync(paginator.Responses, TestContext.Current.CancellationToken));
    }

    // Consumption is marked when enumeration begins, not when it completes.
    [Fact]
    public async Task ListImports_FirstEnumerationAbandonedPartway_SecondThrowsInvalidOperation()
    {
        await SeedImportsAsync(3);

        var paginator = client.Paginators!.ListImports(Request(pageSize: 2));

        await BeginAndAbandonAsync(paginator.Responses, TestContext.Current.CancellationToken);

        _ = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CollectAsync(paginator.Responses, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ListImports_ResponsesReadWithoutEnumerating_DoesNotConsume()
    {
        await SeedImportsAsync(3);

        var paginator = client.Paginators!.ListImports(Request(pageSize: 2));

        _ = paginator.Responses;
        _ = paginator.Responses;

        var pages = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);

        Assert.Equal(3, pages.SelectMany(Arns).Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task ListImports_FreshPaginatorAfterConsumption_PagesNormally()
    {
        await SeedImportsAsync(3);

        var consumed = client.Paginators!.ListImports(Request(pageSize: 2));
        _ = await CollectAsync(consumed.Responses, TestContext.Current.CancellationToken);

        var fresh = client.Paginators!.ListImports(Request(pageSize: 2));
        var pages = await CollectAsync(fresh.Responses, TestContext.Current.CancellationToken);

        Assert.Equal(3, pages.SelectMany(Arns).Distinct(StringComparer.Ordinal).Count());
    }

    // ── Cancellation ────────────────────────────────────────────────

    // Pins the SDK wrapper's guard, not token propagation. PaginatedResponse<T> re-checks the token
    // after pulling each page, so this throws whether or not the paginator passes the token down.
    // Propagation is not observable through the AWS-public surface, so no test here pins it.
    [Fact]
    public async Task ListImports_CancelledToken_SdkWrapperThrowsOperationCanceled()
    {
        await SeedImportsAsync(1);

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var paginator = client.Paginators!.ListImports(Request());

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => CollectAsync(paginator.Responses, cts.Token));
    }

    // ── Disposal ────────────────────────────────────────────────────

    [Fact]
    public async Task ListImports_EnumeratedAfterClientDisposed_ThrowsObjectDisposed()
    {
        var paginator = client.Paginators!.ListImports(Request());
        client.Dispose();

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(
            () => CollectAsync(paginator.Responses, TestContext.Current.CancellationToken));
    }
}

public sealed class InMemoryListImportsPaginatorTests : ListImportsPaginatorTestsBase
{
    protected override DynamoDbClient CreateClient() =>
        new(new DynamoDbLiteOptions($"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));
}

public sealed class FileBasedListImportsPaginatorTests : ListImportsPaginatorTestsBase
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
