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

    // ── Re-enumeration ──────────────────────────────────────────────

    [Fact]
    public async Task ListExports_ReEnumerated_RestartsAndYieldsSamePages()
    {
        await SeedExportsAsync(3);

        var paginator = client.Paginators!.ListExports(Request(maxResults: 2));

        var first = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);
        var second = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);

        Assert.Equal(
            first.Select(static p => p.ExportSummaries.Select(static s => s.ExportArn)),
            second.Select(static p => p.ExportSummaries.Select(static s => s.ExportArn)));
    }

    // ── Cancellation ────────────────────────────────────────────────

    [Fact]
    public async Task ListExports_CancelledToken_ThrowsOperationCanceled()
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
