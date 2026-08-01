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

        for (var i = 0; i < 100; i++)
        {
            await Task.Delay(100, TestContext.Current.CancellationToken);
            var described = await client.DescribeExportAsync(new DescribeExportRequest
            {
                ExportArn = export.ExportDescription.ExportArn
            }, TestContext.Current.CancellationToken);

            if (described.ExportDescription.ExportStatus != ExportStatus.IN_PROGRESS)
                break;
        }
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

    // ── Re-enumeration ──────────────────────────────────────────────

    [Fact]
    public async Task ListImports_ReEnumerated_RestartsAndYieldsSamePages()
    {
        await SeedImportsAsync(3);

        var paginator = client.Paginators!.ListImports(Request(pageSize: 2));

        var first = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);
        var second = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);

        Assert.Equal(
            first.Select(static p => p.ImportSummaryList.Select(static s => s.ImportArn)),
            second.Select(static p => p.ImportSummaryList.Select(static s => s.ImportArn)));
    }

    // ── Cancellation ────────────────────────────────────────────────

    [Fact]
    public async Task ListImports_CancelledToken_ThrowsOperationCanceled()
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
