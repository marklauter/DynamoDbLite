using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;
using Microsoft.Data.Sqlite;
using static DynamoDbLite.Tests.Pagination.PaginatorTestSupport;

namespace DynamoDbLite.Tests.Pagination;

// ListExports and ListImports sort by start_time, which is not unique. A cursor on start_time alone
// skips every record sharing the token's timestamp, so both page on (start_time, arn) — arn is the
// primary key, which makes the pair a total order.
//
// Ties do not occur naturally: start_time carries tick resolution and each record is written with
// file I/O in between. These tests force them on disk, because a cursor that is only ever exercised
// against distinct timestamps is a cursor whose tie behavior nobody has tested.
//
// File-backed by necessity — collapsing the timestamps means writing to the store's database.
public sealed class ListPaginationTieTests
    : IAsyncLifetime
{
    private const string TableName = "TiePagingTable";
    private const string TableArn = "arn:aws:dynamodb:local:000000000000:table/TiePagingTable";
    private const int RecordCount = 6;

    private readonly string tempDir = Path.Combine(Path.GetTempPath(), $"dynamo_tie_paging_{Guid.NewGuid():N}");

    private DynamoDbClient client = null!;
    private string dbPath = null!;

    public async ValueTask InitializeAsync()
    {
        (client, dbPath) = FileBasedTestHelper.CreateFileBasedClient();

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

    public async ValueTask DisposeAsync()
    {
        client.Dispose();
        FileBasedTestHelper.Cleanup(dbPath);

        try
        {
            if (Directory.Exists(tempDir))
                Directory.Delete(tempDir, true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Best-effort: an export or import may still hold file handles.
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task ListExports_EveryRecordSharingATimestamp_IsDeliveredExactlyOnce(int maxResults)
    {
        await SeedExportsAsync(RecordCount);
        await CollapseExportTimestampsAsync();

        var unpaged = await client.ListExportsAsync(new ListExportsRequest(), TestContext.Current.CancellationToken);
        var expected = unpaged.ExportSummaries.Select(static s => s.ExportArn).Order(StringComparer.Ordinal).ToArray();
        Assert.Equal(RecordCount, expected.Length);

        var pages = await CollectAsync(
            client.Paginators!.ListExports(new ListExportsRequest { MaxResults = maxResults }).Responses,
            TestContext.Current.CancellationToken);

        var delivered = pages.SelectMany(static p => p.ExportSummaries).Select(static s => s.ExportArn).ToArray();

        Assert.Equal(expected, delivered.Order(StringComparer.Ordinal));
        Assert.Equal(delivered.Length, delivered.Distinct(StringComparer.Ordinal).Count());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task ListImports_EveryRecordSharingATimestamp_IsDeliveredExactlyOnce(int pageSize)
    {
        await SeedImportsAsync(RecordCount);
        await CollapseImportTimestampsAsync();

        var unpaged = await client.ListImportsAsync(new ListImportsRequest(), TestContext.Current.CancellationToken);
        var expected = unpaged.ImportSummaryList.Select(static s => s.ImportArn).Order(StringComparer.Ordinal).ToArray();
        Assert.Equal(RecordCount, expected.Length);

        var pages = await CollectAsync(
            client.Paginators!.ListImports(new ListImportsRequest { PageSize = pageSize }).Responses,
            TestContext.Current.CancellationToken);

        var delivered = pages.SelectMany(static p => p.ImportSummaryList).Select(static s => s.ImportArn).ToArray();

        Assert.Equal(expected, delivered.Order(StringComparer.Ordinal));
        Assert.Equal(delivered.Length, delivered.Distinct(StringComparer.Ordinal).Count());
    }

    // A page that exactly fills the request and exhausts the listing must end it. Emitting a token
    // there costs a round trip and hands the caller a page with nothing in it.
    [Fact]
    public async Task ListExports_PageSizeDividesTheRecordCount_YieldsNoTrailingEmptyPage()
    {
        await SeedExportsAsync(RecordCount);

        var pages = await CollectAsync(
            client.Paginators!.ListExports(new ListExportsRequest { MaxResults = 3 }).Responses,
            TestContext.Current.CancellationToken);

        Assert.Equal(2, pages.Count);
        Assert.All(pages, static p => Assert.Equal(3, p.ExportSummaries.Count));
        Assert.True(string.IsNullOrEmpty(pages[^1].NextToken));
    }

    [Fact]
    public async Task ListImports_PageSizeDividesTheRecordCount_YieldsNoTrailingEmptyPage()
    {
        await SeedImportsAsync(RecordCount);

        var pages = await CollectAsync(
            client.Paginators!.ListImports(new ListImportsRequest { PageSize = 3 }).Responses,
            TestContext.Current.CancellationToken);

        Assert.Equal(2, pages.Count);
        Assert.All(pages, static p => Assert.Equal(3, p.ImportSummaryList.Count));
        Assert.True(string.IsNullOrEmpty(pages[^1].NextToken));
    }

    // Collapses every start_time onto two values, splitting the records evenly between them. Two tie
    // groups rather than one, so the cursor has to cross a boundary as well as walk within a group.
    // The SQL is written out per table rather than interpolated: table and column names cannot be
    // parameters, and a literal keeps the statement constant.
    private const string CollapseExportsSql = """
        WITH ranked AS (
            SELECT export_arn AS arn, ROW_NUMBER() OVER (ORDER BY export_arn) AS rn FROM exports
        )
        UPDATE exports SET start_time = (
            SELECT CASE WHEN r.rn <= 3
                        THEN '2026-01-01T00:00:00.0000000Z'
                        ELSE '2026-01-02T00:00:00.0000000Z' END
            FROM ranked r WHERE r.arn = exports.export_arn)
        """;

    private const string CollapseImportsSql = """
        WITH ranked AS (
            SELECT import_arn AS arn, ROW_NUMBER() OVER (ORDER BY import_arn) AS rn FROM imports
        )
        UPDATE imports SET start_time = (
            SELECT CASE WHEN r.rn <= 3
                        THEN '2026-01-01T00:00:00.0000000Z'
                        ELSE '2026-01-02T00:00:00.0000000Z' END
            FROM ranked r WHERE r.arn = imports.import_arn)
        """;

    // The SQL is assigned from a constant at each site rather than passed in: CA2100 reads the
    // assignment, and a value arriving through a parameter is not provably constant to it.
    private async Task CollapseExportTimestampsAsync()
    {
        using var connection = new SqliteConnection($"Data Source={dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using (var command = connection.CreateCommand())
        {
            command.CommandText = CollapseExportsSql;
            _ = await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        using var check = connection.CreateCommand();
        check.CommandText = "SELECT COUNT(DISTINCT start_time) FROM exports";
        Assert.Equal(2L, await check.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    private async Task CollapseImportTimestampsAsync()
    {
        using var connection = new SqliteConnection($"Data Source={dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using (var command = connection.CreateCommand())
        {
            command.CommandText = CollapseImportsSql;
            _ = await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        using var check = connection.CreateCommand();
        check.CommandText = "SELECT COUNT(DISTINCT start_time) FROM imports";
        Assert.Equal(2L, await check.ExecuteScalarAsync(TestContext.Current.CancellationToken));
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
                    TableName = $"TiePagingTarget{i}",
                    KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
                    AttributeDefinitions = [new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }]
                }
            }, TestContext.Current.CancellationToken);
        }
    }
}
