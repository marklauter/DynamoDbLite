using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;
using System.Diagnostics.CodeAnalysis;
using static DynamoDbLite.Tests.Pagination.PaginatorTestSupport;

namespace DynamoDbLite.Tests.Pagination;

// ListTables pages on LastEvaluatedTableName out / ExclusiveStartTableName in, and is the only
// surface here carrying a result-key property: TableNames flattens every page in page order.
public sealed class ListTablesPaginatorTests
    : DynamoDbClientFixture
{
    private static readonly string[] TableNames = ["PagedTableA", "PagedTableB", "PagedTableC"];

    protected override async ValueTask SetupAsync(CancellationToken ct)
    {
        foreach (var name in TableNames)
        {
            await CreateHashOnlyTableAsync(Client(StoreType.DdbLite), name, ct);
            await CreateHashOnlyTableAsync(Client(StoreType.DdbLiteFile), name, ct);
        }
    }

    // ── Laziness ────────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP016:Don't use disposed instance", Justification = "Constructing a paginator on a disposed client is the behavior under test: construction issues no call, so it must not throw.")]
    public void ListTables_OnDisposedClient_ConstructsWithoutIssuingCall(StoreType st)
    {
        var client = Client(st);
        client.Dispose();

        var paginator = client.Paginators!.ListTables(new ListTablesRequest());

        Assert.NotNull(paginator);
    }

    // ── Page shape ──────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task ListTables_NoLimit_YieldsOnePageWithEveryTableName(StoreType st)
    {
        var client = Client(st);

        var pages = await CollectAsync(
            client.Paginators!.ListTables(new ListTablesRequest()).Responses,
            TestContext.Current.CancellationToken);

        _ = Assert.Single(pages);
        Assert.Equal(TableNames, pages[0].TableNames);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task ListTables_LimitAboveTableCount_YieldsOnePage(StoreType st)
    {
        var client = Client(st);

        var pages = await CollectAsync(
            client.Paginators!.ListTables(new ListTablesRequest { Limit = 4 }).Responses,
            TestContext.Current.CancellationToken);

        _ = Assert.Single(pages);
        Assert.Equal(TableNames, pages[0].TableNames);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task ListTables_LimitOfOne_YieldsOneNamePerPage(StoreType st)
    {
        var client = Client(st);

        var pages = await CollectAsync(
            client.Paginators!.ListTables(new ListTablesRequest { Limit = 1 }).Responses,
            TestContext.Current.CancellationToken);

        Assert.Equal(3, pages.Count);
        Assert.All(pages, static p => Assert.Single(p.TableNames));
    }

    // ── Caller-supplied start token ─────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task ListTables_CallerSuppliedExclusiveStartTableName_ResumesAfterIt(StoreType st)
    {
        var client = Client(st);

        var names = await CollectAsync(
            client.Paginators!.ListTables(new ListTablesRequest
            {
                Limit = 1,
                ExclusiveStartTableName = TableNames[0],
            }).TableNames,
            TestContext.Current.CancellationToken);

        Assert.Equal([TableNames[1], TableNames[2]], names);
    }

    // ── Result key: TableNames ──────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task TableNames_NoLimit_YieldsEveryTableName(StoreType st)
    {
        var client = Client(st);

        var names = await CollectAsync(
            client.Paginators!.ListTables(new ListTablesRequest()).TableNames,
            TestContext.Current.CancellationToken);

        Assert.Equal(TableNames, names);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task TableNames_LimitOfOne_FlattensEveryPageInPageOrder(StoreType st)
    {
        var client = Client(st);

        var names = await CollectAsync(
            client.Paginators!.ListTables(new ListTablesRequest { Limit = 1 }).TableNames,
            TestContext.Current.CancellationToken);

        Assert.Equal(TableNames, names);
    }

    // ── Re-enumeration ──────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task ListTables_ResponsesReEnumerated_RestartsAndYieldsSamePages(StoreType st)
    {
        var client = Client(st);
        var paginator = client.Paginators!.ListTables(new ListTablesRequest { Limit = 1 });

        var first = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);
        var second = await CollectAsync(paginator.Responses, TestContext.Current.CancellationToken);

        Assert.Equal(
            first.Select(static p => p.TableNames),
            second.Select(static p => p.TableNames));
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task TableNames_ReEnumerated_RestartsAndYieldsSameNames(StoreType st)
    {
        var client = Client(st);
        var paginator = client.Paginators!.ListTables(new ListTablesRequest { Limit = 1 });

        var first = await CollectAsync(paginator.TableNames, TestContext.Current.CancellationToken);
        var second = await CollectAsync(paginator.TableNames, TestContext.Current.CancellationToken);

        Assert.Equal(TableNames, first);
        Assert.Equal(TableNames, second);
    }

    // ── Cancellation ────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task ListTables_CancelledToken_ThrowsOperationCanceled(StoreType st)
    {
        var client = Client(st);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var paginator = client.Paginators!.ListTables(new ListTablesRequest());

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => CollectAsync(paginator.Responses, cts.Token));
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task TableNames_CancelledToken_ThrowsOperationCanceled(StoreType st)
    {
        var client = Client(st);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var paginator = client.Paginators!.ListTables(new ListTablesRequest());

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => CollectAsync(paginator.TableNames, cts.Token));
    }

    // ── Disposal ────────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task ListTables_EnumeratedAfterClientDisposed_ThrowsObjectDisposed(StoreType st)
    {
        var client = Client(st);
        var paginator = client.Paginators!.ListTables(new ListTablesRequest());
        client.Dispose();

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(
            () => CollectAsync(paginator.Responses, TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task TableNames_EnumeratedAfterClientDisposed_ThrowsObjectDisposed(StoreType st)
    {
        var client = Client(st);
        var paginator = client.Paginators!.ListTables(new ListTablesRequest());
        client.Dispose();

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(
            () => CollectAsync(paginator.TableNames, TestContext.Current.CancellationToken));
    }
}
