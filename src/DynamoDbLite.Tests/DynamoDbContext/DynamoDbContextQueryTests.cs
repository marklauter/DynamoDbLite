using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using DynamoDbLite.Tests.Fixtures;
using DynamoDbLite.Tests.Models;

namespace DynamoDbLite.Tests.DynamoDbContext;

public abstract class DynamoDbContextQueryTests
    : DynamoDbContextFixture
{
    [Fact]
    public async Task QueryAsync_ByHashKey_ReturnsAllItems()
    {
        var ct = TestContext.Current.CancellationToken;
        await context.SaveAsync(new CompositeKeyItem { PK = "q-pk", SK = "a" }, ct);
        await context.SaveAsync(new CompositeKeyItem { PK = "q-pk", SK = "b" }, ct);
        await context.SaveAsync(new CompositeKeyItem { PK = "q-pk", SK = "c" }, ct);
        await context.SaveAsync(new CompositeKeyItem { PK = "other", SK = "x" }, ct);

        var query = context.QueryAsync<CompositeKeyItem>("q-pk");
        var results = await query.GetRemainingAsync(ct);

        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task QueryAsync_ByHashKey_EmptyResults_ReturnsEmptyList()
    {
        var query = context.QueryAsync<CompositeKeyItem>("no-items-here");
        var results = await query.GetRemainingAsync(TestContext.Current.CancellationToken);

        Assert.Empty(results);
    }

    [Fact]
    public async Task QueryAsync_WithPagination_ReturnsPages()
    {
        var ct = TestContext.Current.CancellationToken;
        for (var i = 0; i < 5; i++)
            await context.SaveAsync(new CompositeKeyItem { PK = "q-page", SK = $"item-{i:D2}" }, ct);

        var query = context.QueryAsync<CompositeKeyItem>("q-page");
        var page = await query.GetNextSetAsync(ct);

        Assert.True(page.Count > 0);
        Assert.True(page.Count <= 5);
    }

    [Fact]
    public async Task QueryAsync_AllPages_ReturnsAllItems()
    {
        var ct = TestContext.Current.CancellationToken;
        for (var i = 0; i < 5; i++)
            await context.SaveAsync(new CompositeKeyItem { PK = "q-all", SK = $"item-{i:D2}" }, ct);

        var query = context.QueryAsync<CompositeKeyItem>("q-all");
        var results = await query.GetRemainingAsync(ct);

        Assert.Equal(5, results.Count);
    }

    [Fact]
    public async Task QueryAsync_BackwardDirection_ReturnsDescending()
    {
        var ct = TestContext.Current.CancellationToken;
        await context.SaveAsync(new CompositeKeyItem { PK = "q-back", SK = "a" }, ct);
        await context.SaveAsync(new CompositeKeyItem { PK = "q-back", SK = "b" }, ct);
        await context.SaveAsync(new CompositeKeyItem { PK = "q-back", SK = "c" }, ct);

        var query = context.QueryAsync<CompositeKeyItem>("q-back", new QueryConfig { BackwardQuery = true });
        var results = await query.GetRemainingAsync(ct);

        Assert.Equal(3, results.Count);
        Assert.Equal("c", results[0].SK);
        Assert.Equal("b", results[1].SK);
        Assert.Equal("a", results[2].SK);
    }

    [Fact]
    public async Task QueryAsync_WithRangeKeyCondition_FiltersOnSortKey()
    {
        var ct = TestContext.Current.CancellationToken;
        await context.SaveAsync(new CompositeKeyItem { PK = "q-range", SK = "2025-01-01" }, ct);
        await context.SaveAsync(new CompositeKeyItem { PK = "q-range", SK = "2025-06-01" }, ct);
        await context.SaveAsync(new CompositeKeyItem { PK = "q-range", SK = "2025-12-01" }, ct);

        var query = context.QueryAsync<CompositeKeyItem>(
            "q-range",
            QueryOperator.BeginsWith,
            ["2025-0"]);
        var results = await query.GetRemainingAsync(ct);

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task QueryAsync_OnGsi_ReturnsCorrectItems()
    {
        var ct = TestContext.Current.CancellationToken;
        await context.SaveAsync(new GsiItem { PK = "gsi-pk-1", SK = "a", GsiPK = "gsi-hash", GsiSK = "gsi-1", Data = "first" }, ct);
        await context.SaveAsync(new GsiItem { PK = "gsi-pk-2", SK = "b", GsiPK = "gsi-hash", GsiSK = "gsi-2", Data = "second" }, ct);
        await context.SaveAsync(new GsiItem { PK = "gsi-pk-3", SK = "c", GsiPK = "other-gsi", GsiSK = "gsi-3", Data = "third" }, ct);

        var query = context.QueryAsync<GsiItem>("gsi-hash", new QueryConfig { IndexName = "GsiIndex" });
        var results = await query.GetRemainingAsync(ct);

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal("gsi-hash", r.GsiPK));
    }
}
