using DynamoDbLite.Tests.Fixtures;
using DynamoDbLite.Tests.Models;

namespace DynamoDbLite.Tests.DynamoDbContext;

public sealed class DynamoDbContextEdgeCaseTests
    : DynamoDbContextFixture
{
    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task SaveAsync_EmptyStringProperty_HandledCorrectly(StoreType st)
    {
        var context = Context(st);
        var ct = TestContext.Current.CancellationToken;

        await context.SaveAsync(new SimpleItem { Id = "empty-str", Name = "" }, ct);

        var loaded = await context.LoadAsync<SimpleItem>("empty-str", ct);
        Assert.NotNull(loaded);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task SaveAsync_VeryLargeItem_Succeeds(StoreType st)
    {
        var context = Context(st);
        var ct = TestContext.Current.CancellationToken;

        var largeString = new string('x', 100_000);
        await context.SaveAsync(new SimpleItem { Id = "large-1", Name = largeString }, ct);

        var loaded = await context.LoadAsync<SimpleItem>("large-1", ct);
        Assert.Equal(100_000, loaded!.Name.Length);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task LoadAsync_AfterDelete_ReturnsNull(StoreType st)
    {
        var context = Context(st);
        var ct = TestContext.Current.CancellationToken;

        await context.SaveAsync(new SimpleItem { Id = "del-reload", Name = "Exists" }, ct);
        await context.DeleteAsync<SimpleItem>("del-reload", ct);

        var loaded = await context.LoadAsync<SimpleItem>("del-reload", ct);
        Assert.Null(loaded);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_GetRemainingAsync_ReturnsAll(StoreType st)
    {
        var context = Context(st);
        var ct = TestContext.Current.CancellationToken;

        for (var i = 0; i < 10; i++)
            await context.SaveAsync(new CompositeKeyItem { PK = "q-remaining", SK = $"item-{i:D2}" }, ct);

        var query = context.QueryAsync<CompositeKeyItem>("q-remaining");
        var results = await query.GetRemainingAsync(ct);

        Assert.Equal(10, results.Count);
    }
}
