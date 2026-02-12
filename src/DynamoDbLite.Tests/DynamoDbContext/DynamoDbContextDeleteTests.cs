using DynamoDbLite.Tests.Fixtures;
using DynamoDbLite.Tests.Models;

namespace DynamoDbLite.Tests.DynamoDbContext;

public abstract class DynamoDbContextDeleteTests
    : DynamoDbContextFixture
{
    [Fact]
    public async Task DeleteAsync_ExistingItem_RemovesItem()
    {
        var item = new SimpleItem { Id = "del-1", Name = "ToDelete" };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        await context.DeleteAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<SimpleItem>("del-1", TestContext.Current.CancellationToken);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task DeleteAsync_ByHashKey_RemovesItem()
    {
        await context.SaveAsync(new SimpleItem { Id = "del-2", Name = "ToDelete" }, TestContext.Current.CancellationToken);

        await context.DeleteAsync<SimpleItem>("del-2", TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<SimpleItem>("del-2", TestContext.Current.CancellationToken);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task DeleteAsync_ByCompositeKey_RemovesItem()
    {
        await context.SaveAsync(new CompositeKeyItem { PK = "del-pk", SK = "del-sk" }, TestContext.Current.CancellationToken);

        await context.DeleteAsync<CompositeKeyItem>("del-pk", "del-sk", TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CompositeKeyItem>("del-pk", "del-sk", TestContext.Current.CancellationToken);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task DeleteAsync_NonExistentItem_Succeeds()
    {
        await context.DeleteAsync<SimpleItem>("never-existed", TestContext.Current.CancellationToken);
    }
}
