using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;
using DynamoDbLite.Tests.Models;

namespace DynamoDbLite.Tests.DynamoDbContext;

public sealed class DynamoDbContextVersioningTests
    : DynamoDbContextFixture
{
    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task SaveAsync_NewVersionedItem_SetsVersionToZero(StoreType st)
    {
        var context = Context(st);

        var item = new VersionedItem { Id = "v-new", Data = "initial" };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<VersionedItem>("v-new", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal(0, loaded.VersionNumber);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task SaveAsync_ExistingVersionedItem_IncrementsVersion(StoreType st)
    {
        var context = Context(st);
        var ct = TestContext.Current.CancellationToken;

        var item = new VersionedItem { Id = "v-inc", Data = "v0" };
        await context.SaveAsync(item, ct);

        var loaded = await context.LoadAsync<VersionedItem>("v-inc", ct);
        loaded!.Data = "v1";
        await context.SaveAsync(loaded, ct);

        var loaded2 = await context.LoadAsync<VersionedItem>("v-inc", ct);
        Assert.Equal(1, loaded2!.VersionNumber);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task SaveAsync_StaleVersion_ThrowsConditionalCheckFailed(StoreType st)
    {
        var context = Context(st);
        var ct = TestContext.Current.CancellationToken;

        var item = new VersionedItem { Id = "v-stale", Data = "v0" };
        await context.SaveAsync(item, ct);

        var copy1 = await context.LoadAsync<VersionedItem>("v-stale", ct);
        var copy2 = await context.LoadAsync<VersionedItem>("v-stale", ct);

        copy1!.Data = "from-copy1";
        await context.SaveAsync(copy1, ct);

        copy2!.Data = "from-copy2";
        _ = await Assert.ThrowsAsync<ConditionalCheckFailedException>(() => context.SaveAsync(copy2, ct));
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task LoadAsync_VersionedItem_ReturnsCurrentVersion(StoreType st)
    {
        var context = Context(st);
        var ct = TestContext.Current.CancellationToken;

        var item = new VersionedItem { Id = "v-load", Data = "initial" };
        await context.SaveAsync(item, ct);

        for (var i = 0; i < 3; i++)
        {
            var loaded = await context.LoadAsync<VersionedItem>("v-load", ct);
            loaded!.Data = $"update-{i}";
            await context.SaveAsync(loaded, ct);
        }

        var final = await context.LoadAsync<VersionedItem>("v-load", ct);
        Assert.Equal(3, final!.VersionNumber);
        Assert.Equal("update-2", final.Data);
    }
}
