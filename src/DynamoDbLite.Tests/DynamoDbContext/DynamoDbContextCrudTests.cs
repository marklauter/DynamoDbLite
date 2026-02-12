using DynamoDbLite.Tests.Fixtures;
using DynamoDbLite.Tests.Models;

namespace DynamoDbLite.Tests.DynamoDbContext;

public class DynamoDbContextCrudTests
    : DynamoDbContextFixture
{
    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task SaveAsync_NewSimpleItem_CanBeLoaded(StoreType st)
    {
        var context = Context(st);

        var item = new SimpleItem { Id = "1", Name = "Alice", Age = 30, Score = 9.5, IsActive = true };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<SimpleItem>("1", TestContext.Current.CancellationToken);

        Assert.NotNull(loaded);
        Assert.Equal("Alice", loaded.Name);
        Assert.Equal(30, loaded.Age);
        Assert.Equal(9.5, loaded.Score);
        Assert.True(loaded.IsActive);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task SaveAsync_CompositeKey_CanBeLoadedByKeys(StoreType st)
    {
        var context = Context(st);

        var item = new CompositeKeyItem { PK = "user#1", SK = "profile", CreatedAt = new DateTime(2025, 1, 15, 10, 30, 0, DateTimeKind.Utc) };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CompositeKeyItem>("user#1", "profile", TestContext.Current.CancellationToken);

        Assert.NotNull(loaded);
        Assert.Equal("user#1", loaded.PK);
        Assert.Equal("profile", loaded.SK);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task SaveAsync_Overwrite_UpdatesExistingItem(StoreType st)
    {
        var context = Context(st);

        var item = new SimpleItem { Id = "overwrite-1", Name = "Original" };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        item.Name = "Updated";
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<SimpleItem>("overwrite-1", TestContext.Current.CancellationToken);
        Assert.Equal("Updated", loaded!.Name);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task LoadAsync_NonExistentItem_ReturnsNull(StoreType st)
    {
        var context = Context(st);

        var loaded = await context.LoadAsync<SimpleItem>("nonexistent", TestContext.Current.CancellationToken);
        Assert.Null(loaded);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task LoadAsync_ByHashKeyOnly_ReturnsItem(StoreType st)
    {
        var context = Context(st);

        await context.SaveAsync(new SimpleItem { Id = "hash-only", Name = "Test" }, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<SimpleItem>("hash-only", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("Test", loaded.Name);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task LoadAsync_ByCompositeKey_ReturnsItem(StoreType st)
    {
        var context = Context(st);

        var item = new CompositeKeyItem { PK = "pk-1", SK = "sk-1", CustomNamedProp = "hello" };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CompositeKeyItem>("pk-1", "sk-1", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("hello", loaded.CustomNamedProp);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task SaveAsync_WithNullOptionalProperty_Succeeds(StoreType st)
    {
        var context = Context(st);

        var item = new CompositeKeyItem { PK = "null-opt", SK = "1", OptionalValue = null };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CompositeKeyItem>("null-opt", "1", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Null(loaded.OptionalValue);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task SaveAsync_IgnoredProperty_NotPersisted(StoreType st)
    {
        var context = Context(st);

        var item = new CompositeKeyItem { PK = "ign", SK = "1", Ignored = "secret" };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CompositeKeyItem>("ign", "1", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("should-not-persist", loaded.Ignored);
    }
}
