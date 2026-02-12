using DynamoDbLite.Tests.Fixtures;
using DynamoDbLite.Tests.Models;

namespace DynamoDbLite.Tests.DynamoDbContext;

public class DynamoDbContextTypeMappingTests
    : DynamoDbContextFixture
{
    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task SaveAndLoad_IntProperty_RoundTrips(StoreType st)
    {
        var context = Context(st);

        await context.SaveAsync(new SimpleItem { Id = "int-1", Age = 42 }, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<SimpleItem>("int-1", TestContext.Current.CancellationToken);
        Assert.Equal(42, loaded!.Age);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task SaveAndLoad_DoubleProperty_RoundTrips(StoreType st)
    {
        var context = Context(st);

        await context.SaveAsync(new SimpleItem { Id = "dbl-1", Score = 3.14159 }, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<SimpleItem>("dbl-1", TestContext.Current.CancellationToken);
        Assert.Equal(3.14159, loaded!.Score, 5);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task SaveAndLoad_BoolProperty_RoundTrips(StoreType st)
    {
        var context = Context(st);

        await context.SaveAsync(new SimpleItem { Id = "bool-1", IsActive = true }, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<SimpleItem>("bool-1", TestContext.Current.CancellationToken);
        Assert.True(loaded!.IsActive);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task SaveAndLoad_DateTimeProperty_RoundTrips(StoreType st)
    {
        var context = Context(st);

        var dt = new DateTime(2025, 6, 15, 12, 30, 45, DateTimeKind.Utc);
        await context.SaveAsync(new CompositeKeyItem { PK = "dt-1", SK = "a", CreatedAt = dt }, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CompositeKeyItem>("dt-1", "a", TestContext.Current.CancellationToken);
        Assert.Equal(dt, loaded!.CreatedAt);
        Assert.Equal(DateTimeKind.Utc, loaded.CreatedAt.Kind);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task SaveAndLoad_EnumProperty_RoundTrips(StoreType st)
    {
        var context = Context(st);

        await context.SaveAsync(new EnumItem { Id = "enum-1", Status = ItemStatus.Published }, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<EnumItem>("enum-1", TestContext.Current.CancellationToken);
        Assert.Equal(ItemStatus.Published, loaded!.Status);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task SaveAndLoad_NullableProperty_PreservesNull(StoreType st)
    {
        var context = Context(st);

        await context.SaveAsync(new CompositeKeyItem { PK = "nullable-1", SK = "a", OptionalValue = null }, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CompositeKeyItem>("nullable-1", "a", TestContext.Current.CancellationToken);
        Assert.Null(loaded!.OptionalValue);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task SaveAndLoad_StringProperty_RoundTrips(StoreType st)
    {
        var context = Context(st);

        await context.SaveAsync(new SimpleItem { Id = "str-1", Name = "Hello World" }, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<SimpleItem>("str-1", TestContext.Current.CancellationToken);
        Assert.Equal("Hello World", loaded!.Name);
    }
}
