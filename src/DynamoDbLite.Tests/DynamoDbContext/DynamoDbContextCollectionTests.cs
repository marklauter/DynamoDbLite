using DynamoDbLite.Tests.Fixtures;
using DynamoDbLite.Tests.Models;

namespace DynamoDbLite.Tests.DynamoDbContext;

public abstract class DynamoDbContextCollectionTests
    : DynamoDbContextFixture
{
    [Fact]
    public async Task SaveAndLoad_ListOfStrings_RoundTrips()
    {
        var item = new CollectionItem { Id = "list-1", Tags = ["alpha", "beta", "gamma"] };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CollectionItem>("list-1", TestContext.Current.CancellationToken);
        Assert.Equal(["alpha", "beta", "gamma"], loaded!.Tags);
    }

    [Fact]
    public async Task SaveAndLoad_DictionaryOfStringInt_RoundTrips()
    {
        var item = new CollectionItem
        {
            Id = "dict-1",
            Scores = new Dictionary<string, int> { ["math"] = 95, ["science"] = 88 },
        };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CollectionItem>("dict-1", TestContext.Current.CancellationToken);
        Assert.Equal(95, loaded!.Scores["math"]);
        Assert.Equal(88, loaded.Scores["science"]);
    }

    [Fact]
    public async Task SaveAndLoad_HashSetOfStrings_RoundTrips()
    {
        var item = new CollectionItem { Id = "ss-1", StringSet = ["a", "b", "c"] };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CollectionItem>("ss-1", TestContext.Current.CancellationToken);
        Assert.Equal(3, loaded!.StringSet.Count);
        Assert.Contains("a", loaded.StringSet);
        Assert.Contains("b", loaded.StringSet);
        Assert.Contains("c", loaded.StringSet);
    }

    [Fact]
    public async Task SaveAndLoad_HashSetOfInts_RoundTrips()
    {
        var item = new CollectionItem { Id = "ns-1", NumberSet = [1, 2, 3] };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CollectionItem>("ns-1", TestContext.Current.CancellationToken);
        Assert.Equal(3, loaded!.NumberSet.Count);
        Assert.Contains(1, loaded.NumberSet);
        Assert.Contains(2, loaded.NumberSet);
        Assert.Contains(3, loaded.NumberSet);
    }

    [Fact]
    public async Task SaveAndLoad_EmptyList_RoundTrips()
    {
        var item = new CollectionItem { Id = "elist-1", Tags = [] };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CollectionItem>("elist-1", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded!.Tags);
        Assert.Empty(loaded.Tags);
    }

    [Fact]
    public async Task SaveAndLoad_EmptyDictionary_RoundTrips()
    {
        var item = new CollectionItem { Id = "edict-1", Scores = [] };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CollectionItem>("edict-1", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded!.Scores);
        Assert.Empty(loaded.Scores);
    }
}
