using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;
using DynamoDbLite.Tests.Models;

namespace DynamoDbLite.Tests.DynamoDbContext;

public abstract class DynamoDbContextTests
    : DynamoDbContextFixture
{
    // ───────────────────── Basic CRUD ─────────────────────

    [Fact]
    public async Task SaveAsync_NewSimpleItem_CanBeLoaded()
    {
        var item = new SimpleItem { Id = "1", Name = "Alice", Age = 30, Score = 9.5, IsActive = true };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<SimpleItem>("1", TestContext.Current.CancellationToken);

        Assert.NotNull(loaded);
        Assert.Equal("Alice", loaded.Name);
        Assert.Equal(30, loaded.Age);
        Assert.Equal(9.5, loaded.Score);
        Assert.True(loaded.IsActive);
    }

    [Fact]
    public async Task SaveAsync_CompositeKey_CanBeLoadedByKeys()
    {
        var item = new CompositeKeyItem { PK = "user#1", SK = "profile", CreatedAt = new DateTime(2025, 1, 15, 10, 30, 0, DateTimeKind.Utc) };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CompositeKeyItem>("user#1", "profile", TestContext.Current.CancellationToken);

        Assert.NotNull(loaded);
        Assert.Equal("user#1", loaded.PK);
        Assert.Equal("profile", loaded.SK);
    }

    [Fact]
    public async Task SaveAsync_Overwrite_UpdatesExistingItem()
    {
        var item = new SimpleItem { Id = "overwrite-1", Name = "Original" };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        item.Name = "Updated";
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<SimpleItem>("overwrite-1", TestContext.Current.CancellationToken);
        Assert.Equal("Updated", loaded!.Name);
    }

    [Fact]
    public async Task LoadAsync_NonExistentItem_ReturnsNull()
    {
        var loaded = await context.LoadAsync<SimpleItem>("nonexistent", TestContext.Current.CancellationToken);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task LoadAsync_ByHashKeyOnly_ReturnsItem()
    {
        await context.SaveAsync(new SimpleItem { Id = "hash-only", Name = "Test" }, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<SimpleItem>("hash-only", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("Test", loaded.Name);
    }

    [Fact]
    public async Task LoadAsync_ByCompositeKey_ReturnsItem()
    {
        var item = new CompositeKeyItem { PK = "pk-1", SK = "sk-1", CustomNamedProp = "hello" };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CompositeKeyItem>("pk-1", "sk-1", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("hello", loaded.CustomNamedProp);
    }

    [Fact]
    public async Task SaveAsync_WithNullOptionalProperty_Succeeds()
    {
        var item = new CompositeKeyItem { PK = "null-opt", SK = "1", OptionalValue = null };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CompositeKeyItem>("null-opt", "1", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Null(loaded.OptionalValue);
    }

    [Fact]
    public async Task SaveAsync_IgnoredProperty_NotPersisted()
    {
        var item = new CompositeKeyItem { PK = "ign", SK = "1", Ignored = "secret" };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CompositeKeyItem>("ign", "1", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("should-not-persist", loaded.Ignored);
    }

    // ───────────────────── Delete ─────────────────────

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

    // ───────────────────── Type Mappings ─────────────────────

    [Fact]
    public async Task SaveAndLoad_IntProperty_RoundTrips()
    {
        await context.SaveAsync(new SimpleItem { Id = "int-1", Age = 42 }, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<SimpleItem>("int-1", TestContext.Current.CancellationToken);
        Assert.Equal(42, loaded!.Age);
    }

    [Fact]
    public async Task SaveAndLoad_DoubleProperty_RoundTrips()
    {
        await context.SaveAsync(new SimpleItem { Id = "dbl-1", Score = 3.14159 }, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<SimpleItem>("dbl-1", TestContext.Current.CancellationToken);
        Assert.Equal(3.14159, loaded!.Score, 5);
    }

    [Fact]
    public async Task SaveAndLoad_BoolProperty_RoundTrips()
    {
        await context.SaveAsync(new SimpleItem { Id = "bool-1", IsActive = true }, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<SimpleItem>("bool-1", TestContext.Current.CancellationToken);
        Assert.True(loaded!.IsActive);
    }

    [Fact]
    public async Task SaveAndLoad_DateTimeProperty_RoundTrips()
    {
        var dt = new DateTime(2025, 6, 15, 12, 30, 45, DateTimeKind.Utc);
        await context.SaveAsync(new CompositeKeyItem { PK = "dt-1", SK = "a", CreatedAt = dt }, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CompositeKeyItem>("dt-1", "a", TestContext.Current.CancellationToken);
        Assert.Equal(dt, loaded!.CreatedAt);
        Assert.Equal(DateTimeKind.Utc, loaded.CreatedAt.Kind);
    }

    [Fact]
    public async Task SaveAndLoad_EnumProperty_RoundTrips()
    {
        await context.SaveAsync(new EnumItem { Id = "enum-1", Status = ItemStatus.Published }, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<EnumItem>("enum-1", TestContext.Current.CancellationToken);
        Assert.Equal(ItemStatus.Published, loaded!.Status);
    }

    [Fact]
    public async Task SaveAndLoad_NullableProperty_PreservesNull()
    {
        await context.SaveAsync(new CompositeKeyItem { PK = "nullable-1", SK = "a", OptionalValue = null }, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<CompositeKeyItem>("nullable-1", "a", TestContext.Current.CancellationToken);
        Assert.Null(loaded!.OptionalValue);
    }

    [Fact]
    public async Task SaveAndLoad_StringProperty_RoundTrips()
    {
        await context.SaveAsync(new SimpleItem { Id = "str-1", Name = "Hello World" }, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<SimpleItem>("str-1", TestContext.Current.CancellationToken);
        Assert.Equal("Hello World", loaded!.Name);
    }

    // ───────────────────── Collection Types ─────────────────────

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

    // ───────────────────── Numeric Sort Key ─────────────────────

    [Fact]
    public async Task SaveAndLoad_NumericRangeKey_RoundTrips()
    {
        var item = new NumericKeyItem { Category = "electronics", OrderNumber = 42, Description = "Widget" };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<NumericKeyItem>("electronics", 42, TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("Widget", loaded.Description);
    }

    [Fact]
    public async Task QueryAsync_NumericRangeKey_OrderedCorrectly()
    {
        var ct = TestContext.Current.CancellationToken;
        await context.SaveAsync(new NumericKeyItem { Category = "num-order", OrderNumber = 3, Description = "Third" }, ct);
        await context.SaveAsync(new NumericKeyItem { Category = "num-order", OrderNumber = 1, Description = "First" }, ct);
        await context.SaveAsync(new NumericKeyItem { Category = "num-order", OrderNumber = 2, Description = "Second" }, ct);

        var query = context.QueryAsync<NumericKeyItem>("num-order");
        var results = await query.GetRemainingAsync(ct);

        Assert.Equal(3, results.Count);
        Assert.Equal(1, results[0].OrderNumber);
        Assert.Equal(2, results[1].OrderNumber);
        Assert.Equal(3, results[2].OrderNumber);
    }

    // ───────────────────── Query ─────────────────────

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

    // ───────────────────── Scan ─────────────────────

    [Fact]
    public async Task ScanAsync_NoFilter_ReturnsAllItems()
    {
        var ct = TestContext.Current.CancellationToken;
        await context.SaveAsync(new SimpleItem { Id = "scan-1", Name = "A" }, ct);
        await context.SaveAsync(new SimpleItem { Id = "scan-2", Name = "B" }, ct);

        var scan = context.ScanAsync<SimpleItem>([]);
        var results = await scan.GetRemainingAsync(ct);

        Assert.True(results.Count >= 2);
    }

    [Fact]
    public async Task ScanAsync_WithCondition_FiltersResults()
    {
        var ct = TestContext.Current.CancellationToken;
        await context.SaveAsync(new SimpleItem { Id = "scan-f1", Name = "Match", Age = 25 }, ct);
        await context.SaveAsync(new SimpleItem { Id = "scan-f2", Name = "NoMatch", Age = 50 }, ct);

        var conditions = new List<ScanCondition>
        {
            new("Age", ScanOperator.LessThan, 30),
        };
        var scan = context.ScanAsync<SimpleItem>(conditions);
        var results = await scan.GetRemainingAsync(ct);

        Assert.All(results, r => Assert.True(r.Age < 30));
    }

    [Fact]
    public async Task ScanAsync_AllPages_ReturnsAllItems()
    {
        var ct = TestContext.Current.CancellationToken;
        for (var i = 0; i < 5; i++)
            await context.SaveAsync(new SimpleItem { Id = $"scan-all-{i}", Name = $"Item{i}" }, ct);

        var scan = context.ScanAsync<SimpleItem>([]);
        var results = await scan.GetRemainingAsync(ct);

        Assert.True(results.Count >= 5);
    }

    [Fact]
    public async Task ScanAsync_OnGsi_ReturnsIndexItems()
    {
        var ct = TestContext.Current.CancellationToken;
        await context.SaveAsync(new GsiItem { PK = "scan-gsi-1", SK = "a", GsiPK = "scan-gsi-hash", GsiSK = "x", Data = "d1" }, ct);

        var scan = context.ScanAsync<GsiItem>([], new ScanConfig { IndexName = "GsiIndex" });
        var results = await scan.GetRemainingAsync(ct);

        Assert.True(results.Count >= 1);
    }

    // ───────────────────── BatchGet ─────────────────────

    [Fact]
    public async Task BatchGet_MultipleItems_ReturnsAll()
    {
        var ct = TestContext.Current.CancellationToken;
        await context.SaveAsync(new SimpleItem { Id = "bg-1", Name = "One" }, ct);
        await context.SaveAsync(new SimpleItem { Id = "bg-2", Name = "Two" }, ct);
        await context.SaveAsync(new SimpleItem { Id = "bg-3", Name = "Three" }, ct);

        var batch = context.CreateBatchGet<SimpleItem>();
        batch.AddKey("bg-1");
        batch.AddKey("bg-2");
        batch.AddKey("bg-3");
        await batch.ExecuteAsync(ct);

        Assert.Equal(3, batch.Results.Count);
    }

    [Fact]
    public async Task BatchGet_NonExistentKeys_Skipped()
    {
        var ct = TestContext.Current.CancellationToken;
        await context.SaveAsync(new SimpleItem { Id = "bg-exist", Name = "Exists" }, ct);

        var batch = context.CreateBatchGet<SimpleItem>();
        batch.AddKey("bg-exist");
        batch.AddKey("bg-ghost");
        await batch.ExecuteAsync(ct);

        _ = Assert.Single(batch.Results);
        Assert.Equal("bg-exist", batch.Results[0].Id);
    }

    [Fact]
    public async Task BatchGet_MultiTable_ReturnsFromBoth()
    {
        var ct = TestContext.Current.CancellationToken;
        await context.SaveAsync(new SimpleItem { Id = "bg-mt-1", Name = "Simple" }, ct);
        await context.SaveAsync(new EnumItem { Id = "bg-mt-2", Status = ItemStatus.Draft }, ct);

        var simpleBatch = context.CreateBatchGet<SimpleItem>();
        simpleBatch.AddKey("bg-mt-1");

        var enumBatch = context.CreateBatchGet<EnumItem>();
        enumBatch.AddKey("bg-mt-2");

        var multiBatch = context.CreateMultiTableBatchGet(simpleBatch, enumBatch);
        await multiBatch.ExecuteAsync(ct);

        _ = Assert.Single(simpleBatch.Results);
        _ = Assert.Single(enumBatch.Results);
    }

    // ───────────────────── BatchWrite ─────────────────────

    [Fact]
    public async Task BatchWrite_MultiplePuts_AllPersisted()
    {
        var ct = TestContext.Current.CancellationToken;
        var batch = context.CreateBatchWrite<SimpleItem>();
        batch.AddPutItem(new SimpleItem { Id = "bw-1", Name = "One" });
        batch.AddPutItem(new SimpleItem { Id = "bw-2", Name = "Two" });
        batch.AddPutItem(new SimpleItem { Id = "bw-3", Name = "Three" });
        await batch.ExecuteAsync(ct);

        Assert.NotNull(await context.LoadAsync<SimpleItem>("bw-1", ct));
        Assert.NotNull(await context.LoadAsync<SimpleItem>("bw-2", ct));
        Assert.NotNull(await context.LoadAsync<SimpleItem>("bw-3", ct));
    }

    [Fact]
    public async Task BatchWrite_MultipleDeletes_AllRemoved()
    {
        var ct = TestContext.Current.CancellationToken;
        await context.SaveAsync(new SimpleItem { Id = "bwd-1", Name = "One" }, ct);
        await context.SaveAsync(new SimpleItem { Id = "bwd-2", Name = "Two" }, ct);

        var batch = context.CreateBatchWrite<SimpleItem>();
        batch.AddDeleteKey("bwd-1");
        batch.AddDeleteKey("bwd-2");
        await batch.ExecuteAsync(ct);

        Assert.Null(await context.LoadAsync<SimpleItem>("bwd-1", ct));
        Assert.Null(await context.LoadAsync<SimpleItem>("bwd-2", ct));
    }

    [Fact]
    public async Task BatchWrite_MixedPutAndDelete_Succeeds()
    {
        var ct = TestContext.Current.CancellationToken;
        await context.SaveAsync(new SimpleItem { Id = "bwm-del", Name = "ToDelete" }, ct);

        var batch = context.CreateBatchWrite<SimpleItem>();
        batch.AddPutItem(new SimpleItem { Id = "bwm-put", Name = "NewItem" });
        batch.AddDeleteKey("bwm-del");
        await batch.ExecuteAsync(ct);

        Assert.NotNull(await context.LoadAsync<SimpleItem>("bwm-put", ct));
        Assert.Null(await context.LoadAsync<SimpleItem>("bwm-del", ct));
    }

    [Fact]
    public async Task BatchWrite_MultiTable_Succeeds()
    {
        var ct = TestContext.Current.CancellationToken;
        var simpleBatch = context.CreateBatchWrite<SimpleItem>();
        simpleBatch.AddPutItem(new SimpleItem { Id = "bwmt-1", Name = "Simple" });

        var enumBatch = context.CreateBatchWrite<EnumItem>();
        enumBatch.AddPutItem(new EnumItem { Id = "bwmt-2", Status = ItemStatus.Archived });

        var multiBatch = context.CreateMultiTableBatchWrite(simpleBatch, enumBatch);
        await multiBatch.ExecuteAsync(ct);

        Assert.NotNull(await context.LoadAsync<SimpleItem>("bwmt-1", ct));
        Assert.NotNull(await context.LoadAsync<EnumItem>("bwmt-2", ct));
    }

    // ───────────────────── Versioning ─────────────────────

    [Fact]
    public async Task SaveAsync_NewVersionedItem_SetsVersionToZero()
    {
        var item = new VersionedItem { Id = "v-new", Data = "initial" };
        await context.SaveAsync(item, TestContext.Current.CancellationToken);

        var loaded = await context.LoadAsync<VersionedItem>("v-new", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal(0, loaded.VersionNumber);
    }

    [Fact]
    public async Task SaveAsync_ExistingVersionedItem_IncrementsVersion()
    {
        var ct = TestContext.Current.CancellationToken;
        var item = new VersionedItem { Id = "v-inc", Data = "v0" };
        await context.SaveAsync(item, ct);

        var loaded = await context.LoadAsync<VersionedItem>("v-inc", ct);
        loaded!.Data = "v1";
        await context.SaveAsync(loaded, ct);

        var loaded2 = await context.LoadAsync<VersionedItem>("v-inc", ct);
        Assert.Equal(1, loaded2!.VersionNumber);
    }

    [Fact]
    public async Task SaveAsync_StaleVersion_ThrowsConditionalCheckFailed()
    {
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

    [Fact]
    public async Task LoadAsync_VersionedItem_ReturnsCurrentVersion()
    {
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

    // ───────────────────── Attribute Mappings ─────────────────────

    [Fact]
    public async Task SaveAndLoad_PropertyWithCustomName_UsesAttributeName()
    {
        var ct = TestContext.Current.CancellationToken;
        var item = new CompositeKeyItem { PK = "cname-1", SK = "a", CustomNamedProp = "custom-value" };
        await context.SaveAsync(item, ct);

        // Verify via low-level API that the attribute is stored as "custom_name"
        var lowLevel = await client.GetItemAsync(new GetItemRequest
        {
            TableName = "CompositeItems",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "cname-1" },
                ["SK"] = new() { S = "a" },
            },
        }, ct);

        Assert.True(lowLevel.Item.ContainsKey("custom_name"));
        Assert.Equal("custom-value", lowLevel.Item["custom_name"].S);

        // Also verify round-trip through context
        var loaded = await context.LoadAsync<CompositeKeyItem>("cname-1", "a", ct);
        Assert.Equal("custom-value", loaded!.CustomNamedProp);
    }

    [Fact]
    public async Task SaveAndLoad_IgnoredProperty_NotStoredOrRetrieved()
    {
        var ct = TestContext.Current.CancellationToken;
        var item = new CompositeKeyItem { PK = "ignore-attr", SK = "a", Ignored = "should-vanish" };
        await context.SaveAsync(item, ct);

        var lowLevel = await client.GetItemAsync(new GetItemRequest
        {
            TableName = "CompositeItems",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "ignore-attr" },
                ["SK"] = new() { S = "a" },
            },
        }, ct);

        Assert.False(lowLevel.Item.ContainsKey("Ignored"));
    }

    [Fact]
    public async Task SaveAndLoad_GsiKeyProperties_RoundTrip()
    {
        var ct = TestContext.Current.CancellationToken;
        var item = new GsiItem { PK = "gsi-rt-1", SK = "a", GsiPK = "gsi-hash-rt", GsiSK = "gsi-sort-rt", Data = "test" };
        await context.SaveAsync(item, ct);

        var loaded = await context.LoadAsync<GsiItem>("gsi-rt-1", "a", ct);
        Assert.Equal("gsi-hash-rt", loaded!.GsiPK);
        Assert.Equal("gsi-sort-rt", loaded.GsiSK);
        Assert.Equal("test", loaded.Data);
    }

    // ───────────────────── Metadata Fetching ─────────────────────

    [Fact]
    public async Task DynamoDBContext_WithDefaultConfig_WorksWithDisabledMetadata()
    {
        // SDK v4 requires AmazonDynamoDBClient (not IAmazonDynamoDB) for LoadTable
        // when DisableFetchingTableMetadata=false, so we verify the builder path works
        var ct = TestContext.Current.CancellationToken;
        using var ctx = new DynamoDBContextBuilder()
            .ConfigureContext(cfg => cfg.DisableFetchingTableMetadata = true)
            .WithDynamoDBClient(() => client)
            .Build();

        await ctx.SaveAsync(new SimpleItem { Id = "meta-1", Name = "MetaTest" }, ct);
        var loaded = await ctx.LoadAsync<SimpleItem>("meta-1", ct);

        Assert.NotNull(loaded);
        Assert.Equal("MetaTest", loaded.Name);
    }

    // ───────────────────── Edge Cases ─────────────────────

    [Fact]
    public async Task SaveAsync_EmptyStringProperty_HandledCorrectly()
    {
        var ct = TestContext.Current.CancellationToken;
        await context.SaveAsync(new SimpleItem { Id = "empty-str", Name = "" }, ct);

        var loaded = await context.LoadAsync<SimpleItem>("empty-str", ct);
        Assert.NotNull(loaded);
    }

    [Fact]
    public async Task SaveAsync_VeryLargeItem_Succeeds()
    {
        var ct = TestContext.Current.CancellationToken;
        var largeString = new string('x', 100_000);
        await context.SaveAsync(new SimpleItem { Id = "large-1", Name = largeString }, ct);

        var loaded = await context.LoadAsync<SimpleItem>("large-1", ct);
        Assert.Equal(100_000, loaded!.Name.Length);
    }

    [Fact]
    public async Task LoadAsync_AfterDelete_ReturnsNull()
    {
        var ct = TestContext.Current.CancellationToken;
        await context.SaveAsync(new SimpleItem { Id = "del-reload", Name = "Exists" }, ct);
        await context.DeleteAsync<SimpleItem>("del-reload", ct);

        var loaded = await context.LoadAsync<SimpleItem>("del-reload", ct);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task QueryAsync_GetRemainingAsync_ReturnsAll()
    {
        var ct = TestContext.Current.CancellationToken;
        for (var i = 0; i < 10; i++)
            await context.SaveAsync(new CompositeKeyItem { PK = "q-remaining", SK = $"item-{i:D2}" }, ct);

        var query = context.QueryAsync<CompositeKeyItem>("q-remaining");
        var results = await query.GetRemainingAsync(ct);

        Assert.Equal(10, results.Count);
    }
}
