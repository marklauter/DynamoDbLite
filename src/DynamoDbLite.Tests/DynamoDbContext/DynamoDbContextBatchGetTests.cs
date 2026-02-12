using DynamoDbLite.Tests.Fixtures;
using DynamoDbLite.Tests.Models;

namespace DynamoDbLite.Tests.DynamoDbContext;

public class DynamoDbContextBatchGetTests
    : DynamoDbContextFixture
{
    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task BatchGet_MultipleItems_ReturnsAll(StoreType st)
    {
        var context = Context(st);
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

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task BatchGet_NonExistentKeys_Skipped(StoreType st)
    {
        var context = Context(st);
        var ct = TestContext.Current.CancellationToken;

        await context.SaveAsync(new SimpleItem { Id = "bg-exist", Name = "Exists" }, ct);

        var batch = context.CreateBatchGet<SimpleItem>();
        batch.AddKey("bg-exist");
        batch.AddKey("bg-ghost");
        await batch.ExecuteAsync(ct);

        _ = Assert.Single(batch.Results);
        Assert.Equal("bg-exist", batch.Results[0].Id);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task BatchGet_MultiTable_ReturnsFromBoth(StoreType st)
    {
        var context = Context(st);
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
}
