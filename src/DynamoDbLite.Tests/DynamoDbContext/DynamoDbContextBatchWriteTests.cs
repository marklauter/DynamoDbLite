using DynamoDbLite.Tests.Fixtures;
using DynamoDbLite.Tests.Models;

namespace DynamoDbLite.Tests.DynamoDbContext;

public abstract class DynamoDbContextBatchWriteTests
    : DynamoDbContextFixture
{
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
}
