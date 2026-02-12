using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using DynamoDbLite.Tests.Fixtures;
using DynamoDbLite.Tests.Models;

namespace DynamoDbLite.Tests.DynamoDbContext;

public abstract class DynamoDbContextScanTests
    : DynamoDbContextFixture
{
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
}
