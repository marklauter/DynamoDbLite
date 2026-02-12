using DynamoDbLite.Tests.Fixtures;
using DynamoDbLite.Tests.Models;

namespace DynamoDbLite.Tests.DynamoDbContext;

public abstract class DynamoDbContextNumericSortKeyTests
    : DynamoDbContextFixture
{
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
}
