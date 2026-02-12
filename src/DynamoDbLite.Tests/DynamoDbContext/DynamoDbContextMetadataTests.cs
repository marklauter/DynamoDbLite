using Amazon.DynamoDBv2.DataModel;
using DynamoDbLite.Tests.Fixtures;
using DynamoDbLite.Tests.Models;

namespace DynamoDbLite.Tests.DynamoDbContext;

public sealed class DynamoDbContextMetadataTests
    : DynamoDbContextFixture
{
    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DynamoDBContext_WithDefaultConfig_WorksWithDisabledMetadata(StoreType st)
    {
        var client = Client(st);

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
}
