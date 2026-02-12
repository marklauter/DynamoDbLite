using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;
using DynamoDbLite.Tests.Models;

namespace DynamoDbLite.Tests.DynamoDbContext;

public abstract class DynamoDbContextAttributeMappingTests
    : DynamoDbContextFixture
{
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
}
