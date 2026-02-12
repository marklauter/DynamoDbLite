using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

public sealed class ScanTests
    : DynamoDbClientFixture
{
    protected override async ValueTask SetupAsync(CancellationToken ct)
    {
        await CreateTestTableAsync(Client(StoreType.MemoryBased), ct);
        await CreateTestTableAsync(Client(StoreType.FileBased), ct);
    }

    private static async Task SeedItemsAsync(DynamoDbClient client, int count)
    {
        for (var i = 0; i < count; i++)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = $"PK#{i}" },
                    ["SK"] = new() { S = $"SK#{i}" },
                    ["name"] = new() { S = $"Item{i}" },
                    ["active"] = new() { BOOL = i % 2 == 0 },
                }
            }, TestContext.Current.CancellationToken);
        }
    }

    // ── Empty table ─────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ScanAsync_EmptyTable_ReturnsEmptyResult(StoreType st)
    {
        var client = Client(st);
        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = "TestTable"
        }, TestContext.Current.CancellationToken);

        Assert.Equal(0, response.Count);
        Assert.Empty(response.Items);
    }

    // ── All items returned ──────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ScanAsync_AllItems_ReturnsAll(StoreType st)
    {
        var client = Client(st);
        await SeedItemsAsync(client, 5);

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = "TestTable"
        }, TestContext.Current.CancellationToken);

        Assert.Equal(5, response.Count);
        Assert.Equal(5, response.ScannedCount);
    }

    // ── Limit + pagination ──────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ScanAsync_Limit_ReturnsLimitedResults(StoreType st)
    {
        var client = Client(st);
        await SeedItemsAsync(client, 5);

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = "TestTable",
            Limit = 2,
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Count);
        Assert.NotNull(response.LastEvaluatedKey);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ScanAsync_PaginationLoop_ReturnsAllItems(StoreType st)
    {
        var client = Client(st);
        await SeedItemsAsync(client, 5);

        var allItems = new List<Dictionary<string, AttributeValue>>();
        Dictionary<string, AttributeValue>? lastKey = null;

        do
        {
            var response = await client.ScanAsync(new ScanRequest
            {
                TableName = "TestTable",
                Limit = 2,
                ExclusiveStartKey = lastKey,
            }, TestContext.Current.CancellationToken);

            allItems.AddRange(response.Items);
            lastKey = response.LastEvaluatedKey;
        }
        while (lastKey is not null);

        Assert.Equal(5, allItems.Count);
    }

    // ── FilterExpression ────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ScanAsync_FilterExpression_FiltersResults(StoreType st)
    {
        var client = Client(st);
        await SeedItemsAsync(client, 6);

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = "TestTable",
            FilterExpression = "active = :active",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":active"] = new() { BOOL = true }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(6, response.ScannedCount);
        Assert.Equal(3, response.Count);
    }

    // ── ProjectionExpression ────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ScanAsync_ProjectionExpression_ReturnsOnlyRequestedAttributes(StoreType st)
    {
        var client = Client(st);
        await SeedItemsAsync(client, 3);

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = "TestTable",
            ProjectionExpression = "#n",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#n"] = "name"
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(3, response.Count);
        Assert.All(response.Items, item =>
        {
            _ = Assert.Single(item);
            Assert.Contains("name", item.Keys);
        });
    }

    // ── Select.COUNT ────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ScanAsync_SelectCount_ReturnsCountOnly(StoreType st)
    {
        var client = Client(st);
        await SeedItemsAsync(client, 5);

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = "TestTable",
            Select = Select.COUNT,
        }, TestContext.Current.CancellationToken);

        Assert.Equal(5, response.Count);
        Assert.Equal(5, response.ScannedCount);
    }

    // ── Non-existent table ──────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ScanAsync_NonExistentTable_ThrowsResourceNotFoundException(StoreType st)
        => _ = await Assert.ThrowsAsync<ResourceNotFoundException>(()
            => Client(st).ScanAsync(new ScanRequest
            {
                TableName = "NonExistent"
            }, TestContext.Current.CancellationToken));
}
