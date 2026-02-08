using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite.Tests;

public sealed class ScanTests : IAsyncLifetime
{
    private readonly DynamoDbClient client = new(new DynamoDbLiteOptions(
        $"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));

    public async ValueTask InitializeAsync()
    {
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "TestTable",
            KeySchema =
            [
                new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S }
            ]
        }, TestContext.Current.CancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        client.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task SeedItemsAsync(int count)
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

    [Fact]
    public async Task ScanAsync_EmptyTable_ReturnsEmptyResult()
    {
        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = "TestTable"
        }, TestContext.Current.CancellationToken);

        Assert.Equal(0, response.Count);
        Assert.Empty(response.Items);
    }

    // ── All items returned ──────────────────────────────────────────

    [Fact]
    public async Task ScanAsync_AllItems_ReturnsAll()
    {
        await SeedItemsAsync(5);

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = "TestTable"
        }, TestContext.Current.CancellationToken);

        Assert.Equal(5, response.Count);
        Assert.Equal(5, response.ScannedCount);
    }

    // ── Limit + pagination ──────────────────────────────────────────

    [Fact]
    public async Task ScanAsync_Limit_ReturnsLimitedResults()
    {
        await SeedItemsAsync(5);

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = "TestTable",
            Limit = 2,
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Count);
        Assert.NotNull(response.LastEvaluatedKey);
    }

    [Fact]
    public async Task ScanAsync_PaginationLoop_ReturnsAllItems()
    {
        await SeedItemsAsync(5);

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

    [Fact]
    public async Task ScanAsync_FilterExpression_FiltersResults()
    {
        await SeedItemsAsync(6);

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

    [Fact]
    public async Task ScanAsync_ProjectionExpression_ReturnsOnlyRequestedAttributes()
    {
        await SeedItemsAsync(3);

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

    [Fact]
    public async Task ScanAsync_SelectCount_ReturnsCountOnly()
    {
        await SeedItemsAsync(5);

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = "TestTable",
            Select = Select.COUNT,
        }, TestContext.Current.CancellationToken);

        Assert.Equal(5, response.Count);
        Assert.Equal(5, response.ScannedCount);
    }

    // ── Non-existent table ──────────────────────────────────────────

    [Fact]
    public async Task ScanAsync_NonExistentTable_ThrowsResourceNotFoundException()
    {
        _ = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            client.ScanAsync(new ScanRequest
            {
                TableName = "NonExistent"
            }, TestContext.Current.CancellationToken));
    }
}
