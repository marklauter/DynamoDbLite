using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

public sealed class ScanTests
    : DynamoDbClientFixture
{
    protected override async ValueTask SetupAsync(CancellationToken ct)
    {
        await CreateTestTableAsync(Client(StoreType.DdbLite), ct);
        await CreateTestTableAsync(Client(StoreType.DdbLiteFile), ct);
    }

    private async Task SeedItemsAsync(DynamoDbClient client, int count)
    {
        for (var i = 0; i < count; i++)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = TestTableName,
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
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task ScanAsync_EmptyTable_ReturnsEmptyResult(StoreType st)
    {
        var client = Client(st);
        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = TestTableName
        }, TestContext.Current.CancellationToken);

        Assert.Equal(0, response.Count);
        Assert.Empty(response.Items);
    }

    // ── All items returned ──────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task ScanAsync_AllItems_ReturnsAll(StoreType st)
    {
        var client = Client(st);
        await SeedItemsAsync(client, 5);

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = TestTableName
        }, TestContext.Current.CancellationToken);

        Assert.Equal(5, response.Count);
        Assert.Equal(5, response.ScannedCount);
    }

    // ── Limit + pagination ──────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task ScanAsync_Limit_ReturnsLimitedResults(StoreType st)
    {
        var client = Client(st);
        await SeedItemsAsync(client, 5);

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = TestTableName,
            Limit = 2,
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Count);
        Assert.NotNull(response.LastEvaluatedKey);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
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
                TableName = TestTableName,
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
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task ScanAsync_FilterExpression_FiltersResults(StoreType st)
    {
        var client = Client(st);
        await SeedItemsAsync(client, 6);

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = TestTableName,
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
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task ScanAsync_ProjectionExpression_ReturnsOnlyRequestedAttributes(StoreType st)
    {
        var client = Client(st);
        await SeedItemsAsync(client, 3);

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = TestTableName,
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
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task ScanAsync_SelectCount_ReturnsCountOnly(StoreType st)
    {
        var client = Client(st);
        await SeedItemsAsync(client, 5);

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = TestTableName,
            Select = Select.COUNT,
        }, TestContext.Current.CancellationToken);

        Assert.Equal(5, response.Count);
        Assert.Equal(5, response.ScannedCount);
    }

    // ── Non-existent table ──────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task ScanAsync_NonExistentTable_ThrowsResourceNotFoundException(StoreType st)
        => _ = await Assert.ThrowsAsync<ResourceNotFoundException>(()
            => Client(st).ScanAsync(new ScanRequest
            {
                TableName = "NonExistent"
            }, TestContext.Current.CancellationToken));

    // ── Parallel scan (TotalSegments + Segment) ─────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task ScanAsync_ParallelSegments_PartitionItemsDeterministically(StoreType st)
    {
        const int totalSegments = 4;
        const int itemCount = 40;

        var client = Client(st);
        await SeedItemsAsync(client, itemCount);

        async Task<HashSet<string>> ScanSegment(int segment)
        {
            var response = await client.ScanAsync(new ScanRequest
            {
                TableName = TestTableName,
                TotalSegments = totalSegments,
                Segment = segment
            }, TestContext.Current.CancellationToken);
            return [.. response.Items.Select(i => i["PK"].S)];
        }

        var seen = new HashSet<string>();
        for (var segment = 0; segment < totalSegments; segment++)
        {
            var pks = await ScanSegment(segment);
            foreach (var pk in pks)
                Assert.True(seen.Add(pk), $"PK {pk} appeared in multiple segments");
        }

        Assert.Equal(itemCount, seen.Count);

        // Stable: same segment returns same PKs across runs.
        var first = await ScanSegment(0);
        var second = await ScanSegment(0);
        Assert.Equal(first, second);
    }

    // ── Segmentation validation ─────────────────────────────────────

    [Fact]
    public async Task ScanAsync_SegmentWithoutTotalSegments_Throws() =>
        _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            Client(StoreType.DdbLite).ScanAsync(new ScanRequest
            {
                TableName = TestTableName,
                Segment = 0
            }, TestContext.Current.CancellationToken));

    [Fact]
    public async Task ScanAsync_TotalSegmentsWithoutSegment_Throws() =>
        _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            Client(StoreType.DdbLite).ScanAsync(new ScanRequest
            {
                TableName = TestTableName,
                TotalSegments = 2
            }, TestContext.Current.CancellationToken));

    [Fact]
    public async Task ScanAsync_TotalSegmentsBelowMin_Throws() =>
        _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            Client(StoreType.DdbLite).ScanAsync(new ScanRequest
            {
                TableName = TestTableName,
#pragma warning disable DynamoDB1003 // exercising server-side validation on out-of-range TotalSegments
                TotalSegments = 0,
#pragma warning restore DynamoDB1003
                Segment = 0
            }, TestContext.Current.CancellationToken));

    [Fact]
    public async Task ScanAsync_TotalSegmentsAboveMax_Throws() =>
        _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            Client(StoreType.DdbLite).ScanAsync(new ScanRequest
            {
                TableName = TestTableName,
#pragma warning disable DynamoDB1004 // exercising server-side validation on out-of-range TotalSegments
                TotalSegments = 1_000_001,
#pragma warning restore DynamoDB1004
                Segment = 0
            }, TestContext.Current.CancellationToken));

    [Fact]
    public async Task ScanAsync_SegmentOutOfRange_Throws() =>
        _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            Client(StoreType.DdbLite).ScanAsync(new ScanRequest
            {
                TableName = TestTableName,
                TotalSegments = 4,
                Segment = 4
            }, TestContext.Current.CancellationToken));
}
