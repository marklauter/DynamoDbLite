using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

public sealed class QueryTests
    : DynamoDbClientFixture
{
    protected override async ValueTask SetupAsync(CancellationToken ct)
    {
        await CreateTestTableAsync(Client(StoreType.MemoryBased), ct);
        await CreateTestTableAsync(Client(StoreType.FileBased), ct);
        await SeedTestDataAsync(Client(StoreType.MemoryBased), ct);
        await SeedTestDataAsync(Client(StoreType.FileBased), ct);
    }

    private static async Task SeedTestDataAsync(DynamoDbClient client, CancellationToken ct)
    {
        // Seed test data: USER#1 has 5 items, USER#2 has 2 items
        var items = new (string Pk, string Sk, string Name)[]
        {
            ("USER#1", "A", "Alice"),
            ("USER#1", "B", "Bob"),
            ("USER#1", "C", "Carol"),
            ("USER#1", "D", "Dave"),
            ("USER#1", "E", "Eve"),
            ("USER#2", "A", "Frank"),
            ("USER#2", "B", "Grace"),
        };

        foreach (var (pk, sk, name) in items)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = pk },
                    ["SK"] = new() { S = sk },
                    ["name"] = new() { S = name },
                    ["active"] = new() { BOOL = name != "Carol" },
                }
            }, ct);
        }
    }

    // ── PK-only query ───────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_PkOnly_ReturnsAllItemsForPartition(StoreType st)
    {
        var client = Client(st);
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            KeyConditionExpression = "PK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "USER#1" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(5, response.Count);
        Assert.Equal(5, response.ScannedCount);
    }

    // ── SK equality ─────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_SkEquality_ReturnsSingleItem(StoreType st)
    {
        var client = Client(st);
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            KeyConditionExpression = "PK = :pk AND SK = :sk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "USER#1" },
                [":sk"] = new() { S = "B" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(1, response.Count);
        Assert.Equal("Bob", response.Items[0]["name"].S);
    }

    // ── SK comparison operators ─────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_SkLessThan_ReturnsCorrectItems(StoreType st)
    {
        var client = Client(st);
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            KeyConditionExpression = "PK = :pk AND SK < :sk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "USER#1" },
                [":sk"] = new() { S = "C" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Count);
        Assert.Equal("A", response.Items[0]["SK"].S);
        Assert.Equal("B", response.Items[1]["SK"].S);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_SkGreaterThanOrEqual_ReturnsCorrectItems(StoreType st)
    {
        var client = Client(st);
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            KeyConditionExpression = "PK = :pk AND SK >= :sk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "USER#1" },
                [":sk"] = new() { S = "D" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Count);
        Assert.Equal("D", response.Items[0]["SK"].S);
        Assert.Equal("E", response.Items[1]["SK"].S);
    }

    // ── SK BETWEEN ──────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_SkBetween_ReturnsItemsInRange(StoreType st)
    {
        var client = Client(st);
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            KeyConditionExpression = "PK = :pk AND SK BETWEEN :low AND :high",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "USER#1" },
                [":low"] = new() { S = "B" },
                [":high"] = new() { S = "D" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(3, response.Count);
        Assert.Equal("B", response.Items[0]["SK"].S);
        Assert.Equal("C", response.Items[1]["SK"].S);
        Assert.Equal("D", response.Items[2]["SK"].S);
    }

    // ── begins_with ─────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_BeginsWith_ReturnsMatchingItems(StoreType st)
    {
        var client = Client(st);
        // Add items with longer SKs for begins_with testing
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#3" },
                ["SK"] = new() { S = "ORDER#001" },
                ["name"] = new() { S = "Order1" }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#3" },
                ["SK"] = new() { S = "ORDER#002" },
                ["name"] = new() { S = "Order2" }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#3" },
                ["SK"] = new() { S = "PROFILE" },
                ["name"] = new() { S = "Profile" }
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            KeyConditionExpression = "PK = :pk AND begins_with(SK, :prefix)",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "USER#3" },
                [":prefix"] = new() { S = "ORDER#" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Count);
        Assert.All(response.Items, item => Assert.StartsWith("ORDER#", item["SK"].S));
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_BeginsWith_TrailingMaxChar_ReturnsMatchingItems(StoreType st)
    {
        var client = Client(st);
        var prefix = "ORDER\uffff";
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#9" },
                ["SK"] = new() { S = prefix + "A" },
                ["name"] = new() { S = "Hit" }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#9" },
                ["SK"] = new() { S = "ORDEZ" },
                ["name"] = new() { S = "Miss" }
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            KeyConditionExpression = "PK = :pk AND begins_with(SK, :prefix)",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "USER#9" },
                [":prefix"] = new() { S = prefix }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(1, response.Count);
        Assert.Equal("Hit", response.Items[0]["name"].S);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_BeginsWith_AllMaxChars_ReturnsMatchingItems(StoreType st)
    {
        var client = Client(st);
        var prefix = "\uffff\uffff";
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#10" },
                ["SK"] = new() { S = prefix + "X" },
                ["name"] = new() { S = "Hit" }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#10" },
                ["SK"] = new() { S = "other" },
                ["name"] = new() { S = "Miss" }
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            KeyConditionExpression = "PK = :pk AND begins_with(SK, :prefix)",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "USER#10" },
                [":prefix"] = new() { S = prefix }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(1, response.Count);
        Assert.Equal("Hit", response.Items[0]["name"].S);
    }

    // ── ScanIndexForward = false ────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_ScanIndexForwardFalse_ReturnsDescending(StoreType st)
    {
        var client = Client(st);
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            KeyConditionExpression = "PK = :pk",
            ScanIndexForward = false,
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "USER#1" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(5, response.Count);
        Assert.Equal("E", response.Items[0]["SK"].S);
        Assert.Equal("D", response.Items[1]["SK"].S);
        Assert.Equal("C", response.Items[2]["SK"].S);
        Assert.Equal("B", response.Items[3]["SK"].S);
        Assert.Equal("A", response.Items[4]["SK"].S);
    }

    // ── Limit + LastEvaluatedKey pagination ─────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_Limit_ReturnsLimitedResultsWithLastEvaluatedKey(StoreType st)
    {
        var client = Client(st);
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            KeyConditionExpression = "PK = :pk",
            Limit = 2,
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "USER#1" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Count);
        Assert.NotNull(response.LastEvaluatedKey);
        Assert.Equal("USER#1", response.LastEvaluatedKey["PK"].S);
        Assert.Equal("B", response.LastEvaluatedKey["SK"].S);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_PaginationLoop_ReturnsAllItems(StoreType st)
    {
        var client = Client(st);
        var allItems = new List<Dictionary<string, AttributeValue>>();
        Dictionary<string, AttributeValue>? lastKey = null;

        do
        {
            var response = await client.QueryAsync(new QueryRequest
            {
                TableName = "TestTable",
                KeyConditionExpression = "PK = :pk",
                Limit = 2,
                ExclusiveStartKey = lastKey,
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pk"] = new() { S = "USER#1" }
                }
            }, TestContext.Current.CancellationToken);

            allItems.AddRange(response.Items);
            lastKey = response.LastEvaluatedKey;
        }
        while (lastKey is not null);

        Assert.Equal(5, allItems.Count);
        Assert.Equal("A", allItems[0]["SK"].S);
        Assert.Equal("E", allItems[4]["SK"].S);
    }

    // ── FilterExpression ────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_FilterExpression_FiltersResults(StoreType st)
    {
        var client = Client(st);
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            KeyConditionExpression = "PK = :pk",
            FilterExpression = "active = :active",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "USER#1" },
                [":active"] = new() { BOOL = true }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(5, response.ScannedCount);
        Assert.Equal(4, response.Count);
        Assert.All(response.Items, item => Assert.NotEqual("Carol", item["name"].S));
    }

    // ── ProjectionExpression ────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_ProjectionExpression_ReturnsOnlyRequestedAttributes(StoreType st)
    {
        var client = Client(st);
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            KeyConditionExpression = "PK = :pk AND SK = :sk",
            ProjectionExpression = "#n",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#n"] = "name"
            },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "USER#1" },
                [":sk"] = new() { S = "A" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(1, response.Count);
        _ = Assert.Single(response.Items[0]);
        Assert.Equal("Alice", response.Items[0]["name"].S);
    }

    // ── Select.COUNT ────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_SelectCount_ReturnsCountOnly(StoreType st)
    {
        var client = Client(st);
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            KeyConditionExpression = "PK = :pk",
            Select = Select.COUNT,
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "USER#1" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(5, response.Count);
        Assert.Equal(5, response.ScannedCount);
    }

    // ── Non-existent table ──────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_NonExistentTable_ThrowsResourceNotFoundException(StoreType st)
        => _ = await Assert.ThrowsAsync<ResourceNotFoundException>(()
            => Client(st).QueryAsync(new QueryRequest
            {
                TableName = "NonExistent",
                KeyConditionExpression = "PK = :pk",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pk"] = new() { S = "X" }
                }
            }, TestContext.Current.CancellationToken));

    // ── Empty results ───────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_NoMatchingItems_ReturnsEmptyResult(StoreType st)
    {
        var client = Client(st);
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            KeyConditionExpression = "PK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "NONEXISTENT" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(0, response.Count);
        Assert.Empty(response.Items);
        Assert.Null(response.LastEvaluatedKey);
    }
}
