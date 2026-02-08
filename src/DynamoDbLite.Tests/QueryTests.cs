using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite.Tests;

public sealed class QueryTests : IAsyncLifetime
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
            }, TestContext.Current.CancellationToken);
        }
    }

    public ValueTask DisposeAsync()
    {
        client.Dispose();
        return ValueTask.CompletedTask;
    }

    // ── PK-only query ───────────────────────────────────────────────

    [Fact]
    public async Task QueryAsync_PkOnly_ReturnsAllItemsForPartition()
    {
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

    [Fact]
    public async Task QueryAsync_SkEquality_ReturnsSingleItem()
    {
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

    [Fact]
    public async Task QueryAsync_SkLessThan_ReturnsCorrectItems()
    {
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

    [Fact]
    public async Task QueryAsync_SkGreaterThanOrEqual_ReturnsCorrectItems()
    {
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

    [Fact]
    public async Task QueryAsync_SkBetween_ReturnsItemsInRange()
    {
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

    [Fact]
    public async Task QueryAsync_BeginsWith_ReturnsMatchingItems()
    {
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

    // ── ScanIndexForward = false ────────────────────────────────────

    [Fact]
    public async Task QueryAsync_ScanIndexForwardFalse_ReturnsDescending()
    {
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

    [Fact]
    public async Task QueryAsync_Limit_ReturnsLimitedResultsWithLastEvaluatedKey()
    {
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

    [Fact]
    public async Task QueryAsync_PaginationLoop_ReturnsAllItems()
    {
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

    [Fact]
    public async Task QueryAsync_FilterExpression_FiltersResults()
    {
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

    [Fact]
    public async Task QueryAsync_ProjectionExpression_ReturnsOnlyRequestedAttributes()
    {
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

    [Fact]
    public async Task QueryAsync_SelectCount_ReturnsCountOnly()
    {
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

    [Fact]
    public async Task QueryAsync_NonExistentTable_ThrowsResourceNotFoundException()
    {
        _ = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            client.QueryAsync(new QueryRequest
            {
                TableName = "NonExistent",
                KeyConditionExpression = "PK = :pk",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pk"] = new() { S = "X" }
                }
            }, TestContext.Current.CancellationToken));
    }

    // ── Empty results ───────────────────────────────────────────────

    [Fact]
    public async Task QueryAsync_NoMatchingItems_ReturnsEmptyResult()
    {
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
