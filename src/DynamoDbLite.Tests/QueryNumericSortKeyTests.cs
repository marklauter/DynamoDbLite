using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite.Tests;

public sealed class QueryNumericSortKeyTests
    : IAsyncLifetime
{
    private readonly DynamoDbClient client = new(new DynamoDbLiteOptions(
        $"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));

    public async ValueTask InitializeAsync()
    {
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "NumericTable",
            KeySchema =
            [
                new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.N }
            ]
        }, TestContext.Current.CancellationToken);

        // Seed items with numeric SK values that would sort differently as strings vs numbers
        // String order: "1", "10", "2", "20"
        // Numeric order: 1, 2, 10, 20
        var skValues = new[] { "1", "2", "10", "20" };
        foreach (var sk in skValues)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = "NumericTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = "ITEM#1" },
                    ["SK"] = new() { N = sk },
                    ["value"] = new() { S = $"val_{sk}" }
                }
            }, TestContext.Current.CancellationToken);
        }
    }

    public ValueTask DisposeAsync()
    {
        client.Dispose();
        return ValueTask.CompletedTask;
    }

    // ── Numeric ordering ────────────────────────────────────────────

    [Fact]
    public async Task QueryAsync_NumericSk_SortsNumerically()
    {
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "NumericTable",
            KeyConditionExpression = "PK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "ITEM#1" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(4, response.Count);
        Assert.Equal("1", response.Items[0]["SK"].N);
        Assert.Equal("2", response.Items[1]["SK"].N);
        Assert.Equal("10", response.Items[2]["SK"].N);
        Assert.Equal("20", response.Items[3]["SK"].N);
    }

    // ── Numeric comparison ──────────────────────────────────────────

    [Fact]
    public async Task QueryAsync_NumericSk_LessThan_ComparesNumerically()
    {
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "NumericTable",
            KeyConditionExpression = "PK = :pk AND SK < :sk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "ITEM#1" },
                [":sk"] = new() { N = "10" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Count);
        Assert.Equal("1", response.Items[0]["SK"].N);
        Assert.Equal("2", response.Items[1]["SK"].N);
    }

    // ── Numeric BETWEEN ─────────────────────────────────────────────

    [Fact]
    public async Task QueryAsync_NumericSk_Between_ComparesNumerically()
    {
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "NumericTable",
            KeyConditionExpression = "PK = :pk AND SK BETWEEN :low AND :high",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "ITEM#1" },
                [":low"] = new() { N = "2" },
                [":high"] = new() { N = "10" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Count);
        Assert.Equal("2", response.Items[0]["SK"].N);
        Assert.Equal("10", response.Items[1]["SK"].N);
    }

    // ── Descending ──────────────────────────────────────────────────

    [Fact]
    public async Task QueryAsync_NumericSk_Descending_SortsNumericallyReversed()
    {
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "NumericTable",
            KeyConditionExpression = "PK = :pk",
            ScanIndexForward = false,
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "ITEM#1" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(4, response.Count);
        Assert.Equal("20", response.Items[0]["SK"].N);
        Assert.Equal("10", response.Items[1]["SK"].N);
        Assert.Equal("2", response.Items[2]["SK"].N);
        Assert.Equal("1", response.Items[3]["SK"].N);
    }

    // ── Pagination with numeric SK ──────────────────────────────────

    [Fact]
    public async Task QueryAsync_NumericSk_Pagination_WorksCorrectly()
    {
        var allItems = new List<Dictionary<string, AttributeValue>>();
        Dictionary<string, AttributeValue>? lastKey = null;

        do
        {
            var response = await client.QueryAsync(new QueryRequest
            {
                TableName = "NumericTable",
                KeyConditionExpression = "PK = :pk",
                Limit = 2,
                ExclusiveStartKey = lastKey,
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pk"] = new() { S = "ITEM#1" }
                }
            }, TestContext.Current.CancellationToken);

            allItems.AddRange(response.Items);
            lastKey = response.LastEvaluatedKey;
        }
        while (lastKey is not null);

        Assert.Equal(4, allItems.Count);
        Assert.Equal("1", allItems[0]["SK"].N);
        Assert.Equal("2", allItems[1]["SK"].N);
        Assert.Equal("10", allItems[2]["SK"].N);
        Assert.Equal("20", allItems[3]["SK"].N);
    }

    [Fact]
    public async Task QueryAsync_NumericSk_LastEvaluatedKey_HasNumericSk()
    {
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "NumericTable",
            KeyConditionExpression = "PK = :pk",
            Limit = 2,
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "ITEM#1" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.NotNull(response.LastEvaluatedKey);
        Assert.Equal("ITEM#1", response.LastEvaluatedKey["PK"].S);
        Assert.Equal("2", response.LastEvaluatedKey["SK"].N);
    }
}
