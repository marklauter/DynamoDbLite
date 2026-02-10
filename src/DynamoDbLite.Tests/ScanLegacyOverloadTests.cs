using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using System.Globalization;

namespace DynamoDbLite.Tests;

public sealed class ScanLegacyOverloadTests
    : IAsyncLifetime
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

        // Seed test data
        var items = new (string Pk, string Sk, string Name, int Age)[]
        {
            ("USER#1", "PROFILE", "Alice", 30),
            ("USER#2", "PROFILE", "Bob", 25),
            ("USER#3", "PROFILE", "Carol", 35),
        };

        foreach (var (pk, sk, name, age) in items)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = pk },
                    ["SK"] = new() { S = sk },
                    ["name"] = new() { S = name },
                    ["age"] = new() { N = age.ToString(CultureInfo.InvariantCulture) },
                }
            }, TestContext.Current.CancellationToken);
        }
    }

    public ValueTask DisposeAsync()
    {
        client.Dispose();
        return ValueTask.CompletedTask;
    }

    // ── attributesToGet overload ─────────────────────────────────────

    [Fact]
    public async Task ScanAsync_AttributesToGet_ProjectsAttributes()
    {
        var response = await client.ScanAsync(
            "TestTable",
            ["name"],
            TestContext.Current.CancellationToken);

        Assert.Equal(3, response.Count);
        Assert.All(response.Items, item =>
        {
            _ = Assert.Single(item);
            Assert.Contains("name", item.Keys);
        });
    }

    // ── scanFilter overload ─────────────────────────────────────────

    [Fact]
    public async Task ScanAsync_ScanFilter_EQ_FiltersResults()
    {
        var response = await client.ScanAsync(
            "TestTable",
            new Dictionary<string, Condition>
            {
                ["name"] = new()
                {
                    ComparisonOperator = ComparisonOperator.EQ,
                    AttributeValueList = [new AttributeValue { S = "Alice" }]
                }
            },
            TestContext.Current.CancellationToken);

        Assert.Equal(1, response.Count);
        Assert.Equal("Alice", response.Items[0]["name"].S);
    }

    [Fact]
    public async Task ScanAsync_ScanFilter_GT_FiltersResults()
    {
        var response = await client.ScanAsync(
            "TestTable",
            new Dictionary<string, Condition>
            {
                ["age"] = new()
                {
                    ComparisonOperator = ComparisonOperator.GT,
                    AttributeValueList = [new AttributeValue { N = "28" }]
                }
            },
            TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Count);
    }

    [Fact]
    public async Task ScanAsync_ScanFilter_BEGINS_WITH_FiltersResults()
    {
        var response = await client.ScanAsync(
            "TestTable",
            new Dictionary<string, Condition>
            {
                ["name"] = new()
                {
                    ComparisonOperator = ComparisonOperator.BEGINS_WITH,
                    AttributeValueList = [new AttributeValue { S = "A" }]
                }
            },
            TestContext.Current.CancellationToken);

        Assert.Equal(1, response.Count);
        Assert.Equal("Alice", response.Items[0]["name"].S);
    }

    [Fact]
    public async Task ScanAsync_ScanFilter_BETWEEN_FiltersResults()
    {
        var response = await client.ScanAsync(
            "TestTable",
            new Dictionary<string, Condition>
            {
                ["age"] = new()
                {
                    ComparisonOperator = ComparisonOperator.BETWEEN,
                    AttributeValueList =
                    [
                        new AttributeValue { N = "25" },
                        new AttributeValue { N = "30" }
                    ]
                }
            },
            TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Count);
    }

    [Fact]
    public async Task ScanAsync_ScanFilter_NOT_NULL_FiltersResults()
    {
        var response = await client.ScanAsync(
            "TestTable",
            new Dictionary<string, Condition>
            {
                ["name"] = new()
                {
                    ComparisonOperator = ComparisonOperator.NOT_NULL,
                }
            },
            TestContext.Current.CancellationToken);

        Assert.Equal(3, response.Count);
    }

    // ── attributesToGet + scanFilter combined overload ───────────────

    [Fact]
    public async Task ScanAsync_AttributesToGetAndScanFilter_BothApplied()
    {
        var response = await client.ScanAsync(
            "TestTable",
            ["name"],
            new Dictionary<string, Condition>
            {
                ["age"] = new()
                {
                    ComparisonOperator = ComparisonOperator.GE,
                    AttributeValueList = [new AttributeValue { N = "30" }]
                }
            },
            TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Count);
        Assert.All(response.Items, item =>
        {
            _ = Assert.Single(item);
            Assert.Contains("name", item.Keys);
        });
    }
}
