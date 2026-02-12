using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;
using System.Globalization;

namespace DynamoDbLite.Tests;

public sealed class ScanLegacyOverloadTests
    : DynamoDbClientFixture
{
    protected override async ValueTask SetupAsync(CancellationToken ct)
    {
        await CreateTestTableAsync(Client(StoreType.MemoryBased), ct);
        await CreateTestTableAsync(Client(StoreType.FileBased), ct);
        await SeedDataAsync(Client(StoreType.MemoryBased), ct);
        await SeedDataAsync(Client(StoreType.FileBased), ct);
    }

    private static async Task SeedDataAsync(DynamoDbClient client, CancellationToken ct)
    {
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
            }, ct);
        }
    }

    // -- attributesToGet overload ------------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ScanAsync_AttributesToGet_ProjectsAttributes(StoreType st)
    {
        var client = Client(st);

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

    // -- scanFilter overload -----------------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ScanAsync_ScanFilter_EQ_FiltersResults(StoreType st)
    {
        var client = Client(st);

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

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ScanAsync_ScanFilter_GT_FiltersResults(StoreType st)
    {
        var client = Client(st);

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

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ScanAsync_ScanFilter_BEGINS_WITH_FiltersResults(StoreType st)
    {
        var client = Client(st);

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

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ScanAsync_ScanFilter_BETWEEN_FiltersResults(StoreType st)
    {
        var client = Client(st);

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

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ScanAsync_ScanFilter_NOT_NULL_FiltersResults(StoreType st)
    {
        var client = Client(st);

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

    // -- attributesToGet + scanFilter combined overload ---------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ScanAsync_AttributesToGetAndScanFilter_BothApplied(StoreType st)
    {
        var client = Client(st);

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
