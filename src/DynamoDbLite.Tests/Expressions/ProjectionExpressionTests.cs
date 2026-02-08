using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Expressions;

namespace DynamoDbLite.Tests.Expressions;

public sealed class ProjectionExpressionTests
{
    // ── Parsing ─────────────────────────────────────────────────────────

    [Fact]
    public void Parse_SingleAttribute_ReturnsSinglePath()
    {
        var paths = ProjectionExpressionParser.Parse("name");

        _ = Assert.Single(paths);
        _ = Assert.Single(paths[0].Elements);
        Assert.Equal("name", ((AttributeNameElement)paths[0].Elements[0]).Name);
    }

    [Fact]
    public void Parse_MultipleAttributes_ReturnsMultiplePaths()
    {
        var paths = ProjectionExpressionParser.Parse("name, age, email");

        Assert.Equal(3, paths.Count);
    }

    [Fact]
    public void Parse_NestedPath_ReturnsMultipleElements()
    {
        var paths = ProjectionExpressionParser.Parse("address.city");

        _ = Assert.Single(paths);
        Assert.Equal(2, paths[0].Elements.Count);
        Assert.Equal("address", ((AttributeNameElement)paths[0].Elements[0]).Name);
        Assert.Equal("city", ((AttributeNameElement)paths[0].Elements[1]).Name);
    }

    [Fact]
    public void Parse_ListIndex_ReturnsIndexElement()
    {
        var paths = ProjectionExpressionParser.Parse("items[0]");

        _ = Assert.Single(paths);
        Assert.Equal(2, paths[0].Elements.Count);
        Assert.Equal("items", ((AttributeNameElement)paths[0].Elements[0]).Name);
        Assert.Equal(0, ((ListIndexElement)paths[0].Elements[1]).Index);
    }

    [Fact]
    public void Parse_ExpressionAttributeNames_ResolvedCorrectly()
    {
        var paths = ProjectionExpressionParser.Parse("#n, #a",
            new Dictionary<string, string>
            {
                ["#n"] = "name",
                ["#a"] = "age"
            });

        Assert.Equal(2, paths.Count);
        Assert.Equal("name", ((AttributeNameElement)paths[0].Elements[0]).Name);
        Assert.Equal("age", ((AttributeNameElement)paths[1].Elements[0]).Name);
    }

    // ── Evaluation ──────────────────────────────────────────────────────

    [Fact]
    public void Apply_SingleAttribute_ReturnsSubset()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["name"] = new() { S = "Alice" },
            ["age"] = new() { N = "30" }
        };

        var paths = ProjectionExpressionParser.Parse("name");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        _ = Assert.Single(result);
        Assert.Equal("Alice", result["name"].S);
    }

    [Fact]
    public void Apply_MultipleAttributes_ReturnsSubset()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["name"] = new() { S = "Alice" },
            ["age"] = new() { N = "30" },
            ["email"] = new() { S = "alice@example.com" }
        };

        var paths = ProjectionExpressionParser.Parse("name, age");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        Assert.Equal(2, result.Count);
        Assert.Equal("Alice", result["name"].S);
        Assert.Equal("30", result["age"].N);
    }

    [Fact]
    public void Apply_NestedPath_ReturnsNestedValue()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["address"] = new()
            {
                M = new Dictionary<string, AttributeValue>
                {
                    ["city"] = new() { S = "Seattle" },
                    ["zip"] = new() { S = "98101" }
                }
            }
        };

        var paths = ProjectionExpressionParser.Parse("address.city");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        Assert.True(result.ContainsKey("address"));
        Assert.Equal("Seattle", result["address"].M["city"].S);
    }

    [Fact]
    public void Apply_MissingAttribute_ExcludedFromResult()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["name"] = new() { S = "Alice" }
        };

        var paths = ProjectionExpressionParser.Parse("name, email");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        _ = Assert.Single(result);
        Assert.Equal("Alice", result["name"].S);
    }
}
