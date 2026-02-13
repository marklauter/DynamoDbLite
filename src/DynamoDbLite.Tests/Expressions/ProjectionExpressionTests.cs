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

    [Fact]
    public void Apply_ListIndexNestedPath_PreservesListStructure()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["items"] = new()
            {
                L =
                [
                    new() { M = new Dictionary<string, AttributeValue> { ["name"] = new() { S = "a" } } },
                    new() { M = new Dictionary<string, AttributeValue> { ["name"] = new() { S = "b" } } }
                ]
            }
        };

        var paths = ProjectionExpressionParser.Parse("items[1].name");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        // Result should have items as a list (not a map)
        Assert.Null(result["items"].M);
        Assert.NotNull(result["items"].L);
        Assert.Equal("b", result["items"].L[1].M["name"].S);
    }

    [Fact]
    public void Apply_ListIndex_FinalElement_CopiesValue()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["items"] = new()
            {
                L =
                [
                    new() { S = "alpha" },
                    new() { S = "beta" }
                ]
            }
        };

        var paths = ProjectionExpressionParser.Parse("items[0]");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        Assert.NotNull(result["items"].L);
        Assert.Equal("alpha", result["items"].L[0].S);
    }

    [Fact]
    public void Apply_NestedListIndex_OutOfBounds_OmitsAttribute()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["items"] = new()
            {
                L =
                [
                    new() { M = new Dictionary<string, AttributeValue> { ["name"] = new() { S = "a" } } },
                    new() { M = new Dictionary<string, AttributeValue> { ["name"] = new() { S = "b" } } }
                ]
            }
        };

        var paths = ProjectionExpressionParser.Parse("items[5].name");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        Assert.False(result.ContainsKey("items"));
    }

    [Fact]
    public void Apply_NestedMapThenList_PreservesBothTypes()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["data"] = new()
            {
                M = new Dictionary<string, AttributeValue>
                {
                    ["tags"] = new()
                    {
                        L =
                        [
                            new() { S = "red" },
                            new() { S = "blue" }
                        ]
                    }
                }
            }
        };

        var paths = ProjectionExpressionParser.Parse("data.tags[0]");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        Assert.NotNull(result["data"].M);
        Assert.Null(result["data"].L);
        Assert.NotNull(result["data"].M["tags"].L);
        Assert.Null(result["data"].M["tags"].M);
        Assert.Equal("red", result["data"].M["tags"].L[0].S);
    }

    [Fact]
    public void Apply_SharedRootMap_MergesSuccessAndOmitsFailure()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["address"] = new()
            {
                M = new Dictionary<string, AttributeValue>
                {
                    ["city"] = new() { S = "Seattle" },
                    ["state"] = new() { S = "WA" },
                    ["zip"] = new() { S = "98101" }
                }
            }
        };

        var paths = ProjectionExpressionParser.Parse("address.city, address.nonexistent");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        Assert.True(result.ContainsKey("address"));
        _ = Assert.Single(result["address"].M);
        Assert.Equal("Seattle", result["address"].M["city"].S);
    }

    [Fact]
    public void Apply_MultipleListPaths_PopulatesBothSlots()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["items"] = new()
            {
                L =
                [
                    new() { M = new Dictionary<string, AttributeValue> { ["name"] = new() { S = "a" } } },
                    new() { M = new Dictionary<string, AttributeValue> { ["name"] = new() { S = "b" } } }
                ]
            }
        };

        var paths = ProjectionExpressionParser.Parse("items[0].name, items[1].name");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        Assert.Equal(2, result["items"].L.Count);
        Assert.Equal("a", result["items"].L[0].M["name"].S);
        Assert.Equal("b", result["items"].L[1].M["name"].S);
    }

    [Fact]
    public void Apply_NonContiguousListPaths_NullPadsBetweenSlots()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["items"] = new()
            {
                L =
                [
                    new() { M = new Dictionary<string, AttributeValue> { ["name"] = new() { S = "a" } } },
                    new() { M = new Dictionary<string, AttributeValue> { ["name"] = new() { S = "b" } } },
                    new() { M = new Dictionary<string, AttributeValue> { ["name"] = new() { S = "c" } } }
                ]
            }
        };

        var paths = ProjectionExpressionParser.Parse("items[0].name, items[2].name");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        Assert.Equal(3, result["items"].L.Count);
        Assert.Equal("a", result["items"].L[0].M["name"].S);
        Assert.True(result["items"].L[1].NULL);
        Assert.Equal("c", result["items"].L[2].M["name"].S);
    }

    [Fact]
    public void Apply_MissingRootOnMultiElementPath_OmitsAttribute()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["name"] = new() { S = "Alice" }
        };

        var paths = ProjectionExpressionParser.Parse("missing.city");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        Assert.False(result.ContainsKey("missing"));
    }
}
