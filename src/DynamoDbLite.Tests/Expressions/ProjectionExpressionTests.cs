using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Expressions;

namespace DynamoDbLite.Tests.Expressions;

public sealed class ProjectionExpressionTests
{
    // ── Parsing ─────────────────────────────────────────────────────────

    [Fact]
    public void Parse_SingleAttribute_ReturnsSinglePath()
    {
        var paths = ProjectionExpressionParser.Parse("firstname");

        _ = Assert.Single(paths);
        _ = Assert.Single(paths[0].Elements);
        Assert.Equal("firstname", ((AttributeNameElement)paths[0].Elements[0]).Name);
    }

    [Fact]
    public void Parse_MultipleAttributes_ReturnsMultiplePaths()
    {
        var paths = ProjectionExpressionParser.Parse("firstname, age, email");

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
        var paths = ProjectionExpressionParser.Parse("entries[0]");

        _ = Assert.Single(paths);
        Assert.Equal(2, paths[0].Elements.Count);
        Assert.Equal("entries", ((AttributeNameElement)paths[0].Elements[0]).Name);
        Assert.Equal(0, ((ListIndexElement)paths[0].Elements[1]).Index);
    }

    [Fact]
    public void Parse_ExpressionAttributeNames_ResolvedCorrectly()
    {
        var paths = ProjectionExpressionParser.Parse("#n, #a",
            new Dictionary<string, string>
            {
                ["#n"] = "firstname",
                ["#a"] = "age"
            });

        Assert.Equal(2, paths.Count);
        Assert.Equal("firstname", ((AttributeNameElement)paths[0].Elements[0]).Name);
        Assert.Equal("age", ((AttributeNameElement)paths[1].Elements[0]).Name);
    }

    // ── Evaluation ──────────────────────────────────────────────────────

    [Fact]
    public void Apply_SingleAttribute_ReturnsSubset()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["firstname"] = new() { S = "Alice" },
            ["age"] = new() { N = "30" }
        };

        var paths = ProjectionExpressionParser.Parse("firstname");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        _ = Assert.Single(result);
        Assert.Equal("Alice", result["firstname"].S);
    }

    [Fact]
    public void Apply_MultipleAttributes_ReturnsSubset()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["firstname"] = new() { S = "Alice" },
            ["age"] = new() { N = "30" },
            ["email"] = new() { S = "alice@example.com" }
        };

        var paths = ProjectionExpressionParser.Parse("firstname, age");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        Assert.Equal(2, result.Count);
        Assert.Equal("Alice", result["firstname"].S);
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
            ["firstname"] = new() { S = "Alice" }
        };

        var paths = ProjectionExpressionParser.Parse("firstname, email");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        _ = Assert.Single(result);
        Assert.Equal("Alice", result["firstname"].S);
    }

    [Fact]
    public void Apply_ListIndexNestedPath_PreservesListStructure()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["entries"] = new()
            {
                L =
                [
                    new() { M = new Dictionary<string, AttributeValue> { ["firstname"] = new() { S = "a" } } },
                    new() { M = new Dictionary<string, AttributeValue> { ["firstname"] = new() { S = "b" } } }
                ]
            }
        };

        var paths = ProjectionExpressionParser.Parse("entries[1].firstname");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        // Result should have items as a list (not a map)
        Assert.Null(result["entries"].M);
        Assert.NotNull(result["entries"].L);
        Assert.Equal("b", result["entries"].L[1].M["firstname"].S);
    }

    [Fact]
    public void Apply_ListIndex_FinalElement_CopiesValue()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["entries"] = new()
            {
                L =
                [
                    new() { S = "alpha" },
                    new() { S = "beta" }
                ]
            }
        };

        var paths = ProjectionExpressionParser.Parse("entries[0]");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        Assert.NotNull(result["entries"].L);
        Assert.Equal("alpha", result["entries"].L[0].S);
    }

    [Fact]
    public void Apply_NestedListIndex_OutOfBounds_OmitsAttribute()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["entries"] = new()
            {
                L =
                [
                    new() { M = new Dictionary<string, AttributeValue> { ["firstname"] = new() { S = "a" } } },
                    new() { M = new Dictionary<string, AttributeValue> { ["firstname"] = new() { S = "b" } } }
                ]
            }
        };

        var paths = ProjectionExpressionParser.Parse("entries[5].firstname");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        Assert.False(result.ContainsKey("entries"));
    }

    [Fact]
    public void Apply_NestedMapThenList_PreservesBothTypes()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["payload"] = new()
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

        var paths = ProjectionExpressionParser.Parse("payload.tags[0]");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        Assert.NotNull(result["payload"].M);
        Assert.Null(result["payload"].L);
        Assert.NotNull(result["payload"].M["tags"].L);
        Assert.Null(result["payload"].M["tags"].M);
        Assert.Equal("red", result["payload"].M["tags"].L[0].S);
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
            ["entries"] = new()
            {
                L =
                [
                    new() { M = new Dictionary<string, AttributeValue> { ["firstname"] = new() { S = "a" } } },
                    new() { M = new Dictionary<string, AttributeValue> { ["firstname"] = new() { S = "b" } } }
                ]
            }
        };

        var paths = ProjectionExpressionParser.Parse("entries[0].firstname, entries[1].firstname");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        Assert.Equal(2, result["entries"].L.Count);
        Assert.Equal("a", result["entries"].L[0].M["firstname"].S);
        Assert.Equal("b", result["entries"].L[1].M["firstname"].S);
    }

    [Fact]
    public void Apply_NonContiguousListPaths_NullPadsBetweenSlots()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["entries"] = new()
            {
                L =
                [
                    new() { M = new Dictionary<string, AttributeValue> { ["firstname"] = new() { S = "a" } } },
                    new() { M = new Dictionary<string, AttributeValue> { ["firstname"] = new() { S = "b" } } },
                    new() { M = new Dictionary<string, AttributeValue> { ["firstname"] = new() { S = "c" } } }
                ]
            }
        };

        var paths = ProjectionExpressionParser.Parse("entries[0].firstname, entries[2].firstname");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        Assert.Equal(3, result["entries"].L.Count);
        Assert.Equal("a", result["entries"].L[0].M["firstname"].S);
        Assert.True(result["entries"].L[1].NULL);
        Assert.Equal("c", result["entries"].L[2].M["firstname"].S);
    }

    [Fact]
    public void Apply_MissingRootOnMultiElementPath_OmitsAttribute()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["firstname"] = new() { S = "Alice" }
        };

        var paths = ProjectionExpressionParser.Parse("absent.city");
        var result = ProjectionExpressionEvaluator.Apply(item, paths);

        Assert.False(result.ContainsKey("absent"));
    }

    // ── Reserved-word rejection ────────────────────────────────────────

    [Fact]
    public void Reserved_TopLevelIdentifier_Throws()
    {
        var ex = Assert.Throws<ArgumentException>(() =>
            ProjectionExpressionParser.Parse("name"));

        Assert.Contains("ProjectionExpression", ex.Message);
        Assert.Contains("reserved keyword", ex.Message);
    }

    [Fact]
    public void Reserved_NestedPathElement_Throws() =>
        Assert.Throws<ArgumentException>(() =>
            ProjectionExpressionParser.Parse("a.status"));

    [Fact]
    public void Reserved_EscapedViaExpressionAttributeName_Allowed()
    {
        var paths = ProjectionExpressionParser.Parse("#n",
            new Dictionary<string, string> { ["#n"] = "name" });

        Assert.Equal("name", ((AttributeNameElement)paths[0].Elements[0]).Name);
    }
}
