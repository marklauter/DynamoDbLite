using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Expressions;

namespace DynamoDbLite.Tests.Expressions;

public sealed class UpdateExpressionTests
{
    // ── SET ─────────────────────────────────────────────────────────────

    [Fact]
    public void Set_SimpleValue_SetsAttribute()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("SET #n = :val");
        var (result, modifiedKeys) = UpdateExpressionEvaluator.Apply(
            ast, item,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue> { [":val"] = new() { S = "Bob" } });

        Assert.Equal("Bob", result["name"].S);
        Assert.Contains("name", modifiedKeys);
    }

    [Fact]
    public void Set_MultipleAttributes_SetsAll()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("SET #n = :name, age = :age");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue>
            {
                [":name"] = new() { S = "Charlie" },
                [":age"] = new() { N = "25" }
            });

        Assert.Equal("Charlie", result["name"].S);
        Assert.Equal("25", result["age"].N);
    }

    [Fact]
    public void Set_ArithmeticAdd_IncrementsNumber()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("SET age = age + :inc");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":inc"] = new() { N = "5" } });

        Assert.Equal("35", result["age"].N);
    }

    [Fact]
    public void Set_ArithmeticSubtract_DecrementsNumber()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("SET age = age - :dec");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":dec"] = new() { N = "5" } });

        Assert.Equal("25", result["age"].N);
    }

    [Fact]
    public void Set_IfNotExists_UsesDefault_WhenNotPresent()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("SET score = if_not_exists(score, :default)");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":default"] = new() { N = "0" } });

        Assert.Equal("0", result["score"].N);
    }

    [Fact]
    public void Set_IfNotExists_UsesExisting_WhenPresent()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("SET age = if_not_exists(age, :default)");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":default"] = new() { N = "0" } });

        Assert.Equal("30", result["age"].N);
    }

    [Fact]
    public void Set_ListAppend_ConcatenatesLists()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["items"] = new() { L = [new() { S = "a" }] }
        };

        var ast = UpdateExpressionParser.Parse("SET items = list_append(items, :newItems)");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue>
            {
                [":newItems"] = new() { L = [new() { S = "b" }, new() { S = "c" }] }
            });

        Assert.Equal(3, result["items"].L.Count);
        Assert.Equal("a", result["items"].L[0].S);
        Assert.Equal("b", result["items"].L[1].S);
        Assert.Equal("c", result["items"].L[2].S);
    }

    // ── REMOVE ─────────────────────────────────────────────────────────

    [Fact]
    public void Remove_ExistingAttribute_RemovesIt()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("REMOVE #n");
        var (result, modifiedKeys) = UpdateExpressionEvaluator.Apply(
            ast, item,
            new Dictionary<string, string> { ["#n"] = "name" }, null);

        Assert.False(result.ContainsKey("name"));
        Assert.Contains("name", modifiedKeys);
    }

    [Fact]
    public void Remove_MultipleAttributes_RemovesAll()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("REMOVE #n, age");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item,
            new Dictionary<string, string> { ["#n"] = "name" }, null);

        Assert.False(result.ContainsKey("name"));
        Assert.False(result.ContainsKey("age"));
    }

    // ── ADD ─────────────────────────────────────────────────────────────

    [Fact]
    public void Add_Number_IncrementsExistingValue()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("ADD age :inc");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":inc"] = new() { N = "10" } });

        Assert.Equal("40", result["age"].N);
    }

    [Fact]
    public void Add_Number_CreatesIfNotExists()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("ADD score :val");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":val"] = new() { N = "100" } });

        Assert.Equal("100", result["score"].N);
    }

    [Fact]
    public void Add_StringSet_UnionsElements()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["tags"] = new() { SS = ["a", "b"] }
        };

        var ast = UpdateExpressionParser.Parse("ADD tags :newTags");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue>
            {
                [":newTags"] = new() { SS = ["b", "c"] }
            });

        Assert.Equal(3, result["tags"].SS.Count);
        Assert.Contains("a", result["tags"].SS);
        Assert.Contains("b", result["tags"].SS);
        Assert.Contains("c", result["tags"].SS);
    }

    // ── DELETE ──────────────────────────────────────────────────────────

    [Fact]
    public void Delete_StringSet_RemovesElements()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["tags"] = new() { SS = ["a", "b", "c"] }
        };

        var ast = UpdateExpressionParser.Parse("DELETE tags :removeTags");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue>
            {
                [":removeTags"] = new() { SS = ["b"] }
            });

        Assert.Equal(2, result["tags"].SS.Count);
        Assert.Contains("a", result["tags"].SS);
        Assert.Contains("c", result["tags"].SS);
    }

    // ── Combined clauses ───────────────────────────────────────────────

    [Fact]
    public void SetAndRemove_Combined_AppliesBoth()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("SET age = :age REMOVE #n");
        var (result, modifiedKeys) = UpdateExpressionEvaluator.Apply(
            ast, item,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue> { [":age"] = new() { N = "31" } });

        Assert.Equal("31", result["age"].N);
        Assert.False(result.ContainsKey("name"));
        Assert.Contains("age", modifiedKeys);
        Assert.Contains("name", modifiedKeys);
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private static Dictionary<string, AttributeValue> CreateTestItem() =>
        new()
        {
            ["PK"] = new() { S = "USER#1" },
            ["name"] = new() { S = "Alice" },
            ["age"] = new() { N = "30" },
        };
}
