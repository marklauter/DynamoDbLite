using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Expressions;

namespace DynamoDbLite.Tests.Expressions;

public sealed class ConditionExpressionTests
{
    private static readonly Dictionary<string, AttributeValue> TestItem = new()
    {
        ["PK"] = new() { S = "USER#1" },
        ["name"] = new() { S = "Alice" },
        ["age"] = new() { N = "30" },
        ["active"] = new() { BOOL = true },
        ["tags"] = new() { SS = ["admin", "user"] },
        ["scores"] = new() { NS = ["10", "20", "30"] },
        ["bin"] = new() { B = new MemoryStream([0x01, 0x02, 0x03]) },
        ["blobs"] = new() { BS = [new MemoryStream([0xFF]), new MemoryStream([0xEE])] },
        ["entries"] = new() { L = [new() { S = "x" }, new() { N = "1" }] },
        ["meta"] = new() { M = new Dictionary<string, AttributeValue> { ["k"] = new() { S = "v" } } },
        ["null_attr"] = new() { NULL = true },
    };

    // ── Comparison ─────────────────────────────────────────────────────

    [Fact]
    public void Comparison_Equal_String_True()
    {
        var ast = ConditionExpressionParser.Parse("#n = :val");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue> { [":val"] = new() { S = "Alice" } });

        Assert.True(result);
    }

    [Fact]
    public void Comparison_Equal_String_False()
    {
        var ast = ConditionExpressionParser.Parse("#n = :val");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue> { [":val"] = new() { S = "Bob" } });

        Assert.False(result);
    }

    [Fact]
    public void Comparison_NotEqual_True()
    {
        var ast = ConditionExpressionParser.Parse("age <> :val");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":val"] = new() { N = "25" } });

        Assert.True(result);
    }

    [Fact]
    public void Comparison_GreaterThan_Number()
    {
        var ast = ConditionExpressionParser.Parse("age > :val");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":val"] = new() { N = "25" } });

        Assert.True(result);
    }

    [Fact]
    public void Comparison_LessThanOrEqual_Number()
    {
        var ast = ConditionExpressionParser.Parse("age <= :val");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":val"] = new() { N = "30" } });

        Assert.True(result);
    }

    // ── BETWEEN ───────────────────────────────────────────────────────

    [Fact]
    public void Between_InRange_True()
    {
        var ast = ConditionExpressionParser.Parse("age BETWEEN :low AND :high");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue>
            {
                [":low"] = new() { N = "20" },
                [":high"] = new() { N = "40" }
            });

        Assert.True(result);
    }

    [Fact]
    public void Between_OutOfRange_False()
    {
        var ast = ConditionExpressionParser.Parse("age BETWEEN :low AND :high");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue>
            {
                [":low"] = new() { N = "31" },
                [":high"] = new() { N = "40" }
            });

        Assert.False(result);
    }

    // ── IN ─────────────────────────────────────────────────────────────

    [Fact]
    public void In_ValuePresent_True()
    {
        var ast = ConditionExpressionParser.Parse("#n IN (:a, :b, :c)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue>
            {
                [":a"] = new() { S = "Alice" },
                [":b"] = new() { S = "Bob" },
                [":c"] = new() { S = "Charlie" }
            });

        Assert.True(result);
    }

    [Fact]
    public void In_ValueAbsent_False()
    {
        var ast = ConditionExpressionParser.Parse("#n IN (:a, :b)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue>
            {
                [":a"] = new() { S = "Bob" },
                [":b"] = new() { S = "Charlie" }
            });

        Assert.False(result);
    }

    // ── Logical operators ──────────────────────────────────────────────

    [Fact]
    public void And_BothTrue_ReturnsTrue()
    {
        var ast = ConditionExpressionParser.Parse("#n = :name AND age = :age");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue>
            {
                [":name"] = new() { S = "Alice" },
                [":age"] = new() { N = "30" }
            });

        Assert.True(result);
    }

    [Fact]
    public void And_OneFalse_ReturnsFalse()
    {
        var ast = ConditionExpressionParser.Parse("#n = :name AND age = :age");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue>
            {
                [":name"] = new() { S = "Alice" },
                [":age"] = new() { N = "99" }
            });

        Assert.False(result);
    }

    [Fact]
    public void Or_OneTrue_ReturnsTrue()
    {
        var ast = ConditionExpressionParser.Parse("#n = :name OR age = :age");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue>
            {
                [":name"] = new() { S = "Bob" },
                [":age"] = new() { N = "30" }
            });

        Assert.True(result);
    }

    [Fact]
    public void Not_InvertsResult()
    {
        var ast = ConditionExpressionParser.Parse("NOT #n = :name");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue> { [":name"] = new() { S = "Bob" } });

        Assert.True(result);
    }

    // ── Functions ──────────────────────────────────────────────────────

    [Fact]
    public void AttributeExists_ExistingAttribute_True()
    {
        var ast = ConditionExpressionParser.Parse("attribute_exists(#n)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem,
            new Dictionary<string, string> { ["#n"] = "name" }, null);

        Assert.True(result);
    }

    [Fact]
    public void AttributeExists_MissingAttribute_False()
    {
        var ast = ConditionExpressionParser.Parse("attribute_exists(email)");
        var result = ConditionExpressionEvaluator.Evaluate(ast, TestItem, null, null);

        Assert.False(result);
    }

    [Fact]
    public void AttributeNotExists_MissingAttribute_True()
    {
        var ast = ConditionExpressionParser.Parse("attribute_not_exists(email)");
        var result = ConditionExpressionEvaluator.Evaluate(ast, TestItem, null, null);

        Assert.True(result);
    }

    [Fact]
    public void AttributeNotExists_NullItem_True()
    {
        var ast = ConditionExpressionParser.Parse("attribute_not_exists(PK)");
        var result = ConditionExpressionEvaluator.Evaluate(ast, null, null, null);

        Assert.True(result);
    }

    [Fact]
    public void BeginsWith_MatchingPrefix_True()
    {
        var ast = ConditionExpressionParser.Parse("begins_with(PK, :prefix)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":prefix"] = new() { S = "USER#" } });

        Assert.True(result);
    }

    [Fact]
    public void BeginsWith_NonMatchingPrefix_False()
    {
        var ast = ConditionExpressionParser.Parse("begins_with(PK, :prefix)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":prefix"] = new() { S = "ORDER#" } });

        Assert.False(result);
    }

    [Fact]
    public void Contains_StringContains_True()
    {
        var ast = ConditionExpressionParser.Parse("contains(#n, :substr)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue> { [":substr"] = new() { S = "lic" } });

        Assert.True(result);
    }

    [Fact]
    public void Contains_StringSetContains_True()
    {
        var ast = ConditionExpressionParser.Parse("contains(tags, :val)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":val"] = new() { S = "admin" } });

        Assert.True(result);
    }

    [Fact]
    public void AttributeType_CorrectType_True()
    {
        var ast = ConditionExpressionParser.Parse("attribute_type(#n, :type)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue> { [":type"] = new() { S = "S" } });

        Assert.True(result);
    }

    [Fact]
    public void AttributeType_WrongType_False()
    {
        var ast = ConditionExpressionParser.Parse("attribute_type(#n, :type)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue> { [":type"] = new() { S = "N" } });

        Assert.False(result);
    }

    [Fact]
    public void AttributeType_NonEmptyStringSet_True()
    {
        var ast = ConditionExpressionParser.Parse("attribute_type(tags, :type)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":type"] = new() { S = "SS" } });

        Assert.True(result);
    }

    [Fact]
    public void AttributeType_EmptyStringSet_True()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["tags"] = new() { SS = [] },
        };

        var ast = ConditionExpressionParser.Parse("attribute_type(tags, :type)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":type"] = new() { S = "SS" } });

        Assert.True(result);
    }

    [Fact]
    public void AttributeType_EmptyNumberSet_True()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["nums"] = new() { NS = [] },
        };

        var ast = ConditionExpressionParser.Parse("attribute_type(nums, :type)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":type"] = new() { S = "NS" } });

        Assert.True(result);
    }

    // ── Size function ──────────────────────────────────────────────────

    [Fact]
    public void Size_StringAttribute_ReturnsLength()
    {
        var ast = ConditionExpressionParser.Parse("size(#n) = :len");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue> { [":len"] = new() { N = "5" } });

        Assert.True(result);
    }

    // ── Empty container path resolution ────────────────────────────────

    [Fact]
    public void ResolvePath_ThroughEmptyMap_ReturnsNull()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["a"] = new() { M = [] },
        };

        var ast = ConditionExpressionParser.Parse("attribute_exists(a.b)");
        var result = ConditionExpressionEvaluator.Evaluate(ast, item, null, null);

        Assert.False(result);
    }

    [Fact]
    public void ResolvePath_ThroughEmptyList_ReturnsNull()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["a"] = new() { L = [] },
        };

        var ast = ConditionExpressionParser.Parse("attribute_exists(a[0])");
        var result = ConditionExpressionEvaluator.Evaluate(ast, item, null, null);

        Assert.False(result);
    }

    // ── Parenthesized expressions ──────────────────────────────────────

    [Fact]
    public void Parenthesized_Expression_RespectsPrecedence()
    {
        var ast = ConditionExpressionParser.Parse("(#n = :name OR age = :wrong) AND attribute_exists(PK)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue>
            {
                [":name"] = new() { S = "Alice" },
                [":wrong"] = new() { N = "99" }
            });

        Assert.True(result);
    }

    // ── Size across all types ──────────────────────────────────────────

    [Theory]
    [InlineData("age", 2)]
    [InlineData("bin", 3)]
    [InlineData("tags", 2)]
    [InlineData("scores", 3)]
    [InlineData("blobs", 2)]
    [InlineData("entries", 2)]
    [InlineData("meta", 1)]
    public void Size_AllTypes_ReturnsExpectedCount(string attr, int expected)
    {
        var ast = ConditionExpressionParser.Parse($"size({attr}) = :len");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":len"] = new() { N = expected.ToString(System.Globalization.CultureInfo.InvariantCulture) } });

        Assert.True(result);
    }

    [Fact]
    public void Size_MissingAttribute_ReturnsZero()
    {
        var ast = ConditionExpressionParser.Parse("size(nope) = :zero");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":zero"] = new() { N = "0" } });

        Assert.True(result);
    }

    // ── attribute_type across all types ────────────────────────────────

    [Theory]
    [InlineData("bin", "B")]
    [InlineData("scores", "NS")]
    [InlineData("blobs", "BS")]
    [InlineData("entries", "L")]
    [InlineData("meta", "M")]
    [InlineData("active", "BOOL")]
    [InlineData("null_attr", "NULL")]
    public void AttributeType_AllTypes_ReturnsTypeName(string attr, string typeName)
    {
        var ast = ConditionExpressionParser.Parse($"attribute_type({attr}, :type)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":type"] = new() { S = typeName } });

        Assert.True(result);
    }

    // ── contains() across collection types ─────────────────────────────

    [Fact]
    public void Contains_NumberSet_True()
    {
        var ast = ConditionExpressionParser.Parse("contains(scores, :v)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":v"] = new() { N = "20" } });

        Assert.True(result);
    }

    [Fact]
    public void Contains_NumberSet_False()
    {
        var ast = ConditionExpressionParser.Parse("contains(scores, :v)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":v"] = new() { N = "999" } });

        Assert.False(result);
    }

    [Fact]
    public void Contains_BinarySet_True()
    {
        var ast = ConditionExpressionParser.Parse("contains(blobs, :v)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":v"] = new() { B = new MemoryStream([0xFF]) } });

        Assert.True(result);
    }

    [Fact]
    public void Contains_BinarySet_False()
    {
        var ast = ConditionExpressionParser.Parse("contains(blobs, :v)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":v"] = new() { B = new MemoryStream([0xAA]) } });

        Assert.False(result);
    }

    [Fact]
    public void Contains_List_TrueWhenElementMatches()
    {
        var ast = ConditionExpressionParser.Parse("contains(entries, :v)");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":v"] = new() { S = "x" } });

        Assert.True(result);
    }

    // ── Equality across scalar types ───────────────────────────────────

    [Fact]
    public void Equality_Binary_True()
    {
        var ast = ConditionExpressionParser.Parse("bin = :v");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":v"] = new() { B = new MemoryStream([0x01, 0x02, 0x03]) } });

        Assert.True(result);
    }

    [Fact]
    public void Equality_Binary_False()
    {
        var ast = ConditionExpressionParser.Parse("bin = :v");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":v"] = new() { B = new MemoryStream([0x01, 0x02]) } });

        Assert.False(result);
    }

    [Fact]
    public void Equality_Bool_True()
    {
        var ast = ConditionExpressionParser.Parse("active = :v");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":v"] = new() { BOOL = true } });

        Assert.True(result);
    }

    [Fact]
    public void Equality_Null_True()
    {
        var ast = ConditionExpressionParser.Parse("null_attr = :v");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":v"] = new() { NULL = true } });

        Assert.True(result);
    }

    [Fact]
    public void Equality_ListElement_ResolvesByIndex()
    {
        var ast = ConditionExpressionParser.Parse("entries[0] = :v");
        var result = ConditionExpressionEvaluator.Evaluate(
            ast, TestItem, null,
            new Dictionary<string, AttributeValue> { [":v"] = new() { S = "x" } });

        Assert.True(result);
    }

    // ── Reserved-word rejection ────────────────────────────────────────

    [Fact]
    public void Reserved_TopLevelIdentifier_Throws()
    {
        var ex = Assert.Throws<ArgumentException>(() =>
            ConditionExpressionParser.Parse("name = :v"));

        Assert.Contains("ConditionExpression", ex.Message);
        Assert.Contains("reserved keyword", ex.Message);
        Assert.Contains("name", ex.Message);
    }

    [Fact]
    public void Reserved_InFunctionArgument_Throws() =>
        Assert.Throws<ArgumentException>(() =>
            ConditionExpressionParser.Parse("attribute_exists(name)"));

    [Fact]
    public void Reserved_EscapedViaExpressionAttributeName_Allowed()
    {
        var ast = ConditionExpressionParser.Parse("#n = :v");
        Assert.NotNull(ast);
    }
}
