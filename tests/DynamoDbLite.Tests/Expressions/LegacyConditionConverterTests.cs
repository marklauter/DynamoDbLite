using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Expressions;

namespace DynamoDbLite.Tests.Expressions;

public sealed class LegacyConditionConverterTests
{
    // ── Single-value comparison operators ───────────────────────────

    [Theory]
    [InlineData("EQ", "=")]
    [InlineData("NE", "<>")]
    [InlineData("LT", "<")]
    [InlineData("LE", "<=")]
    [InlineData("GT", ">")]
    [InlineData("GE", ">=")]
    public void Convert_SingleValueOperator_ProducesMatchingExpression(string op, string sqlOp)
    {
        var conditions = new Dictionary<string, Condition>
        {
            ["age"] = new()
            {
                ComparisonOperator = op,
                AttributeValueList = [new AttributeValue { N = "30" }]
            }
        };

        var (expression, attrNames, attrValues) = LegacyConditionConverter.Convert(conditions);

        Assert.Equal($"#legacyN0 {sqlOp} :legacyV0", expression);
        Assert.Equal("age", attrNames["#legacyN0"]);
        Assert.Equal("30", attrValues[":legacyV0"].N);
    }

    // ── Function-style operators ────────────────────────────────────

    [Fact]
    public void Convert_BeginsWith_ProducesBeginsWithFunction()
    {
        var conditions = new Dictionary<string, Condition>
        {
            ["name"] = new()
            {
                ComparisonOperator = ComparisonOperator.BEGINS_WITH,
                AttributeValueList = [new AttributeValue { S = "A" }]
            }
        };

        var (expression, _, attrValues) = LegacyConditionConverter.Convert(conditions);

        Assert.Equal("begins_with(#legacyN0, :legacyV0)", expression);
        Assert.Equal("A", attrValues[":legacyV0"].S);
    }

    [Fact]
    public void Convert_Contains_ProducesContainsFunction()
    {
        var conditions = new Dictionary<string, Condition>
        {
            ["name"] = new()
            {
                ComparisonOperator = ComparisonOperator.CONTAINS,
                AttributeValueList = [new AttributeValue { S = "v" }]
            }
        };

        var (expression, _, _) = LegacyConditionConverter.Convert(conditions);

        Assert.Equal("contains(#legacyN0, :legacyV0)", expression);
    }

    [Fact]
    public void Convert_Between_ProducesBetweenWithTwoValueKeys()
    {
        var conditions = new Dictionary<string, Condition>
        {
            ["age"] = new()
            {
                ComparisonOperator = ComparisonOperator.BETWEEN,
                AttributeValueList =
                [
                    new AttributeValue { N = "25" },
                    new AttributeValue { N = "35" }
                ]
            }
        };

        var (expression, _, attrValues) = LegacyConditionConverter.Convert(conditions);

        Assert.Equal("#legacyN0 BETWEEN :legacyV0a AND :legacyV0b", expression);
        Assert.Equal("25", attrValues[":legacyV0a"].N);
        Assert.Equal("35", attrValues[":legacyV0b"].N);
    }

    // ── Existence checks ────────────────────────────────────────────

    [Fact]
    public void Convert_NotNull_ProducesAttributeExists()
    {
        var conditions = new Dictionary<string, Condition>
        {
            ["name"] = new() { ComparisonOperator = ComparisonOperator.NOT_NULL }
        };

        var (expression, attrNames, attrValues) = LegacyConditionConverter.Convert(conditions);

        Assert.Equal("attribute_exists(#legacyN0)", expression);
        Assert.Equal("name", attrNames["#legacyN0"]);
        Assert.Empty(attrValues);
    }

    [Fact]
    public void Convert_Null_ProducesAttributeNotExists()
    {
        var conditions = new Dictionary<string, Condition>
        {
            ["missing"] = new() { ComparisonOperator = ComparisonOperator.NULL }
        };

        var (expression, _, attrValues) = LegacyConditionConverter.Convert(conditions);

        Assert.Equal("attribute_not_exists(#legacyN0)", expression);
        Assert.Empty(attrValues);
    }

    // ── Multi-condition join ────────────────────────────────────────

    [Fact]
    public void Convert_MultipleConditions_JoinedWithAnd()
    {
        var conditions = new Dictionary<string, Condition>
        {
            ["a"] = new()
            {
                ComparisonOperator = ComparisonOperator.EQ,
                AttributeValueList = [new AttributeValue { S = "x" }]
            },
            ["b"] = new()
            {
                ComparisonOperator = ComparisonOperator.GT,
                AttributeValueList = [new AttributeValue { N = "1" }]
            }
        };

        var (expression, attrNames, attrValues) = LegacyConditionConverter.Convert(conditions);

        Assert.Equal("#legacyN0 = :legacyV0 AND #legacyN1 > :legacyV1", expression);
        Assert.Equal("a", attrNames["#legacyN0"]);
        Assert.Equal("b", attrNames["#legacyN1"]);
        Assert.Equal("x", attrValues[":legacyV0"].S);
        Assert.Equal("1", attrValues[":legacyV1"].N);
    }

    // ── Custom prefix ───────────────────────────────────────────────

    [Theory]
    [InlineData("qf")]
    [InlineData("sf")]
    [InlineData("legacy")]
    public void Convert_CustomPrefix_AppearsInGeneratedNames(string prefix)
    {
        var conditions = new Dictionary<string, Condition>
        {
            ["x"] = new()
            {
                ComparisonOperator = ComparisonOperator.EQ,
                AttributeValueList = [new AttributeValue { S = "y" }]
            }
        };

        var (expression, attrNames, attrValues) = LegacyConditionConverter.Convert(conditions, prefix);

        Assert.Equal($"#{prefix}N0 = :{prefix}V0", expression);
        Assert.Contains($"#{prefix}N0", attrNames.Keys);
        Assert.Contains($":{prefix}V0", attrValues.Keys);
    }

    // ── Error path ──────────────────────────────────────────────────

    [Fact]
    public void Convert_UnknownOperator_ThrowsArgumentException()
    {
        var conditions = new Dictionary<string, Condition>
        {
            ["x"] = new()
            {
                ComparisonOperator = "BOGUS",
                AttributeValueList = [new AttributeValue { S = "v" }]
            }
        };

        var ex = Assert.Throws<ArgumentException>(() => LegacyConditionConverter.Convert(conditions));
        Assert.Contains("BOGUS", ex.Message);
    }

    [Fact]
    public void Convert_EmptyConditions_ReturnsEmptyExpression()
    {
        var (expression, attrNames, attrValues) = LegacyConditionConverter.Convert([]);

        Assert.Equal(string.Empty, expression);
        Assert.Empty(attrNames);
        Assert.Empty(attrValues);
    }

    // ── BuildProjectionFromAttributesToGet ──────────────────────────

    [Fact]
    public void BuildProjectionFromAttributesToGet_SingleAttribute_ProducesAliasAndName()
    {
        var (projection, names) = LegacyConditionConverter.BuildProjectionFromAttributesToGet(["name"]);

        Assert.Equal("#ag0", projection);
        Assert.Equal("name", names["#ag0"]);
    }

    [Fact]
    public void BuildProjectionFromAttributesToGet_MultipleAttributes_JoinedWithComma()
    {
        var (projection, names) = LegacyConditionConverter.BuildProjectionFromAttributesToGet(
            ["name", "age", "email"]);

        Assert.Equal("#ag0, #ag1, #ag2", projection);
        Assert.Equal("name", names["#ag0"]);
        Assert.Equal("age", names["#ag1"]);
        Assert.Equal("email", names["#ag2"]);
    }

    [Fact]
    public void BuildProjectionFromAttributesToGet_EmptyList_ProducesEmptyProjection()
    {
        var (projection, names) = LegacyConditionConverter.BuildProjectionFromAttributesToGet([]);

        Assert.Equal(string.Empty, projection);
        Assert.Empty(names);
    }
}
