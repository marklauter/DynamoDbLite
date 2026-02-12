namespace DynamoDbLite.Tests.Expressions;

public sealed class KeyConditionExpressionTests
{
    // ── PK-only ─────────────────────────────────────────────────────

    [Fact]
    public void Parse_PkOnly_ReturnsKeyConditionWithoutSortKey()
    {
        var result = KeyConditionExpressionParser.Parse("PK = :pk");

        Assert.NotNull(result.PartitionKey);
        _ = Assert.IsType<PathOperand>(result.PartitionKey.KeyPath);
        _ = Assert.IsType<ValueRefOperand>(result.PartitionKey.Value);
        Assert.Equal(":pk", ((ValueRefOperand)result.PartitionKey.Value).ValueRef);
        Assert.Null(result.SortKey);
    }

    [Fact]
    public void Parse_PkOnly_WithExpressionAttrName()
    {
        var result = KeyConditionExpressionParser.Parse("#pk = :pk");

        var pathOp = Assert.IsType<PathOperand>(result.PartitionKey.KeyPath);
        var nameEl = Assert.IsType<AttributeNameElement>(pathOp.Path.Elements[0]);
        Assert.Equal("#pk", nameEl.Name);
        Assert.Null(result.SortKey);
    }

    // ── PK + SK equality ────────────────────────────────────────────

    [Fact]
    public void Parse_PkAndSkEquality_ReturnsBothConditions()
    {
        var result = KeyConditionExpressionParser.Parse("PK = :pk AND SK = :sk");

        Assert.NotNull(result.PartitionKey);
        var sk = Assert.IsType<SortKeyComparisonCondition>(result.SortKey);
        Assert.Equal("=", sk.Operator);
        Assert.Equal(":sk", ((ValueRefOperand)sk.Value).ValueRef);
    }

    // ── PK + SK comparison operators ────────────────────────────────

    [Theory]
    [InlineData("PK = :pk AND SK < :sk", "<")]
    [InlineData("PK = :pk AND SK <= :sk", "<=")]
    [InlineData("PK = :pk AND SK > :sk", ">")]
    [InlineData("PK = :pk AND SK >= :sk", ">=")]
    public void Parse_PkAndSkComparison_ReturnsCorrectOperator(string expression, string expectedOp)
    {
        var result = KeyConditionExpressionParser.Parse(expression);

        var sk = Assert.IsType<SortKeyComparisonCondition>(result.SortKey);
        Assert.Equal(expectedOp, sk.Operator);
    }

    // ── PK + SK BETWEEN ─────────────────────────────────────────────

    [Fact]
    public void Parse_PkAndSkBetween_ReturnsBetweenCondition()
    {
        var result = KeyConditionExpressionParser.Parse("PK = :pk AND SK BETWEEN :low AND :high");

        var sk = Assert.IsType<SortKeyBetweenCondition>(result.SortKey);
        Assert.Equal(":low", ((ValueRefOperand)sk.Lower).ValueRef);
        Assert.Equal(":high", ((ValueRefOperand)sk.Upper).ValueRef);
    }

    // ── PK + begins_with ────────────────────────────────────────────

    [Fact]
    public void Parse_PkAndBeginsWith_ReturnsBeginsWithCondition()
    {
        var result = KeyConditionExpressionParser.Parse("PK = :pk AND begins_with(SK, :prefix)");

        var sk = Assert.IsType<SortKeyBeginsWithCondition>(result.SortKey);
        Assert.Equal(":prefix", ((ValueRefOperand)sk.Prefix).ValueRef);
    }

    // ── Expression attribute names ──────────────────────────────────

    [Fact]
    public void Parse_WithExpressionAttrNames_ParsesCorrectly()
    {
        var result = KeyConditionExpressionParser.Parse("#pk = :pk AND #sk >= :sk");

        var pkPath = Assert.IsType<PathOperand>(result.PartitionKey.KeyPath);
        Assert.Equal("#pk", ((AttributeNameElement)pkPath.Path.Elements[0]).Name);

        var sk = Assert.IsType<SortKeyComparisonCondition>(result.SortKey);
        var skPath = Assert.IsType<PathOperand>(sk.KeyPath);
        Assert.Equal("#sk", ((AttributeNameElement)skPath.Path.Elements[0]).Name);
    }

    [Fact]
    public void Parse_BeginsWithExpressionAttrNames_ParsesCorrectly()
    {
        var result = KeyConditionExpressionParser.Parse("#pk = :pk AND begins_with(#sk, :prefix)");

        var sk = Assert.IsType<SortKeyBeginsWithCondition>(result.SortKey);
        var skPath = Assert.IsType<PathOperand>(sk.KeyPath);
        Assert.Equal("#sk", ((AttributeNameElement)skPath.Path.Elements[0]).Name);
    }
}
