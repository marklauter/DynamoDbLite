using Superpower;
using Superpower.Parsers;

namespace DynamoDbLite.Expressions;

internal static class KeyConditionExpressionParser
{
    private static readonly Tokenizer<DynamoDbToken> Tokenizer = DynamoDbTokenizer.Instance;

    // ── Path parsing (top-level identifiers only) ───────────────────

    private static readonly TokenListParser<DynamoDbToken, Operand> PathOperand =
        Token.EqualTo(DynamoDbToken.Identifier).Select(static t => (Operand)new PathOperand(
            new AttributePath([new AttributeNameElement(t.ToStringValue())])))
        .Or(Token.EqualTo(DynamoDbToken.ExpressionAttrName).Select(static t => (Operand)new PathOperand(
            new AttributePath([new AttributeNameElement(t.ToStringValue())]))));

    // ── Value ref ───────────────────────────────────────────────────

    private static readonly TokenListParser<DynamoDbToken, Operand> ValueRef =
        Token.EqualTo(DynamoDbToken.ExpressionAttrValue)
            .Select(static t => (Operand)new ValueRefOperand(t.ToStringValue()));

    // ── Comparison operators ────────────────────────────────────────

    private static readonly TokenListParser<DynamoDbToken, string> ComparisonOp =
        Token.EqualTo(DynamoDbToken.Equal).Value("=")
        .Or(Token.EqualTo(DynamoDbToken.LessThanOrEqual).Value("<="))
        .Or(Token.EqualTo(DynamoDbToken.GreaterThanOrEqual).Value(">="))
        .Or(Token.EqualTo(DynamoDbToken.LessThan).Value("<"))
        .Or(Token.EqualTo(DynamoDbToken.GreaterThan).Value(">"));

    // ── PK condition: path = :value ─────────────────────────────────

    private static readonly TokenListParser<DynamoDbToken, PartitionKeyCondition> PkCondition =
        from path in PathOperand
        from eq in Token.EqualTo(DynamoDbToken.Equal)
        from value in ValueRef
        select new PartitionKeyCondition(path, value);

    // ── SK conditions ───────────────────────────────────────────────

    private static readonly TokenListParser<DynamoDbToken, SortKeyCondition> SkComparison =
        from path in PathOperand
        from op in ComparisonOp
        from value in ValueRef
        select (SortKeyCondition)new SortKeyComparisonCondition(path, op, value);

    private static readonly TokenListParser<DynamoDbToken, SortKeyCondition> SkBetween =
        from path in PathOperand
        from kw in Token.EqualTo(DynamoDbToken.Between)
        from lower in ValueRef
        from and in Token.EqualTo(DynamoDbToken.And)
        from upper in ValueRef
        select (SortKeyCondition)new SortKeyBetweenCondition(path, lower, upper);

    private static readonly TokenListParser<DynamoDbToken, SortKeyCondition> SkBeginsWith =
        from kw in Token.EqualTo(DynamoDbToken.BeginsWith)
        from open in Token.EqualTo(DynamoDbToken.OpenParen)
        from path in PathOperand
        from comma in Token.EqualTo(DynamoDbToken.Comma)
        from prefix in ValueRef
        from close in Token.EqualTo(DynamoDbToken.CloseParen)
        select (SortKeyCondition)new SortKeyBeginsWithCondition(path, prefix);

    private static readonly TokenListParser<DynamoDbToken, SortKeyCondition> SkCondition =
        SkBeginsWith.Try()
        .Or(SkBetween.Try())
        .Or(SkComparison);

    // ── Root: PK AND SK  or  PK only ───────────────────────────────

    private static readonly TokenListParser<DynamoDbToken, KeyCondition> PkAndSk =
        from pk in PkCondition
        from and in Token.EqualTo(DynamoDbToken.And)
        from sk in SkCondition
        select new KeyCondition(pk, sk);

    private static readonly TokenListParser<DynamoDbToken, KeyCondition> PkOnly =
        PkCondition.Select(static pk => new KeyCondition(pk, null));

    private static readonly TokenListParser<DynamoDbToken, KeyCondition> Root =
        PkAndSk.Try().Or(PkOnly);

    internal static KeyCondition Parse(string expression)
    {
        var tokens = Tokenizer.Tokenize(expression);
        return Root.Parse(tokens);
    }
}
