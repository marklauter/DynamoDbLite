using Superpower;
using Superpower.Parsers;

namespace DynamoDbLite.Expressions;

internal static class ConditionExpressionParser
{
    private static readonly Tokenizer<DynamoDbToken> Tokenizer = DynamoDbTokenizer.Instance;

    // ── Path parsing ───────────────────────────────────────────────────

    private static readonly TokenListParser<DynamoDbToken, PathElement> IdentifierPathElement =
        Token.EqualTo(DynamoDbToken.Identifier).Select(static t => (PathElement)new AttributeNameElement(t.ToStringValue()))
        .Or(Token.EqualTo(DynamoDbToken.ExpressionAttrName).Select(static t => (PathElement)new AttributeNameElement(t.ToStringValue())));

    private static readonly TokenListParser<DynamoDbToken, PathElement> IndexPathElement =
        from open in Token.EqualTo(DynamoDbToken.OpenBracket)
        from index in Token.EqualTo(DynamoDbToken.Number)
        from close in Token.EqualTo(DynamoDbToken.CloseBracket)
        select (PathElement)new ListIndexElement(int.Parse(index.ToStringValue(), System.Globalization.CultureInfo.InvariantCulture));

    private static readonly TokenListParser<DynamoDbToken, PathElement> DotPathElement =
        from dot in Token.EqualTo(DynamoDbToken.Dot)
        from name in IdentifierPathElement
        select name;

    private static readonly TokenListParser<DynamoDbToken, PathElement> PathSuffix =
        IndexPathElement.Or(DotPathElement);

    private static readonly TokenListParser<DynamoDbToken, AttributePath> Path =
        from first in IdentifierPathElement
        from rest in PathSuffix.Many()
        select new AttributePath([first, .. rest]);

    // ── Operands ───────────────────────────────────────────────────────

    private static readonly TokenListParser<DynamoDbToken, Operand> ValueRef =
        Token.EqualTo(DynamoDbToken.ExpressionAttrValue)
            .Select(static t => (Operand)new ValueRefOperand(t.ToStringValue()));

    private static readonly TokenListParser<DynamoDbToken, Operand> SizeFunction =
        from kw in Token.EqualTo(DynamoDbToken.Size)
        from open in Token.EqualTo(DynamoDbToken.OpenParen)
        from path in Path
        from close in Token.EqualTo(DynamoDbToken.CloseParen)
        select (Operand)new SizeFunctionOperand(path);

    private static readonly TokenListParser<DynamoDbToken, Operand> PathOp =
        Path.Select(static p => (Operand)new PathOperand(p));

    private static readonly TokenListParser<DynamoDbToken, Operand> Operand =
        SizeFunction.Try().Or(ValueRef).Or(PathOp);

    // ── Comparison operators ───────────────────────────────────────────

    private static readonly TokenListParser<DynamoDbToken, string> ComparisonOp =
        Token.EqualTo(DynamoDbToken.Equal).Value("=")
        .Or(Token.EqualTo(DynamoDbToken.NotEqual).Value("<>"))
        .Or(Token.EqualTo(DynamoDbToken.LessThanOrEqual).Value("<="))
        .Or(Token.EqualTo(DynamoDbToken.GreaterThanOrEqual).Value(">="))
        .Or(Token.EqualTo(DynamoDbToken.LessThan).Value("<"))
        .Or(Token.EqualTo(DynamoDbToken.GreaterThan).Value(">"));

    // ── Condition terms ────────────────────────────────────────────────

    private static readonly TokenListParser<DynamoDbToken, ConditionNode> Comparison =
        from left in Operand
        from op in ComparisonOp
        from right in Operand
        select (ConditionNode)new ComparisonNode(left, op, right);

    private static readonly TokenListParser<DynamoDbToken, ConditionNode> BetweenExpr =
        from value in Operand
        from kw in Token.EqualTo(DynamoDbToken.Between)
        from lower in Operand
        from and in Token.EqualTo(DynamoDbToken.And)
        from upper in Operand
        select (ConditionNode)new BetweenNode(value, lower, upper);

    private static readonly TokenListParser<DynamoDbToken, ConditionNode> InExpr =
        from value in Operand
        from kw in Token.EqualTo(DynamoDbToken.In)
        from open in Token.EqualTo(DynamoDbToken.OpenParen)
        from list in Operand.ManyDelimitedBy(Token.EqualTo(DynamoDbToken.Comma))
        from close in Token.EqualTo(DynamoDbToken.CloseParen)
        select (ConditionNode)new InNode(value, list);

    private static readonly TokenListParser<DynamoDbToken, ConditionNode> AttributeExistsCall =
        from kw in Token.EqualTo(DynamoDbToken.AttributeExists)
        from open in Token.EqualTo(DynamoDbToken.OpenParen)
        from path in Operand
        from close in Token.EqualTo(DynamoDbToken.CloseParen)
        select (ConditionNode)new FunctionConditionNode("attribute_exists", [path]);

    private static readonly TokenListParser<DynamoDbToken, ConditionNode> AttributeNotExistsCall =
        from kw in Token.EqualTo(DynamoDbToken.AttributeNotExists)
        from open in Token.EqualTo(DynamoDbToken.OpenParen)
        from path in Operand
        from close in Token.EqualTo(DynamoDbToken.CloseParen)
        select (ConditionNode)new FunctionConditionNode("attribute_not_exists", [path]);

    private static readonly TokenListParser<DynamoDbToken, ConditionNode> AttributeTypeCall =
        from kw in Token.EqualTo(DynamoDbToken.AttributeType)
        from open in Token.EqualTo(DynamoDbToken.OpenParen)
        from path in Operand
        from comma in Token.EqualTo(DynamoDbToken.Comma)
        from typeVal in Operand
        from close in Token.EqualTo(DynamoDbToken.CloseParen)
        select (ConditionNode)new FunctionConditionNode("attribute_type", [path, typeVal]);

    private static readonly TokenListParser<DynamoDbToken, ConditionNode> BeginsWithCall =
        from kw in Token.EqualTo(DynamoDbToken.BeginsWith)
        from open in Token.EqualTo(DynamoDbToken.OpenParen)
        from path in Operand
        from comma in Token.EqualTo(DynamoDbToken.Comma)
        from substr in Operand
        from close in Token.EqualTo(DynamoDbToken.CloseParen)
        select (ConditionNode)new FunctionConditionNode("begins_with", [path, substr]);

    private static readonly TokenListParser<DynamoDbToken, ConditionNode> ContainsCall =
        from kw in Token.EqualTo(DynamoDbToken.Contains)
        from open in Token.EqualTo(DynamoDbToken.OpenParen)
        from path in Operand
        from comma in Token.EqualTo(DynamoDbToken.Comma)
        from operand in Operand
        from close in Token.EqualTo(DynamoDbToken.CloseParen)
        select (ConditionNode)new FunctionConditionNode("contains", [path, operand]);

    private static readonly TokenListParser<DynamoDbToken, ConditionNode> FunctionCall =
        AttributeExistsCall.Try()
        .Or(AttributeNotExistsCall.Try())
        .Or(AttributeTypeCall.Try())
        .Or(BeginsWithCall.Try())
        .Or(ContainsCall);

    private static readonly TokenListParser<DynamoDbToken, ConditionNode> ParenExpr =
        from open in Token.EqualTo(DynamoDbToken.OpenParen)
        from expr in Superpower.Parse.Ref(() => OrExpr!)
        from close in Token.EqualTo(DynamoDbToken.CloseParen)
        select expr;

    private static readonly TokenListParser<DynamoDbToken, ConditionNode> Atom =
        FunctionCall.Try()
        .Or(ParenExpr.Try())
        .Or(BetweenExpr.Try())
        .Or(InExpr.Try())
        .Or(Comparison);

    private static readonly TokenListParser<DynamoDbToken, ConditionNode> NotExpr =
        (from kw in Token.EqualTo(DynamoDbToken.Not)
         from inner in Superpower.Parse.Ref(() => NotExpr!)
         select (ConditionNode)new NotNode(inner))
        .Or(Atom);

    private static readonly TokenListParser<DynamoDbToken, ConditionNode> AndExpr =
        Superpower.Parse.Chain(
            Token.EqualTo(DynamoDbToken.And).Value("AND"),
            NotExpr,
            static (op, left, right) => new LogicalNode(left, op, right));

    private static readonly TokenListParser<DynamoDbToken, ConditionNode> OrExpr =
        Superpower.Parse.Chain(
            Token.EqualTo(DynamoDbToken.Or).Value("OR"),
            AndExpr,
            static (op, left, right) => new LogicalNode(left, op, right));

    private static readonly TokenListParser<DynamoDbToken, ConditionNode> Root = OrExpr;

    internal static ConditionNode Parse(string expression)
    {
        var tokens = Tokenizer.Tokenize(expression);
        return Root.Parse(tokens);
    }
}
