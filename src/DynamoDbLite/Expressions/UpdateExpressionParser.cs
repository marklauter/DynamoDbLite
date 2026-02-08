using Superpower;
using Superpower.Parsers;

namespace DynamoDbLite.Expressions;

internal static class UpdateExpressionParser
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

    // ── Update values ──────────────────────────────────────────────────

    private static readonly TokenListParser<DynamoDbToken, UpdateValue> PathValue =
        Path.Select(static p => (UpdateValue)new PathUpdateValue(p));

    private static readonly TokenListParser<DynamoDbToken, UpdateValue> ValueRefValue =
        Token.EqualTo(DynamoDbToken.ExpressionAttrValue)
            .Select(static t => (UpdateValue)new ValueRefUpdateValue(t.ToStringValue()));

    private static readonly TokenListParser<DynamoDbToken, UpdateValue> IfNotExistsValue =
        from kw in Token.EqualTo(DynamoDbToken.IfNotExists)
        from open in Token.EqualTo(DynamoDbToken.OpenParen)
        from path in Path
        from comma in Token.EqualTo(DynamoDbToken.Comma)
        from def in Superpower.Parse.Ref(() => SimpleValue!)
        from close in Token.EqualTo(DynamoDbToken.CloseParen)
        select (UpdateValue)new IfNotExistsUpdateValue(path, def);

    private static readonly TokenListParser<DynamoDbToken, UpdateValue> ListAppendValue =
        from kw in Token.EqualTo(DynamoDbToken.ListAppend)
        from open in Token.EqualTo(DynamoDbToken.OpenParen)
        from first in Superpower.Parse.Ref(() => SimpleValue!)
        from comma in Token.EqualTo(DynamoDbToken.Comma)
        from second in Superpower.Parse.Ref(() => SimpleValue!)
        from close in Token.EqualTo(DynamoDbToken.CloseParen)
        select (UpdateValue)new ListAppendUpdateValue(first, second);

    private static readonly TokenListParser<DynamoDbToken, UpdateValue> SimpleValue =
        IfNotExistsValue.Try()
        .Or(ListAppendValue.Try())
        .Or(ValueRefValue)
        .Or(PathValue);

    private static readonly TokenListParser<DynamoDbToken, UpdateValue> SetValue =
        from left in SimpleValue
        from rest in (
            from op in Token.EqualTo(DynamoDbToken.Plus).Value("+")
                .Or(Token.EqualTo(DynamoDbToken.Minus).Value("-"))
            from right in SimpleValue
            select (op, right)
        ).OptionalOrDefault()
        select rest.op is not null
            ? new ArithmeticUpdateValue(left, rest.op, rest.right)
            : left;

    // ── Actions ────────────────────────────────────────────────────────

    private static readonly TokenListParser<DynamoDbToken, SetAction> SetActionParser =
        from path in Path
        from eq in Token.EqualTo(DynamoDbToken.Equal)
        from value in SetValue
        select new SetAction(path, value);

    private static readonly TokenListParser<DynamoDbToken, RemoveAction> RemoveActionParser =
        Path.Select(static p => new RemoveAction(p));

    private static readonly TokenListParser<DynamoDbToken, AddAction> AddActionParser =
        from path in Path
        from valueRef in Token.EqualTo(DynamoDbToken.ExpressionAttrValue)
        select new AddAction(path, valueRef.ToStringValue());

    private static readonly TokenListParser<DynamoDbToken, DeleteAction> DeleteActionParser =
        from path in Path
        from valueRef in Token.EqualTo(DynamoDbToken.ExpressionAttrValue)
        select new DeleteAction(path, valueRef.ToStringValue());

    // ── Clauses ────────────────────────────────────────────────────────

    private static readonly TokenListParser<DynamoDbToken, IReadOnlyList<SetAction>> SetClause =
        from kw in Token.EqualTo(DynamoDbToken.Set)
        from actions in SetActionParser.ManyDelimitedBy(Token.EqualTo(DynamoDbToken.Comma))
        select (IReadOnlyList<SetAction>)actions;

    private static readonly TokenListParser<DynamoDbToken, IReadOnlyList<RemoveAction>> RemoveClause =
        from kw in Token.EqualTo(DynamoDbToken.Remove)
        from actions in RemoveActionParser.ManyDelimitedBy(Token.EqualTo(DynamoDbToken.Comma))
        select (IReadOnlyList<RemoveAction>)actions;

    private static readonly TokenListParser<DynamoDbToken, IReadOnlyList<AddAction>> AddClause =
        from kw in Token.EqualTo(DynamoDbToken.Add)
        from actions in AddActionParser.ManyDelimitedBy(Token.EqualTo(DynamoDbToken.Comma))
        select (IReadOnlyList<AddAction>)actions;

    private static readonly TokenListParser<DynamoDbToken, IReadOnlyList<DeleteAction>> DeleteClause =
        from kw in Token.EqualTo(DynamoDbToken.Delete)
        from actions in DeleteActionParser.ManyDelimitedBy(Token.EqualTo(DynamoDbToken.Comma))
        select (IReadOnlyList<DeleteAction>)actions;

    private static readonly TokenListParser<DynamoDbToken, UpdateExpression> Root =
        from sets in SetClause.OptionalOrDefault([])
        from removes in RemoveClause.OptionalOrDefault([])
        from adds in AddClause.OptionalOrDefault([])
        from deletes in DeleteClause.OptionalOrDefault([])
        select new UpdateExpression(sets, removes, adds, deletes);

    internal static UpdateExpression Parse(string expression)
    {
        var tokens = Tokenizer.Tokenize(expression);
        return Root.Parse(tokens);
    }
}
