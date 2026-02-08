using Superpower;
using Superpower.Parsers;

namespace DynamoDbLite.Expressions;

internal static class ProjectionExpressionParser
{
    private static readonly Tokenizer<DynamoDbToken> Tokenizer = DynamoDbTokenizer.Instance;

    private static readonly TokenListParser<DynamoDbToken, PathElement> IdentifierElement =
        Token.EqualTo(DynamoDbToken.Identifier).Select(static t => (PathElement)new AttributeNameElement(t.ToStringValue()))
        .Or(Token.EqualTo(DynamoDbToken.ExpressionAttrName).Select(static t => (PathElement)new AttributeNameElement(t.ToStringValue())));

    private static readonly TokenListParser<DynamoDbToken, PathElement> ListIndexElement =
        from open in Token.EqualTo(DynamoDbToken.OpenBracket)
        from index in Token.EqualTo(DynamoDbToken.Number)
        from close in Token.EqualTo(DynamoDbToken.CloseBracket)
        select (PathElement)new ListIndexElement(int.Parse(index.ToStringValue(), System.Globalization.CultureInfo.InvariantCulture));

    private static readonly TokenListParser<DynamoDbToken, PathElement> DotElement =
        from dot in Token.EqualTo(DynamoDbToken.Dot)
        from name in Token.EqualTo(DynamoDbToken.Identifier).Select(static t => (PathElement)new AttributeNameElement(t.ToStringValue()))
            .Or(Token.EqualTo(DynamoDbToken.ExpressionAttrName).Select(static t => (PathElement)new AttributeNameElement(t.ToStringValue())))
        select name;

    private static readonly TokenListParser<DynamoDbToken, PathElement> Suffix =
        ListIndexElement.Or(DotElement);

    private static readonly TokenListParser<DynamoDbToken, AttributePath> Path =
        from first in IdentifierElement
        from rest in Suffix.Many()
        select new AttributePath([first, .. rest]);

    private static readonly TokenListParser<DynamoDbToken, IReadOnlyList<AttributePath>> Projection =
        Path.ManyDelimitedBy(Token.EqualTo(DynamoDbToken.Comma))
            .Select(static paths => (IReadOnlyList<AttributePath>)paths);

    internal static IReadOnlyList<AttributePath> Parse(
        string expression,
        Dictionary<string, string>? expressionAttributeNames = null)
    {
        var tokens = Tokenizer.Tokenize(expression);
        var paths = Projection.Parse(tokens);

        return expressionAttributeNames is null ? paths : paths.Select(p => ResolvePath(p, expressionAttributeNames)).ToList();
    }

    private static AttributePath ResolvePath(AttributePath path, Dictionary<string, string> names) =>
        new(path.Elements.Select(e => e is AttributeNameElement { Name: var name } && name.StartsWith('#')
            ? new AttributeNameElement(ExpressionHelper.ResolveAttributeName(name, names))
            : e).ToList());
}
