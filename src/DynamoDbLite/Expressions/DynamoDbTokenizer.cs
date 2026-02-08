using Superpower;
using Superpower.Model;
using Superpower.Parsers;
using Superpower.Tokenizers;

namespace DynamoDbLite.Expressions;

internal static class DynamoDbTokenizer
{
    private static readonly TextParser<TextSpan> IdentifierChars =
        Span.MatchedBy(
            Character.Letter.Or(Character.EqualTo('_'))
                .IgnoreThen(Character.LetterOrDigit.Or(Character.EqualTo('_')).Many()));

    private static readonly TextParser<TextSpan> ExpressionAttrNameSpan =
        Span.MatchedBy(
            Character.EqualTo('#')
                .IgnoreThen(Character.LetterOrDigit.Or(Character.EqualTo('_')).AtLeastOnce()));

    private static readonly TextParser<TextSpan> ExpressionAttrValueSpan =
        Span.MatchedBy(
            Character.EqualTo(':')
                .IgnoreThen(Character.LetterOrDigit.Or(Character.EqualTo('_')).AtLeastOnce()));

    internal static readonly Tokenizer<DynamoDbToken> Instance =
        new TokenizerBuilder<DynamoDbToken>()

            // Whitespace
            .Ignore(Span.WhiteSpace)

            // Prefix tokens (# and : are natural delimiters)
            .Match(ExpressionAttrNameSpan, DynamoDbToken.ExpressionAttrName)
            .Match(ExpressionAttrValueSpan, DynamoDbToken.ExpressionAttrValue)

            // Multi-character operators (before single-character ones)
            .Match(Span.EqualTo("<>"), DynamoDbToken.NotEqual)
            .Match(Span.EqualTo("<="), DynamoDbToken.LessThanOrEqual)
            .Match(Span.EqualTo(">="), DynamoDbToken.GreaterThanOrEqual)

            // Single-character operators
            .Match(Character.EqualTo('='), DynamoDbToken.Equal)
            .Match(Character.EqualTo('<'), DynamoDbToken.LessThan)
            .Match(Character.EqualTo('>'), DynamoDbToken.GreaterThan)

            // Punctuation
            .Match(Character.EqualTo(','), DynamoDbToken.Comma)
            .Match(Character.EqualTo('.'), DynamoDbToken.Dot)
            .Match(Character.EqualTo('('), DynamoDbToken.OpenParen)
            .Match(Character.EqualTo(')'), DynamoDbToken.CloseParen)
            .Match(Character.EqualTo('['), DynamoDbToken.OpenBracket)
            .Match(Character.EqualTo(']'), DynamoDbToken.CloseBracket)
            .Match(Character.EqualTo('+'), DynamoDbToken.Plus)
            .Match(Character.EqualTo('-'), DynamoDbToken.Minus)

            // Keywords (case-insensitive, require delimiters)
            .Match(Span.EqualToIgnoreCase("AND"), DynamoDbToken.And, requireDelimiters: true)
            .Match(Span.EqualToIgnoreCase("OR"), DynamoDbToken.Or, requireDelimiters: true)
            .Match(Span.EqualToIgnoreCase("NOT"), DynamoDbToken.Not, requireDelimiters: true)
            .Match(Span.EqualToIgnoreCase("BETWEEN"), DynamoDbToken.Between, requireDelimiters: true)
            .Match(Span.EqualToIgnoreCase("IN"), DynamoDbToken.In, requireDelimiters: true)
            .Match(Span.EqualToIgnoreCase("SET"), DynamoDbToken.Set, requireDelimiters: true)
            .Match(Span.EqualToIgnoreCase("REMOVE"), DynamoDbToken.Remove, requireDelimiters: true)
            .Match(Span.EqualToIgnoreCase("ADD"), DynamoDbToken.Add, requireDelimiters: true)
            .Match(Span.EqualToIgnoreCase("DELETE"), DynamoDbToken.Delete, requireDelimiters: true)

            // Functions (case-insensitive, require delimiters)
            .Match(Span.EqualToIgnoreCase("attribute_exists"), DynamoDbToken.AttributeExists, requireDelimiters: true)
            .Match(Span.EqualToIgnoreCase("attribute_not_exists"), DynamoDbToken.AttributeNotExists, requireDelimiters: true)
            .Match(Span.EqualToIgnoreCase("attribute_type"), DynamoDbToken.AttributeType, requireDelimiters: true)
            .Match(Span.EqualToIgnoreCase("begins_with"), DynamoDbToken.BeginsWith, requireDelimiters: true)
            .Match(Span.EqualToIgnoreCase("contains"), DynamoDbToken.Contains, requireDelimiters: true)
            .Match(Span.EqualToIgnoreCase("size"), DynamoDbToken.Size, requireDelimiters: true)
            .Match(Span.EqualToIgnoreCase("if_not_exists"), DynamoDbToken.IfNotExists, requireDelimiters: true)
            .Match(Span.EqualToIgnoreCase("list_append"), DynamoDbToken.ListAppend, requireDelimiters: true)

            // Generic identifier (after keywords so they take priority)
            .Match(IdentifierChars, DynamoDbToken.Identifier, requireDelimiters: true)

            // Number literals
            .Match(Numerics.Natural, DynamoDbToken.Number)

            .Build();
}
