using Superpower;
using Superpower.Model;
using System.Collections.Frozen;

namespace DynamoDbLite.Expressions;

internal sealed class DynamoDbTokenizer : Tokenizer<DynamoDbToken>
{
    private static readonly FrozenDictionary<string, DynamoDbToken> Keywords =
        new Dictionary<string, DynamoDbToken>(StringComparer.OrdinalIgnoreCase)
        {
            ["AND"] = DynamoDbToken.And,
            ["OR"] = DynamoDbToken.Or,
            ["NOT"] = DynamoDbToken.Not,
            ["BETWEEN"] = DynamoDbToken.Between,
            ["IN"] = DynamoDbToken.In,
            ["SET"] = DynamoDbToken.Set,
            ["REMOVE"] = DynamoDbToken.Remove,
            ["ADD"] = DynamoDbToken.Add,
            ["DELETE"] = DynamoDbToken.Delete,
            ["attribute_exists"] = DynamoDbToken.AttributeExists,
            ["attribute_not_exists"] = DynamoDbToken.AttributeNotExists,
            ["attribute_type"] = DynamoDbToken.AttributeType,
            ["begins_with"] = DynamoDbToken.BeginsWith,
            ["contains"] = DynamoDbToken.Contains,
            ["size"] = DynamoDbToken.Size,
            ["if_not_exists"] = DynamoDbToken.IfNotExists,
            ["list_append"] = DynamoDbToken.ListAppend,
        }.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);

    protected override IEnumerable<Result<DynamoDbToken>> Tokenize(TextSpan span)
    {
        var next = SkipWhiteSpace(span);

        while (next.HasValue)
        {
            var ch = next.Value;

            if (ch == '#')
            {
                var start = next.Location;
                next = next.Remainder.ConsumeChar();
                while (next.HasValue && (char.IsLetterOrDigit(next.Value) || next.Value == '_'))
                    next = next.Remainder.ConsumeChar();
                yield return Result.Value(DynamoDbToken.ExpressionAttrName, start, next.Location);
            }
            else if (ch == ':')
            {
                var start = next.Location;
                next = next.Remainder.ConsumeChar();
                while (next.HasValue && (char.IsLetterOrDigit(next.Value) || next.Value == '_'))
                    next = next.Remainder.ConsumeChar();
                yield return Result.Value(DynamoDbToken.ExpressionAttrValue, start, next.Location);
            }
            else if (char.IsLetter(ch) || ch == '_')
            {
                var start = next.Location;
                while (next.HasValue && (char.IsLetterOrDigit(next.Value) || next.Value == '_'))
                    next = next.Remainder.ConsumeChar();
                var text = start.Until(next.Location);
                var token = Keywords.TryGetValue(text.ToStringValue(), out var kw) ? kw : DynamoDbToken.Identifier;
                yield return Result.Value(token, start, next.Location);
            }
            else if (char.IsDigit(ch))
            {
                var start = next.Location;
                while (next.HasValue && char.IsDigit(next.Value))
                    next = next.Remainder.ConsumeChar();
                yield return Result.Value(DynamoDbToken.Number, start, next.Location);
            }
            else if (ch == '=')
            {
                yield return Result.Value(DynamoDbToken.Equal, next.Location, next.Remainder);
                next = next.Remainder.ConsumeChar();
            }
            else if (ch == '<')
            {
                var start = next.Location;
                next = next.Remainder.ConsumeChar();
                if (next.HasValue && next.Value == '>')
                {
                    next = next.Remainder.ConsumeChar();
                    yield return Result.Value(DynamoDbToken.NotEqual, start, next.Location);
                }
                else if (next.HasValue && next.Value == '=')
                {
                    next = next.Remainder.ConsumeChar();
                    yield return Result.Value(DynamoDbToken.LessThanOrEqual, start, next.Location);
                }
                else
                {
                    yield return Result.Value(DynamoDbToken.LessThan, start, next.Location);
                }
            }
            else if (ch == '>')
            {
                var start = next.Location;
                next = next.Remainder.ConsumeChar();
                if (next.HasValue && next.Value == '=')
                {
                    next = next.Remainder.ConsumeChar();
                    yield return Result.Value(DynamoDbToken.GreaterThanOrEqual, start, next.Location);
                }
                else
                {
                    yield return Result.Value(DynamoDbToken.GreaterThan, start, next.Location);
                }
            }
            else if (ch == ',')
            {
                yield return Result.Value(DynamoDbToken.Comma, next.Location, next.Remainder);
                next = next.Remainder.ConsumeChar();
            }
            else if (ch == '.')
            {
                yield return Result.Value(DynamoDbToken.Dot, next.Location, next.Remainder);
                next = next.Remainder.ConsumeChar();
            }
            else if (ch == '(')
            {
                yield return Result.Value(DynamoDbToken.OpenParen, next.Location, next.Remainder);
                next = next.Remainder.ConsumeChar();
            }
            else if (ch == ')')
            {
                yield return Result.Value(DynamoDbToken.CloseParen, next.Location, next.Remainder);
                next = next.Remainder.ConsumeChar();
            }
            else if (ch == '[')
            {
                yield return Result.Value(DynamoDbToken.OpenBracket, next.Location, next.Remainder);
                next = next.Remainder.ConsumeChar();
            }
            else if (ch == ']')
            {
                yield return Result.Value(DynamoDbToken.CloseBracket, next.Location, next.Remainder);
                next = next.Remainder.ConsumeChar();
            }
            else if (ch == '+')
            {
                yield return Result.Value(DynamoDbToken.Plus, next.Location, next.Remainder);
                next = next.Remainder.ConsumeChar();
            }
            else if (ch == '-')
            {
                yield return Result.Value(DynamoDbToken.Minus, next.Location, next.Remainder);
                next = next.Remainder.ConsumeChar();
            }
            else
            {
                yield return Result.Empty<DynamoDbToken>(next.Location, $"Unexpected character '{ch}'");
                next = next.Remainder.ConsumeChar();
            }

            next = SkipWhiteSpace(next.Location);
        }
    }

    private static new Result<char> SkipWhiteSpace(TextSpan span)
    {
        var next = span.ConsumeChar();
        while (next.HasValue && char.IsWhiteSpace(next.Value))
            next = next.Remainder.ConsumeChar();
        return next;
    }
}
