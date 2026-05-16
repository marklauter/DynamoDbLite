using Amazon.DynamoDBv2.Model;
using System.Globalization;

namespace DynamoDbLite.Expressions;

// Converts pre-Expression DynamoDB API shapes (KeyConditions, QueryFilter, ScanFilter,
// AttributesToGet) into the modern expression form. Pure functions; no I/O.
internal static class LegacyConditionConverter
{
    internal static (string FilterExpression, Dictionary<string, string> AttrNames, Dictionary<string, AttributeValue> AttrValues)
        Convert(Dictionary<string, Condition> conditions, string prefix = "legacy")
    {
        var expressions = new List<string>(conditions.Count);
        var attrNames = new Dictionary<string, string>(conditions.Count);
        var attrValues = new Dictionary<string, AttributeValue>(conditions.Count);
        var i = 0;

        foreach (var (attributeName, condition) in conditions)
        {
            var nameKey = BuildLegacyNameKey(prefix, i);
            attrNames[nameKey] = attributeName;

            var expr = condition.ComparisonOperator.Value switch
            {
                "EQ" => BuildSingleValueCondition(nameKey, "=", prefix, i, condition, attrValues),
                "NE" => BuildSingleValueCondition(nameKey, "<>", prefix, i, condition, attrValues),
                "LT" => BuildSingleValueCondition(nameKey, "<", prefix, i, condition, attrValues),
                "LE" => BuildSingleValueCondition(nameKey, "<=", prefix, i, condition, attrValues),
                "GT" => BuildSingleValueCondition(nameKey, ">", prefix, i, condition, attrValues),
                "GE" => BuildSingleValueCondition(nameKey, ">=", prefix, i, condition, attrValues),
                "BEGINS_WITH" => BuildBeginsWithCondition(nameKey, prefix, i, condition, attrValues),
                "CONTAINS" => BuildContainsCondition(nameKey, prefix, i, condition, attrValues),
                "BETWEEN" => BuildBetweenCondition(nameKey, prefix, i, condition, attrValues),
                "NOT_NULL" => $"attribute_exists({nameKey})",
                "NULL" => $"attribute_not_exists({nameKey})",
                _ => throw new ArgumentException($"Unsupported comparison operator: {condition.ComparisonOperator.Value}")
            };

            expressions.Add(expr);
            i++;
        }

        return (string.Join(" AND ", expressions), attrNames, attrValues);
    }

    internal static (string Projection, Dictionary<string, string> Names) BuildProjectionFromAttributesToGet(
        List<string> attributesToGet)
    {
        var names = new Dictionary<string, string>(attributesToGet.Count);
        var aliases = new string[attributesToGet.Count];
        for (var i = 0; i < attributesToGet.Count; i++)
        {
            var alias = $"#ag{i}";
            names[alias] = attributesToGet[i];
            aliases[i] = alias;
        }

        return (string.Join(", ", aliases), names);
    }

    private static string BuildSingleValueCondition(
        string nameKey, string op, string prefix, int index,
        Condition condition, Dictionary<string, AttributeValue> attrValues)
    {
        var valueKey = BuildLegacyValueKey(prefix, index);
        attrValues[valueKey] = condition.AttributeValueList[0];
        return $"{nameKey} {op} {valueKey}";
    }

    private static string BuildBeginsWithCondition(
        string nameKey, string prefix, int index,
        Condition condition, Dictionary<string, AttributeValue> attrValues)
    {
        var valueKey = BuildLegacyValueKey(prefix, index);
        attrValues[valueKey] = condition.AttributeValueList[0];
        return $"begins_with({nameKey}, {valueKey})";
    }

    private static string BuildContainsCondition(
        string nameKey, string prefix, int index,
        Condition condition, Dictionary<string, AttributeValue> attrValues)
    {
        var valueKey = BuildLegacyValueKey(prefix, index);
        attrValues[valueKey] = condition.AttributeValueList[0];
        return $"contains({nameKey}, {valueKey})";
    }

    private static string BuildBetweenCondition(
        string nameKey, string prefix, int index,
        Condition condition, Dictionary<string, AttributeValue> attrValues)
    {
        var lowKey = BuildLegacyValueKey(prefix, index, 'a');
        var highKey = BuildLegacyValueKey(prefix, index, 'b');
        attrValues[lowKey] = condition.AttributeValueList[0];
        attrValues[highKey] = condition.AttributeValueList[1];
        return $"{nameKey} BETWEEN {lowKey} AND {highKey}";
    }

    // Builds "#{prefix}N{index}" without intermediate allocations.
    private static string BuildLegacyNameKey(string prefix, int index) =>
        string.Create(2 + prefix.Length + CountDigits(index), (prefix, index), static (span, state) =>
        {
            span[0] = '#';
            state.prefix.AsSpan().CopyTo(span[1..]);
            span[1 + state.prefix.Length] = 'N';
            _ = state.index.TryFormat(span[(2 + state.prefix.Length)..], out _, provider: CultureInfo.InvariantCulture);
        });

    // Builds ":{prefix}V{index}" or ":{prefix}V{index}{suffix}" without intermediate allocations.
    private static string BuildLegacyValueKey(string prefix, int index, char suffix = '\0')
    {
        var hasSuffix = suffix != '\0';
        var len = 2 + prefix.Length + CountDigits(index) + (hasSuffix ? 1 : 0);
        return string.Create(len, (prefix, index, suffix, hasSuffix), static (span, state) =>
        {
            span[0] = ':';
            state.prefix.AsSpan().CopyTo(span[1..]);
            span[1 + state.prefix.Length] = 'V';
            var digitStart = 2 + state.prefix.Length;
            _ = state.index.TryFormat(span[digitStart..], out var digitsWritten, provider: CultureInfo.InvariantCulture);
            if (state.hasSuffix)
                span[digitStart + digitsWritten] = state.suffix;
        });
    }

    private static int CountDigits(int n) =>
        n < 10 ? 1
        : n < 100 ? 2
        : n < 1000 ? 3
        : n < 10000 ? 4
        : n < 100000 ? 5
        : n < 1000000 ? 6
        : n < 10000000 ? 7
        : n < 100000000 ? 8
        : n < 1000000000 ? 9
        : 10;
}
