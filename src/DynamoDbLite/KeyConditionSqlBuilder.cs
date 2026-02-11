using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Dapper;
using DynamoDbLite.Expressions;
using DynamoDbLite.SqliteStores.Models;
using System.Globalization;

namespace DynamoDbLite;

internal sealed record KeyConditionSql(
    string PkValue,
    string? SkWhereSql,
    DynamicParameters? SkParams,
    string OrderByColumn);

internal static class KeyConditionSqlBuilder
{
    internal static KeyConditionSql Build(
        KeyCondition keyCondition,
        KeySchemaInfo keyInfo,
        Dictionary<string, AttributeValue>? exprAttrValues)
    {
        var pkValue = ResolveValue(keyCondition.PartitionKey.Value, exprAttrValues);
        var rangeKey = keyInfo.KeySchema.FirstOrDefault(static k => k.KeyType == KeyType.RANGE);
        var isNumericSk = rangeKey is not null && keyInfo.AttributeDefinitions
            .First(a => a.AttributeName == rangeKey.AttributeName)
            .AttributeType == ScalarAttributeType.N;
        var orderByColumn = isNumericSk ? "sk_num" : "sk";

        if (keyCondition.SortKey is null)
            return new KeyConditionSql(pkValue, null, null, orderByColumn);

        var parameters = new DynamicParameters();

        var skWhere = keyCondition.SortKey switch
        {
            SortKeyComparisonCondition comp => BuildComparison(comp, isNumericSk, orderByColumn, exprAttrValues, parameters),
            SortKeyBetweenCondition between => BuildBetween(between, isNumericSk, orderByColumn, exprAttrValues, parameters),
            SortKeyBeginsWithCondition beginsWith => BuildBeginsWith(beginsWith, exprAttrValues, parameters),
            _ => throw new ArgumentException($"Unknown sort key condition type: {keyCondition.SortKey.GetType().Name}")
        };

        return new KeyConditionSql(pkValue, skWhere, parameters, orderByColumn);
    }

    private static string BuildComparison(
        SortKeyComparisonCondition comp,
        bool isNumericSk,
        string column,
        Dictionary<string, AttributeValue>? exprAttrValues,
        DynamicParameters parameters)
    {
        var rawValue = ResolveValue(comp.Value, exprAttrValues);

        if (isNumericSk)
        {
            parameters.Add("@skVal", double.Parse(rawValue, CultureInfo.InvariantCulture));
            return $"{column} {comp.Operator} @skVal";
        }

        parameters.Add("@skVal", rawValue);
        return $"{column} {comp.Operator} @skVal";
    }

    private static string BuildBetween(
        SortKeyBetweenCondition between,
        bool isNumericSk,
        string column,
        Dictionary<string, AttributeValue>? exprAttrValues,
        DynamicParameters parameters)
    {
        var lower = ResolveValue(between.Lower, exprAttrValues);
        var upper = ResolveValue(between.Upper, exprAttrValues);

        if (isNumericSk)
        {
            parameters.Add("@skLow", double.Parse(lower, CultureInfo.InvariantCulture));
            parameters.Add("@skHigh", double.Parse(upper, CultureInfo.InvariantCulture));
            return $"{column} BETWEEN @skLow AND @skHigh";
        }

        parameters.Add("@skLow", lower);
        parameters.Add("@skHigh", upper);
        return $"{column} BETWEEN @skLow AND @skHigh";
    }

    private static string BuildBeginsWith(
        SortKeyBeginsWithCondition beginsWith,
        Dictionary<string, AttributeValue>? exprAttrValues,
        DynamicParameters parameters)
    {
        var prefix = ResolveValue(beginsWith.Prefix, exprAttrValues);
        parameters.Add("@skPrefix", prefix);

        // Compute exclusive upper bound: increment last character
        var prefixEnd = IncrementPrefix(prefix);
        parameters.Add("@skPrefixEnd", prefixEnd);

        return "sk >= @skPrefix AND sk < @skPrefixEnd";
    }

    private static string ResolveValue(Operand operand, Dictionary<string, AttributeValue>? exprAttrValues) =>
        operand switch
        {
            ValueRefOperand valueRef => exprAttrValues?.TryGetValue(valueRef.ValueRef, out var v) is true
                ? v.S ?? v.N ?? (v.B is not null ? Convert.ToBase64String(v.B.ToArray()) : throw new ArgumentException($"Unsupported key value type for {valueRef.ValueRef}"))
                : throw new ArgumentException($"Expression attribute value {valueRef.ValueRef} is not defined"),
            _ => throw new ArgumentException($"Expected value reference operand, got: {operand.GetType().Name}")
        };

    private static string IncrementPrefix(string prefix)
    {
        if (prefix.Length == 0)
            return "\uffff";

        var chars = prefix.ToCharArray();
        chars[^1]++;
        return new string(chars);
    }
}
