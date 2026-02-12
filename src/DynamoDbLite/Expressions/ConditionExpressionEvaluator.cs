using Amazon.DynamoDBv2.Model;
using System.Globalization;

namespace DynamoDbLite.Expressions;

internal static class ConditionExpressionEvaluator
{
    internal static bool Evaluate(
        ConditionNode node,
        Dictionary<string, AttributeValue>? item,
        Dictionary<string, string>? expressionAttributeNames,
        Dictionary<string, AttributeValue>? expressionAttributeValues) =>
        node switch
        {
            ComparisonNode comp => EvaluateComparison(comp, item, expressionAttributeNames, expressionAttributeValues),
            BetweenNode between => EvaluateBetween(between, item, expressionAttributeNames, expressionAttributeValues),
            InNode inNode => EvaluateIn(inNode, item, expressionAttributeNames, expressionAttributeValues),
            LogicalNode logical => EvaluateLogical(logical, item, expressionAttributeNames, expressionAttributeValues),
            NotNode not => !Evaluate(not.Inner, item, expressionAttributeNames, expressionAttributeValues),
            FunctionConditionNode func => EvaluateFunction(func, item, expressionAttributeNames, expressionAttributeValues),
            _ => throw new ArgumentException($"Unknown condition node type: {node.GetType().Name}")
        };

    private static AttributeValue? ResolveOperand(
        Operand operand,
        Dictionary<string, AttributeValue>? item,
        Dictionary<string, string>? expressionAttributeNames,
        Dictionary<string, AttributeValue>? expressionAttributeValues) =>
        operand switch
        {
            PathOperand path => ExpressionHelper.ResolvePath(item, path.Path, expressionAttributeNames),
            ValueRefOperand valueRef => expressionAttributeValues?.TryGetValue(valueRef.ValueRef, out var v) is true
                ? v
                : throw new ArgumentException($"Expression attribute value {valueRef.ValueRef} is not defined"),
            SizeFunctionOperand sizeOp => EvaluateSize(sizeOp, item, expressionAttributeNames),
            LiteralOperand lit => lit.Value,
            _ => throw new ArgumentException($"Unknown operand type: {operand.GetType().Name}")
        };

    private static AttributeValue EvaluateSize(
        SizeFunctionOperand sizeOp,
        Dictionary<string, AttributeValue>? item,
        Dictionary<string, string>? expressionAttributeNames)
    {
        var value = ExpressionHelper.ResolvePath(item, sizeOp.Path, expressionAttributeNames);
        if (value is null)
            return new AttributeValue { N = "0" };

        var size = value switch
        {
            { S: not null } => value.S.Length,
            { N: not null } => value.N.Length,
            { B: not null } => (int)value.B.Length,
            { SS: not null } => value.SS.Count,
            { NS: not null } => value.NS.Count,
            { BS: not null } => value.BS.Count,
            { L: not null } => value.L.Count,
            { M: not null } => value.M.Count,
            _ => 0
        };

        return new AttributeValue { N = size.ToString(CultureInfo.InvariantCulture) };
    }

    private static ReadOnlySpan<byte> GetSpan(MemoryStream ms) =>
        ms.TryGetBuffer(out var segment) ? segment.AsSpan() : ms.ToArray();

    private static int CompareValues(AttributeValue? left, AttributeValue? right) =>
        (left, right) switch
        {
            (null, _) or (_, null) => throw new ArgumentException("Cannot compare null attribute values"),
            ({ S: not null }, { S: not null }) => string.Compare(left.S, right.S, StringComparison.Ordinal),
            ({ N: not null }, { N: not null }) => decimal.Parse(left.N, CultureInfo.InvariantCulture)
                .CompareTo(decimal.Parse(right.N, CultureInfo.InvariantCulture)),
            ({ B: not null }, { B: not null }) => GetSpan(left.B).SequenceCompareTo(GetSpan(right.B)),
            _ => throw new ArgumentException("Cannot compare values of different or unsupported types")
        };

    private static bool ValuesEqual(AttributeValue? left, AttributeValue? right) =>
        (left, right) switch
        {
            (null, null) => true,
            (null, _) or (_, null) => false,
            _ => ScalarEqual(left, right)
        };

    private static bool ScalarEqual(AttributeValue left, AttributeValue right) =>
        (left, right) switch
        {
            ({ S: not null }, { S: not null }) => left.S == right.S,
            ({ N: not null }, { N: not null }) => decimal.Parse(left.N, CultureInfo.InvariantCulture)
                == decimal.Parse(right.N, CultureInfo.InvariantCulture),
            ({ B: not null }, { B: not null }) => GetSpan(left.B).SequenceEqual(GetSpan(right.B)),
            ({ BOOL: not null }, { BOOL: not null }) => left.BOOL == right.BOOL,
            ({ NULL: true }, { NULL: true }) => true,
            _ => false
        };

    private static bool EvaluateComparison(
        ComparisonNode node,
        Dictionary<string, AttributeValue>? item,
        Dictionary<string, string>? expressionAttributeNames,
        Dictionary<string, AttributeValue>? expressionAttributeValues)
    {
        var left = ResolveOperand(node.Left, item, expressionAttributeNames, expressionAttributeValues);
        var right = ResolveOperand(node.Right, item, expressionAttributeNames, expressionAttributeValues);

        if (node.Operator is "=" or "<>")
        {
            var eq = ValuesEqual(left, right);
            return node.Operator == "=" ? eq : !eq;
        }

        if (left is null || right is null)
            return false;

        var cmp = CompareValues(left, right);
        return node.Operator switch
        {
            "<" => cmp < 0,
            "<=" => cmp <= 0,
            ">" => cmp > 0,
            ">=" => cmp >= 0,
            _ => throw new ArgumentException($"Unknown comparison operator: {node.Operator}")
        };
    }

    private static bool EvaluateBetween(
        BetweenNode node,
        Dictionary<string, AttributeValue>? item,
        Dictionary<string, string>? expressionAttributeNames,
        Dictionary<string, AttributeValue>? expressionAttributeValues)
    {
        var value = ResolveOperand(node.Value, item, expressionAttributeNames, expressionAttributeValues);
        var lower = ResolveOperand(node.Lower, item, expressionAttributeNames, expressionAttributeValues);
        var upper = ResolveOperand(node.Upper, item, expressionAttributeNames, expressionAttributeValues);

        return value is not null
            && lower is not null
            && upper is not null
            && CompareValues(value, lower) >= 0
            && CompareValues(value, upper) <= 0;
    }

    private static bool EvaluateIn(
        InNode node,
        Dictionary<string, AttributeValue>? item,
        Dictionary<string, string>? expressionAttributeNames,
        Dictionary<string, AttributeValue>? expressionAttributeValues)
    {
        var value = ResolveOperand(node.Value, item, expressionAttributeNames, expressionAttributeValues);
        return node.List.Any(listItem =>
            ValuesEqual(value, ResolveOperand(listItem, item, expressionAttributeNames, expressionAttributeValues)));
    }

    private static bool EvaluateLogical(
        LogicalNode node,
        Dictionary<string, AttributeValue>? item,
        Dictionary<string, string>? expressionAttributeNames,
        Dictionary<string, AttributeValue>? expressionAttributeValues) =>
        node.Operator switch
        {
            "AND" => Evaluate(node.Left, item, expressionAttributeNames, expressionAttributeValues)
                  && Evaluate(node.Right, item, expressionAttributeNames, expressionAttributeValues),
            "OR" => Evaluate(node.Left, item, expressionAttributeNames, expressionAttributeValues)
                 || Evaluate(node.Right, item, expressionAttributeNames, expressionAttributeValues),
            _ => throw new ArgumentException($"Unknown logical operator: {node.Operator}")
        };

    private static bool EvaluateFunction(
        FunctionConditionNode node,
        Dictionary<string, AttributeValue>? item,
        Dictionary<string, string>? expressionAttributeNames,
        Dictionary<string, AttributeValue>? expressionAttributeValues) =>
        node.FunctionName switch
        {
            "attribute_exists" => ResolveOperand(node.Arguments[0], item, expressionAttributeNames, expressionAttributeValues) is not null,
            "attribute_not_exists" => ResolveOperand(node.Arguments[0], item, expressionAttributeNames, expressionAttributeValues) is null,
            "attribute_type" => EvaluateAttributeType(node, item, expressionAttributeNames, expressionAttributeValues),
            "begins_with" => EvaluateBeginsWith(node, item, expressionAttributeNames, expressionAttributeValues),
            "contains" => EvaluateContains(node, item, expressionAttributeNames, expressionAttributeValues),
            _ => throw new ArgumentException($"Unknown function: {node.FunctionName}")
        };

    private static bool EvaluateAttributeType(
        FunctionConditionNode node,
        Dictionary<string, AttributeValue>? item,
        Dictionary<string, string>? expressionAttributeNames,
        Dictionary<string, AttributeValue>? expressionAttributeValues)
    {
        var value = ResolveOperand(node.Arguments[0], item, expressionAttributeNames, expressionAttributeValues);
        var typeVal = ResolveOperand(node.Arguments[1], item, expressionAttributeNames, expressionAttributeValues);

        return value is not null && typeVal?.S is not null && ExpressionHelper.GetAttributeType(value) == typeVal.S;
    }

    private static bool EvaluateBeginsWith(
        FunctionConditionNode node,
        Dictionary<string, AttributeValue>? item,
        Dictionary<string, string>? expressionAttributeNames,
        Dictionary<string, AttributeValue>? expressionAttributeValues)
    {
        var value = ResolveOperand(node.Arguments[0], item, expressionAttributeNames, expressionAttributeValues);
        var prefix = ResolveOperand(node.Arguments[1], item, expressionAttributeNames, expressionAttributeValues);

        return value?.S is not null && prefix?.S is not null && value.S.StartsWith(prefix.S, StringComparison.Ordinal);
    }

    private static bool EvaluateContains(
        FunctionConditionNode node,
        Dictionary<string, AttributeValue>? item,
        Dictionary<string, string>? expressionAttributeNames,
        Dictionary<string, AttributeValue>? expressionAttributeValues)
    {
        var value = ResolveOperand(node.Arguments[0], item, expressionAttributeNames, expressionAttributeValues);
        var operand = ResolveOperand(node.Arguments[1], item, expressionAttributeNames, expressionAttributeValues);

        if (value is null || operand is null)
            return false;

        // String contains
        if (value.S is not null && operand.S is not null)
            return value.S.Contains(operand.S, StringComparison.Ordinal);

        // Set contains
        if (value.SS is not null && operand.S is not null)
            return value.SS.Contains(operand.S);
        if (value.NS is not null && operand.N is not null)
            return value.NS.Contains(operand.N);
        if (value.BS is not null && operand.B is not null)
        {
            var operandBytes = operand.B.ToArray();
            return value.BS.Any(b => GetSpan(b).SequenceEqual(operandBytes));
        }

        // List contains
        return value.L is not null && value.L.Any(item2 => ValuesEqual(item2, operand));
    }
}
