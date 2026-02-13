using Amazon.DynamoDBv2.Model;
using System.Globalization;

namespace DynamoDbLite.Expressions;

internal static class UpdateExpressionEvaluator
{
    internal static (Dictionary<string, AttributeValue> Item, HashSet<string> ModifiedKeys) Apply(
        UpdateExpression expression,
        Dictionary<string, AttributeValue> item,
        Dictionary<string, string>? expressionAttributeNames,
        Dictionary<string, AttributeValue>? expressionAttributeValues)
    {
        var modifiedKeys = new HashSet<string>();

        foreach (var set in expression.Sets)
        {
            var value = ResolveUpdateValue(set.Value, item, expressionAttributeNames, expressionAttributeValues);
            ExpressionHelper.SetAtPath(item, set.Path, value, expressionAttributeNames);
            _ = modifiedKeys.Add(GetTopLevelKey(set.Path, expressionAttributeNames));
        }

        foreach (var remove in expression.Removes)
        {
            ExpressionHelper.RemoveAtPath(item, remove.Path, expressionAttributeNames);
            _ = modifiedKeys.Add(GetTopLevelKey(remove.Path, expressionAttributeNames));
        }

        foreach (var add in expression.Adds)
        {
            var addValue = ResolveValueRef(add.ValueRef, expressionAttributeValues);
            ApplyAdd(item, add.Path, addValue, expressionAttributeNames);
            _ = modifiedKeys.Add(GetTopLevelKey(add.Path, expressionAttributeNames));
        }

        foreach (var delete in expression.Deletes)
        {
            var deleteValue = ResolveValueRef(delete.ValueRef, expressionAttributeValues);
            ApplyDelete(item, delete.Path, deleteValue, expressionAttributeNames);
            _ = modifiedKeys.Add(GetTopLevelKey(delete.Path, expressionAttributeNames));
        }

        return (item, modifiedKeys);
    }

    private static ReadOnlySpan<byte> GetSpan(MemoryStream ms) =>
        ms.TryGetBuffer(out var segment) ? segment.AsSpan() : ms.ToArray();

    private static bool BinarySetContains(List<MemoryStream> set, MemoryStream value)
    {
        var valueSpan = GetSpan(value);
        foreach (var item in set)
        {
            if (GetSpan(item).SequenceEqual(valueSpan))
                return true;
        }

        return false;
    }

    private static void BinarySetRemoveAll(List<MemoryStream> set, MemoryStream value)
    {
        var valueSpan = GetSpan(value);
        for (var i = set.Count - 1; i >= 0; i--)
        {
            if (GetSpan(set[i]).SequenceEqual(valueSpan))
                set.RemoveAt(i);
        }
    }

    private static string GetTopLevelKey(AttributePath path, Dictionary<string, string>? expressionAttributeNames)
    {
        var first = path.Elements[0];
        return first is AttributeNameElement nameEl
            ? ExpressionHelper.ResolveAttributeName(nameEl.Name, expressionAttributeNames)
            : throw new ArgumentException("Expected attribute name as first path element");
    }

    private static AttributeValue ResolveValueRef(
        string valueRef,
        Dictionary<string, AttributeValue>? expressionAttributeValues) =>
        expressionAttributeValues?.TryGetValue(valueRef, out var v) is true
            ? v
            : throw new ArgumentException($"Expression attribute value {valueRef} is not defined");

    private static AttributeValue ResolveUpdateValue(
        UpdateValue updateValue,
        Dictionary<string, AttributeValue> item,
        Dictionary<string, string>? expressionAttributeNames,
        Dictionary<string, AttributeValue>? expressionAttributeValues) =>
        updateValue switch
        {
            PathUpdateValue pathVal => ExpressionHelper.ResolvePath(item, pathVal.Path, expressionAttributeNames)
                ?? throw new ArgumentException("Path not found in item"),
            ValueRefUpdateValue valueRef => ResolveValueRef(valueRef.ValueRef, expressionAttributeValues),
            ArithmeticUpdateValue arith => EvaluateArithmetic(arith, item, expressionAttributeNames, expressionAttributeValues),
            IfNotExistsUpdateValue ifne => EvaluateIfNotExists(ifne, item, expressionAttributeNames, expressionAttributeValues),
            ListAppendUpdateValue la => EvaluateListAppend(la, item, expressionAttributeNames, expressionAttributeValues),
            _ => throw new ArgumentException($"Unknown update value type: {updateValue.GetType().Name}")
        };

    private static AttributeValue EvaluateArithmetic(
        ArithmeticUpdateValue arith,
        Dictionary<string, AttributeValue> item,
        Dictionary<string, string>? expressionAttributeNames,
        Dictionary<string, AttributeValue>? expressionAttributeValues)
    {
        var left = ResolveUpdateValue(arith.Left, item, expressionAttributeNames, expressionAttributeValues);
        var right = ResolveUpdateValue(arith.Right, item, expressionAttributeNames, expressionAttributeValues);

        if (left.N is null || right.N is null)
            throw new ArgumentException("Arithmetic operations require numeric operands");

        var leftNum = decimal.Parse(left.N, CultureInfo.InvariantCulture);
        var rightNum = decimal.Parse(right.N, CultureInfo.InvariantCulture);

        var result = arith.Operator switch
        {
            "+" => leftNum + rightNum,
            "-" => leftNum - rightNum,
            _ => throw new ArgumentException($"Unknown arithmetic operator: {arith.Operator}")
        };

        return new AttributeValue { N = result.ToString(CultureInfo.InvariantCulture) };
    }

    private static AttributeValue EvaluateIfNotExists(
        IfNotExistsUpdateValue ifne,
        Dictionary<string, AttributeValue> item,
        Dictionary<string, string>? expressionAttributeNames,
        Dictionary<string, AttributeValue>? expressionAttributeValues)
    {
        var existing = ExpressionHelper.ResolvePath(item, ifne.Path, expressionAttributeNames);
        return existing ?? ResolveUpdateValue(ifne.Default, item, expressionAttributeNames, expressionAttributeValues);
    }

    private static AttributeValue EvaluateListAppend(
        ListAppendUpdateValue la,
        Dictionary<string, AttributeValue> item,
        Dictionary<string, string>? expressionAttributeNames,
        Dictionary<string, AttributeValue>? expressionAttributeValues)
    {
        var first = ResolveUpdateValue(la.First, item, expressionAttributeNames, expressionAttributeValues);
        var second = ResolveUpdateValue(la.Second, item, expressionAttributeNames, expressionAttributeValues);

        var firstList = first.L ?? throw new ArgumentException(
            "Invalid UpdateExpression: Incorrect operand type for operator or function; operator or function: list_append, operand type: " + ExpressionHelper.GetAttributeType(first));
        var secondList = second.L ?? throw new ArgumentException(
            "Invalid UpdateExpression: Incorrect operand type for operator or function; operator or function: list_append, operand type: " + ExpressionHelper.GetAttributeType(second));

        return new AttributeValue { L = [.. firstList, .. secondList] };
    }

    private static void ApplyAdd(
        Dictionary<string, AttributeValue> item,
        AttributePath path,
        AttributeValue addValue,
        Dictionary<string, string>? expressionAttributeNames)
    {
        var existing = ExpressionHelper.ResolvePath(item, path, expressionAttributeNames);

        if (existing is null)
        {
            // Path doesn't exist — set the value
            ExpressionHelper.SetAtPath(item, path, addValue, expressionAttributeNames);
            return;
        }

        if (existing.N is not null && addValue.N is not null)
        {
            // Numeric add
            var result = decimal.Parse(existing.N, CultureInfo.InvariantCulture)
                       + decimal.Parse(addValue.N, CultureInfo.InvariantCulture);
            existing.N = result.ToString(CultureInfo.InvariantCulture);
        }
        else if (existing.SS is not null && addValue.SS is not null)
        {
            // String set union
            foreach (var s in addValue.SS)
                if (!existing.SS.Contains(s))
                    existing.SS.Add(s);
        }
        else if (existing.NS is not null && addValue.NS is not null)
        {
            // Number set union
            foreach (var n in addValue.NS)
                if (!existing.NS.Contains(n))
                    existing.NS.Add(n);
        }
        else if (existing.BS is not null && addValue.BS is not null)
        {
            // Binary set union
            foreach (var b in addValue.BS)
            {
                if (!BinarySetContains(existing.BS, b))
                    existing.BS.Add(b);
            }
        }
    }

    private static void ApplyDelete(
        Dictionary<string, AttributeValue> item,
        AttributePath path,
        AttributeValue deleteValue,
        Dictionary<string, string>? expressionAttributeNames)
    {
        var existing = ExpressionHelper.ResolvePath(item, path, expressionAttributeNames);
        if (existing is null)
            return;

        if (existing.SS is not null && deleteValue.SS is not null)
        {
            foreach (var s in deleteValue.SS)
                _ = existing.SS.Remove(s);
        }
        else if (existing.NS is not null && deleteValue.NS is not null)
        {
            foreach (var n in deleteValue.NS)
                _ = existing.NS.Remove(n);
        }
        else if (existing.BS is not null && deleteValue.BS is not null)
        {
            foreach (var b in deleteValue.BS)
                BinarySetRemoveAll(existing.BS, b);
        }
    }
}
