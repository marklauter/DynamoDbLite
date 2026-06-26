using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite.Expressions;

internal static class ExpressionHelper
{
    internal static string ResolveAttributeName(
        string name,
        Dictionary<string, string>? expressionAttributeNames) =>
        name.StartsWith('#')
            ? expressionAttributeNames?.TryGetValue(name, out var resolved) is true
                ? resolved
                : throw new ArgumentException($"Expression attribute name {name} is not defined")
            : name;

    internal static AttributeValue? ResolvePath(
        Dictionary<string, AttributeValue>? item,
        AttributePath path,
        Dictionary<string, string>? expressionAttributeNames)
    {
        if (item is null)
            return null;

        AttributeValue? current = null;
        var currentMap = item;
        List<AttributeValue>? currentList = null;

        foreach (var element in path.Elements)
        {
            switch (element)
            {
                case AttributeNameElement nameEl:
                    var resolvedName = ResolveAttributeName(nameEl.Name, expressionAttributeNames);
                    if (currentMap is null || !currentMap.TryGetValue(resolvedName, out current))
                        return null;
                    currentMap = current.M;
                    currentList = current.L;
                    break;

                case ListIndexElement indexEl:
                    if (currentList is null || indexEl.Index >= currentList.Count)
                        return null;
                    current = currentList[indexEl.Index];
                    currentMap = current.M;
                    currentList = current.L;
                    break;

                default:
                    throw UnhandledPathElement(element);
            }
        }

        return current;
    }

    internal static void SetAtPath(
        Dictionary<string, AttributeValue> item,
        AttributePath path,
        AttributeValue value,
        Dictionary<string, string>? expressionAttributeNames)
    {
        var elements = path.Elements;
        var currentMap = item;
        AttributeValue? current = null;

        for (var i = 0; i < elements.Count - 1; i++)
        {
            switch (elements[i])
            {
                case AttributeNameElement nameEl:
                    var resolvedName = ResolveAttributeName(nameEl.Name, expressionAttributeNames);
                    if (!currentMap!.TryGetValue(resolvedName, out current))
                    {
                        current = new AttributeValue { M = [] };
                        currentMap[resolvedName] = current;
                    }

                    currentMap = current.M;
                    break;

                case ListIndexElement indexEl:
                    var list = RequireList(current);
                    while (list.Count <= indexEl.Index)
                        list.Add(new AttributeValue { NULL = true });
                    current = list[indexEl.Index];
                    currentMap = current.M;
                    break;
                default:
                    throw UnhandledPathElement(elements[i]);
            }
        }

        switch (elements[^1])
        {
            case AttributeNameElement nameEl:
                var finalName = ResolveAttributeName(nameEl.Name, expressionAttributeNames);
                currentMap![finalName] = value;
                break;

            case ListIndexElement indexEl:
                var finalList = RequireList(current);
                while (finalList.Count <= indexEl.Index)
                    finalList.Add(new AttributeValue { NULL = true });
                finalList[indexEl.Index] = value;
                break;
            default:
                throw UnhandledPathElement(elements[^1]);
        }
    }

    internal static string GetAttributeType(AttributeValue value) =>
        value switch
        {
            { S: not null } => "S",
            { N: not null } => "N",
            { B: not null } => "B",
            { SS: not null } => "SS",
            { NS: not null } => "NS",
            { BS: not null } => "BS",
            { L: not null } => "L",
            { M: not null } => "M",
            { BOOL: not null } => "BOOL",
            { NULL: true } => "NULL",
            _ => "UNKNOWN"
        };

    internal static void RemoveAtPath(
        Dictionary<string, AttributeValue> item,
        AttributePath path,
        Dictionary<string, string>? expressionAttributeNames)
    {
        var elements = path.Elements;
        var currentMap = item;
        AttributeValue? current = null;

        for (var i = 0; i < elements.Count - 1; i++)
        {
            switch (elements[i])
            {
                case AttributeNameElement nameEl:
                    var resolvedName = ResolveAttributeName(nameEl.Name, expressionAttributeNames);
                    if (!currentMap!.TryGetValue(resolvedName, out current))
                        return;
                    currentMap = current.M;
                    break;

                case ListIndexElement indexEl:
                    if (current?.L is null || indexEl.Index >= current.L.Count)
                        return;
                    current = current.L[indexEl.Index];
                    currentMap = current.M;
                    break;
                default:
                    throw UnhandledPathElement(elements[i]);
            }
        }

        switch (elements[^1])
        {
            case AttributeNameElement nameEl:
                var finalName = ResolveAttributeName(nameEl.Name, expressionAttributeNames);
                _ = currentMap?.Remove(finalName);
                break;

            case ListIndexElement indexEl:
                if (current?.L is not null && indexEl.Index < current.L.Count)
                    current.L.RemoveAt(indexEl.Index);
                break;
            default:
                throw UnhandledPathElement(elements[^1]);
        }
    }

    // Binary sets compare by content, not by MemoryStream reference, so set ADD/DELETE must
    // compare the underlying bytes. Shared by the UpdateExpression evaluator and the legacy
    // AttributeUpdates path.
    internal static bool BinarySetContains(List<MemoryStream> set, MemoryStream value)
    {
        var valueSpan = GetSpan(value);
        foreach (var item in set)
        {
            if (GetSpan(item).SequenceEqual(valueSpan))
                return true;
        }

        return false;
    }

    internal static void BinarySetRemoveAll(List<MemoryStream> set, MemoryStream value)
    {
        var valueSpan = GetSpan(value);
        for (var i = set.Count - 1; i >= 0; i--)
        {
            if (GetSpan(set[i]).SequenceEqual(valueSpan))
                set.RemoveAt(i);
        }
    }

    private static ReadOnlySpan<byte> GetSpan(MemoryStream ms) =>
        ms.TryGetBuffer(out var segment) ? segment.AsSpan() : ms.ToArray(); // streams created via ReadableStream always expose their buffer

    private static InvalidOperationException UnhandledPathElement(PathElement element) =>
        new($"Unhandled PathElement: {element.GetType().Name}");

    // A list-index element requires the resolved value to be a list. When the path walks into a
    // non-list value (e.g. `a.b[0]` where `a.b` is a map), the SDK's `.L` is null — match DynamoDB's
    // validation rejection instead of throwing NullReferenceException.
    private static List<AttributeValue> RequireList(AttributeValue? current) =>
        current?.L
        ?? throw new AmazonDynamoDBException(
            "The document path provided in the update expression is invalid for update");
}
