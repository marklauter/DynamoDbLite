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
                    currentMap = current.M is { Count: > 0 } ? current.M : null;
                    currentList = current.L is { Count: > 0 } ? current.L : null;
                    break;

                case ListIndexElement indexEl:
                    if (currentList is null || indexEl.Index >= currentList.Count)
                        return null;
                    current = currentList[indexEl.Index];
                    currentMap = current.M is { Count: > 0 } ? current.M : null;
                    currentList = current.L is { Count: > 0 } ? current.L : null;
                    break;
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
                    var list = current!.L;
                    while (list.Count <= indexEl.Index)
                        list.Add(new AttributeValue { NULL = true });
                    current = list[indexEl.Index];
                    currentMap = current.M is { Count: > 0 } ? current.M : null;
                    break;
            }
        }

        switch (elements[^1])
        {
            case AttributeNameElement nameEl:
                var finalName = ResolveAttributeName(nameEl.Name, expressionAttributeNames);
                currentMap![finalName] = value;
                break;

            case ListIndexElement indexEl:
                var finalList = current!.L;
                while (finalList.Count <= indexEl.Index)
                    finalList.Add(new AttributeValue { NULL = true });
                finalList[indexEl.Index] = value;
                break;
        }
    }

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
                    currentMap = current.M is { Count: > 0 } ? current.M : null;
                    break;

                case ListIndexElement indexEl:
                    if (current?.L is null || indexEl.Index >= current.L.Count)
                        return;
                    current = current.L[indexEl.Index];
                    currentMap = current.M is { Count: > 0 } ? current.M : null;
                    break;
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
        }
    }
}
