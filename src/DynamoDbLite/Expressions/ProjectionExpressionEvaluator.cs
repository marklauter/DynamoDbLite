using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite.Expressions;

internal static class ProjectionExpressionEvaluator
{
    internal static Dictionary<string, AttributeValue> Apply(
        Dictionary<string, AttributeValue> item,
        IReadOnlyList<AttributePath> paths)
    {
        var result = new Dictionary<string, AttributeValue>();

        foreach (var path in paths)
        {
            var value = ExpressionHelper.ResolvePath(item, path, null);
            if (value is null)
                continue;

            if (path.Elements is [AttributeNameElement nameEl])
            {
                result[nameEl.Name] = value;
            }
            else
            {
                SetNestedValue(result, path, value);
            }
        }

        return result;
    }

    private static void SetNestedValue(
        Dictionary<string, AttributeValue> result,
        AttributePath path,
        AttributeValue value)
    {
        var currentMap = result;
        AttributeValue? current = null;

        for (var i = 0; i < path.Elements.Count - 1; i++)
        {
            switch (path.Elements[i])
            {
                case AttributeNameElement nameEl:
                    if (!currentMap.TryGetValue(nameEl.Name, out current))
                    {
                        current = new AttributeValue { M = new Dictionary<string, AttributeValue>() };
                        currentMap[nameEl.Name] = current;
                    }
                    currentMap = current.M!;
                    break;

                case ListIndexElement indexEl:
                    var list = current!.L ??= [];
                    while (list.Count <= indexEl.Index)
                        list.Add(new AttributeValue { NULL = true });
                    current = list[indexEl.Index];
                    if (current.M is null)
                        current.M = new Dictionary<string, AttributeValue>();
                    currentMap = current.M;
                    break;
            }
        }

        switch (path.Elements[^1])
        {
            case AttributeNameElement nameEl:
                currentMap[nameEl.Name] = value;
                break;

            case ListIndexElement indexEl:
                var list = current!.L ??= [];
                while (list.Count <= indexEl.Index)
                    list.Add(new AttributeValue { NULL = true });
                list[indexEl.Index] = value;
                break;
        }
    }
}
