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
            if (path.Elements is [AttributeNameElement nameEl])
            {
                if (item.TryGetValue(nameEl.Name, out var value))
                    result[nameEl.Name] = value;
            }
            else
            {
                CopyPath(item, result, path);
            }
        }

        return result;
    }

    private static void CopyPath(
        Dictionary<string, AttributeValue> source,
        Dictionary<string, AttributeValue> result,
        AttributePath path)
    {
        // Pre-check: if path doesn't resolve in source, skip entirely
        // to avoid creating empty containers in the result
        if (ExpressionHelper.ResolvePath(source, path, null) is null)
            return;

        var srcMap = source;
        var rstMap = result;
        List<AttributeValue>? srcList = null;
        List<AttributeValue>? rstList = null;
        AttributeValue? srcVal;
        AttributeValue? rstVal;

        var elements = path.Elements;

        for (var i = 0; i < elements.Count; i++)
        {
            var isLast = i == elements.Count - 1;

            switch (elements[i])
            {
                case AttributeNameElement nameEl:
                {
                    if (srcMap is null || !srcMap.TryGetValue(nameEl.Name, out srcVal))
                        return;

                    if (isLast)
                    {
                        rstMap![nameEl.Name] = srcVal;
                        return;
                    }

                    // Intermediate: create matching container in result
                    if (!rstMap!.TryGetValue(nameEl.Name, out rstVal))
                    {
                        rstVal = CreateMatchingContainer(srcVal);
                        if (rstVal is null)
                            return;
                        rstMap[nameEl.Name] = rstVal;
                    }

                    srcMap = srcVal.M;
                    srcList = srcVal.L;
                    rstMap = rstVal.M;
                    rstList = rstVal.L;
                    break;
                }

                case ListIndexElement indexEl:
                {
                    if (srcList is null || indexEl.Index >= srcList.Count)
                        return;

                    srcVal = srcList[indexEl.Index];

                    if (isLast)
                    {
                        NullPadList(rstList!, indexEl.Index);
                        rstList![indexEl.Index] = srcVal;
                        return;
                    }

                    // Intermediate: null-pad and create matching container
                    NullPadList(rstList!, indexEl.Index);
                    rstVal = rstList![indexEl.Index];

                    if (rstVal.NULL is true)
                    {
                        rstVal = CreateMatchingContainer(srcVal);
                        if (rstVal is null)
                            return;
                        rstList[indexEl.Index] = rstVal;
                    }

                    srcMap = srcVal.M;
                    srcList = srcVal.L;
                    rstMap = rstVal.M;
                    rstList = rstVal.L;
                    break;
                }
            }
        }
    }

    private static AttributeValue? CreateMatchingContainer(AttributeValue srcVal) =>
        srcVal switch
        {
            { L: not null } => new AttributeValue { L = [] },
            { M: not null } => new AttributeValue { M = [] },
            _ => null
        };

    private static void NullPadList(List<AttributeValue> list, int index)
    {
        while (list.Count <= index)
            list.Add(new AttributeValue { NULL = true });
    }
}
