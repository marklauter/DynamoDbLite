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
        var elements = path.Elements;
        var first = (AttributeNameElement)elements[0];

        if (!source.TryGetValue(first.Name, out var srcVal))
            return;

        // Container already in result from a prior path — recurse into it directly
        if (result.TryGetValue(first.Name, out var rstVal))
        {
            _ = CopyPathFrom(srcVal, rstVal, elements, 1);
            return;
        }

        // New container — create detached, only attach if the deeper path resolves
        rstVal = CreateMatchingContainer(srcVal);
        if (rstVal is not null && CopyPathFrom(srcVal, rstVal, elements, 1))
            result[first.Name] = rstVal;
    }

    private static bool CopyPathFrom(
        AttributeValue src,
        AttributeValue rst,
        IReadOnlyList<PathElement> elements,
        int index)
    {
        var isLast = index == elements.Count - 1;

        switch (elements[index])
        {
            case AttributeNameElement nameEl:
            {
                if (src.M is null || !src.M.TryGetValue(nameEl.Name, out var srcChild))
                    return false;

                if (isLast)
                {
                    rst.M![nameEl.Name] = srcChild;
                    return true;
                }

                // Existing child in result — recurse into it (additive)
                if (rst.M!.TryGetValue(nameEl.Name, out var rstChild))
                    return CopyPathFrom(srcChild, rstChild, elements, index + 1);

                // New child — create detached, attach only on success
                rstChild = CreateMatchingContainer(srcChild);
                if (rstChild is null)
                    return false;

                if (!CopyPathFrom(srcChild, rstChild, elements, index + 1))
                    return false;

                rst.M[nameEl.Name] = rstChild;
                return true;
            }

            case ListIndexElement indexEl:
            {
                if (src.L is null || indexEl.Index >= src.L.Count)
                    return false;

                var srcChild = src.L[indexEl.Index];

                if (isLast)
                {
                    NullPadList(rst.L!, indexEl.Index);
                    rst.L![indexEl.Index] = srcChild;
                    return true;
                }

                // Existing populated slot — recurse into it
                if (indexEl.Index < rst.L!.Count && rst.L[indexEl.Index].NULL is not true)
                    return CopyPathFrom(srcChild, rst.L[indexEl.Index], elements, index + 1);

                // New slot — create detached, pad + attach only on success
                var rstChild = CreateMatchingContainer(srcChild);
                if (rstChild is null)
                    return false;

                if (!CopyPathFrom(srcChild, rstChild, elements, index + 1))
                    return false;

                NullPadList(rst.L, indexEl.Index);
                rst.L[indexEl.Index] = rstChild;
                return true;
            }

            default:
                return false;
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
