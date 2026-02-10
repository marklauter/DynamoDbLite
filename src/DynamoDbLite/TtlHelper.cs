using Amazon.DynamoDBv2.Model;
using System.Globalization;

namespace DynamoDbLite;

internal static class TtlHelper
{
    internal static double? ExtractTtlEpoch(
        Dictionary<string, AttributeValue> item,
        string ttlAttributeName)
    {
        if (!item.TryGetValue(ttlAttributeName, out var attr))
            return null;

        if (attr.N is null)
            return null;

        return double.TryParse(attr.N, NumberStyles.Any, CultureInfo.InvariantCulture, out var epoch)
            ? epoch
            : null;
    }
}
