using Amazon.DynamoDBv2.Model;
using System.Globalization;

namespace DynamoDbLite;

internal static class TtlEpochParser
{
    internal static bool TryParse(
        Dictionary<string, AttributeValue> item,
        string ttlAttributeName,
        out double epoch)
    {
        epoch = default;
        return item.TryGetValue(ttlAttributeName, out var attr)
            && attr.N is not null
            && double.TryParse(attr.N, NumberStyles.Any, CultureInfo.InvariantCulture, out epoch);
    }
}
