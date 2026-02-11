using Amazon.DynamoDBv2.Model;
using System.Globalization;

namespace DynamoDbLite;

internal static class TtlEpochParser
{
    internal static double? ParseTtlEpoch(
        Dictionary<string, AttributeValue> item,
        string ttlAttributeName) =>
        !item.TryGetValue(ttlAttributeName, out var attr)
            ? null
            : attr.N is null
                ? null
                : double.TryParse(attr.N, NumberStyles.Any, CultureInfo.InvariantCulture, out var epoch)
                    ? epoch
                    : null;
}
