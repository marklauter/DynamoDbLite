using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.SqliteStores.Models;

namespace DynamoDbLite.Expressions;

internal static class KeyHelper
{
    private static ReadOnlySpan<byte> GetSpan(MemoryStream ms) =>
        ms.TryGetBuffer(out var segment) ? segment.AsSpan() : ms.ToArray(); // defensive: AWS SDK MemoryStreams always expose buffer

    internal static (string Pk, string Sk) ExtractKeys(
        Dictionary<string, AttributeValue> item,
        List<KeySchemaElement> keySchema,
        List<AttributeDefinition> attributeDefinitions)
    {
        var hashKey = keySchema.First(static k => k.KeyType == KeyType.HASH);
        var rangeKey = keySchema.FirstOrDefault(static k => k.KeyType == KeyType.RANGE);

        var pk = ExtractKeyValue(item, hashKey.AttributeName, attributeDefinitions);
        var sk = rangeKey is not null
            ? ExtractKeyValue(item, rangeKey.AttributeName, attributeDefinitions)
            : string.Empty;

        return (pk, sk);
    }

    internal static void ValidateKeyTypes(
        Dictionary<string, AttributeValue> item,
        List<KeySchemaElement> keySchema,
        List<AttributeDefinition> attributeDefinitions)
    {
        foreach (var key in keySchema)
        {
            if (!item.TryGetValue(key.AttributeName, out var value))
                throw new AmazonDynamoDBException(
                    $"One or more parameter values were invalid: Missing the key {key.AttributeName} in the item");

            var expectedType = attributeDefinitions
                .First(a => a.AttributeName == key.AttributeName)
                .AttributeType;

            var actualType = GetScalarType(value);
            if (actualType != expectedType.Value)
                throw new AmazonDynamoDBException(
                    $"One or more parameter values were invalid: Type mismatch for key {key.AttributeName}");
        }
    }

    private static string GetScalarType(AttributeValue value) =>
        value switch
        {
            { S: not null } => ScalarAttributeType.S.Value,
            { N: not null } => ScalarAttributeType.N.Value,
            { B: not null } => ScalarAttributeType.B.Value,
            _ => throw new AmazonDynamoDBException(
                "One or more parameter values were invalid: Key attributes must be scalars (S, N, or B)")
        };

    internal static (string Pk, string Sk)? TryExtractIndexKeys(
        Dictionary<string, AttributeValue> item,
        List<KeySchemaElement> keySchema,
        List<AttributeDefinition> attributeDefinitions)
    {
        var hashKey = keySchema.First(static k => k.KeyType == KeyType.HASH);
        if (!item.TryGetValue(hashKey.AttributeName, out _))
            return null;

        var rangeKey = keySchema.FirstOrDefault(static k => k.KeyType == KeyType.RANGE);
        if (rangeKey is not null && !item.TryGetValue(rangeKey.AttributeName, out _))
            return null;

        var pk = ExtractKeyValue(item, hashKey.AttributeName, attributeDefinitions);
        var sk = rangeKey is not null
            ? ExtractKeyValue(item, rangeKey.AttributeName, attributeDefinitions)
            : string.Empty;

        return (pk, sk);
    }

    private static string ExtractKeyValue(
        Dictionary<string, AttributeValue> item,
        string attributeName,
        List<AttributeDefinition> attributeDefinitions)
    {
        var value = item[attributeName];
        var attrDef = attributeDefinitions.First(a => a.AttributeName == attributeName);

        return attrDef.AttributeType.Value switch
        {
            "S" => value.S,
            "N" => value.N,
            "B" => Convert.ToBase64String(GetSpan(value.B)),
            _ => throw new AmazonDynamoDBException($"Unsupported key type: {attrDef.AttributeType.Value}")
        };
    }

    internal static AttributeValue BuildKeyAttributeValue(string value, ScalarAttributeType type) =>
        type.Value switch
        {
            "S" => new AttributeValue { S = value },
            "N" => new AttributeValue { N = value },
            "B" => new AttributeValue { B = new MemoryStream(Convert.FromBase64String(value)) },
            _ => throw new ArgumentException($"Unsupported key attribute type: {type.Value}")
        };

    internal static Dictionary<string, AttributeValue> BuildLastEvaluatedKey(string pk, string sk, KeySchemaInfo keyInfo)
    {
        var hashKey = keyInfo.KeySchema.First(static k => k.KeyType == KeyType.HASH);
        var rangeKey = keyInfo.KeySchema.FirstOrDefault(static k => k.KeyType == KeyType.RANGE);

        var result = new Dictionary<string, AttributeValue>
        {
            [hashKey.AttributeName] = BuildKeyAttributeValue(pk, keyInfo.AttributeDefinitions.First(a => a.AttributeName == hashKey.AttributeName).AttributeType)
        };

        if (rangeKey is not null)
            result[rangeKey.AttributeName] = BuildKeyAttributeValue(sk, keyInfo.AttributeDefinitions.First(a => a.AttributeName == rangeKey.AttributeName).AttributeType);

        return result;
    }

    internal static Dictionary<string, AttributeValue> BuildIndexLastEvaluatedKey(
        IndexItemRow lastRow,
        KeySchemaInfo indexKeyInfo,
        KeySchemaInfo tableKeyInfo)
    {
        var result = new Dictionary<string, AttributeValue>();

        // Add index keys
        var indexHashKey = indexKeyInfo.KeySchema.First(static k => k.KeyType == KeyType.HASH);
        result[indexHashKey.AttributeName] = BuildKeyAttributeValue(
            lastRow.Pk,
            indexKeyInfo.AttributeDefinitions.First(a => a.AttributeName == indexHashKey.AttributeName).AttributeType);

        var indexRangeKey = indexKeyInfo.KeySchema.FirstOrDefault(static k => k.KeyType == KeyType.RANGE);
        if (indexRangeKey is not null)
            result[indexRangeKey.AttributeName] = BuildKeyAttributeValue(
                lastRow.Sk,
                indexKeyInfo.AttributeDefinitions.First(a => a.AttributeName == indexRangeKey.AttributeName).AttributeType);

        // Add table keys
        var tableHashKey = tableKeyInfo.KeySchema.First(static k => k.KeyType == KeyType.HASH);
        if (!result.ContainsKey(tableHashKey.AttributeName))
            result[tableHashKey.AttributeName] = BuildKeyAttributeValue(
                lastRow.TablePk,
                tableKeyInfo.AttributeDefinitions.First(a => a.AttributeName == tableHashKey.AttributeName).AttributeType);

        var tableRangeKey = tableKeyInfo.KeySchema.FirstOrDefault(static k => k.KeyType == KeyType.RANGE);
        if (tableRangeKey is not null && !result.ContainsKey(tableRangeKey.AttributeName))
            result[tableRangeKey.AttributeName] = BuildKeyAttributeValue(
                lastRow.TableSk,
                tableKeyInfo.AttributeDefinitions.First(a => a.AttributeName == tableRangeKey.AttributeName).AttributeType);

        return result;
    }
}
