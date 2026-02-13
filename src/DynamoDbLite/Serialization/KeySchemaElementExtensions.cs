using Amazon.DynamoDBv2.Model;
using System.Runtime.CompilerServices;

namespace DynamoDbLite.Serialization;

internal static class KeySchemaElementExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static KeySchemaWire[] ToKeySchemas(this List<KeySchemaElement> elements)
    {
        var schemas = new KeySchemaWire[elements.Count];
        for (var i = 0; i < schemas.Length; i++)
            schemas[i] = new KeySchemaWire(elements[i].AttributeName, elements[i].KeyType.Value);
        return schemas;
    }
}
