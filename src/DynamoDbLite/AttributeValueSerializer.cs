using Amazon.DynamoDBv2.Model;
using System.Buffers;
using System.Text.Json;

namespace DynamoDbLite;

internal static class AttributeValueSerializer
{
    private static MemoryStream ReadableStream(byte[] bytes) =>
        new(bytes, 0, bytes.Length, writable: false, publiclyVisible: true);

    private static ReadOnlySpan<byte> GetSpan(MemoryStream ms) =>
        ms.TryGetBuffer(out var segment) ? segment.AsSpan() : ms.ToArray();

    internal static string Serialize(Dictionary<string, AttributeValue> item)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        using var writer = new Utf8JsonWriter(buffer);
        WriteMap(writer, item);
        writer.Flush();
        return System.Text.Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    internal static Dictionary<string, AttributeValue> Deserialize(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return ReadMap(doc.RootElement);
    }

    private static void WriteMap(Utf8JsonWriter writer, Dictionary<string, AttributeValue> map)
    {
        writer.WriteStartObject();
        foreach (var (name, value) in map)
        {
            writer.WritePropertyName(name);
            WriteAttributeValue(writer, value);
        }

        writer.WriteEndObject();
    }

    private static void WriteAttributeValue(Utf8JsonWriter writer, AttributeValue value)
    {
        writer.WriteStartObject();

        if (value.S is not null)
        {
            writer.WriteString("S", value.S);
        }
        else if (value.N is not null)
        {
            writer.WriteString("N", value.N);
        }
        else if (value.B is not null)
        {
            writer.WritePropertyName("B");
            writer.WriteBase64StringValue(GetSpan(value.B));
        }
        else if (value.BOOL is not null)
        {
            writer.WriteBoolean("BOOL", value.BOOL.Value);
        }
        else if (value.NULL is true)
        {
            writer.WriteBoolean("NULL", true);
        }
        else if (value.SS is { Count: > 0 })
        {
            writer.WriteStartArray("SS");
            foreach (var s in value.SS)
                writer.WriteStringValue(s);
            writer.WriteEndArray();
        }
        else if (value.NS is { Count: > 0 })
        {
            writer.WriteStartArray("NS");
            foreach (var n in value.NS)
                writer.WriteStringValue(n);
            writer.WriteEndArray();
        }
        else if (value.BS is { Count: > 0 })
        {
            writer.WriteStartArray("BS");
            foreach (var b in value.BS)
                writer.WriteBase64StringValue(GetSpan(b));
            writer.WriteEndArray();
        }
        else if (value.L is { Count: > 0 } || IsAlwaysSend(value.L))
        {
            writer.WriteStartArray("L");
            foreach (var item in value.L)
            {
                WriteAttributeValue(writer, item);
            }

            writer.WriteEndArray();
        }
        else if (value.M is { Count: > 0 } || IsAlwaysSend(value.M))
        {
            writer.WritePropertyName("M");
            WriteMap(writer, value.M);
        }
        else
        {
            // Fallback for empty collections or unset attribute values
            writer.WriteBoolean("NULL", true);
        }

        writer.WriteEndObject();
    }

    private static Dictionary<string, AttributeValue> ReadMap(JsonElement element)
    {
        using var obj = element.EnumerateObject();
        return obj.ToDictionary(p => p.Name, p => ReadAttributeValue(p.Value));
    }

    private static AttributeValue ReadAttributeValue(JsonElement element)
    {
        using var obj = element.EnumerateObject();
        var first = obj.Select(ReadProperty).FirstOrDefault();
        return first ?? new AttributeValue { NULL = true };
    }

    private static AttributeValue ReadProperty(JsonProperty prop) =>
        prop.Name switch
        {
            "S" => new AttributeValue { S = prop.Value.GetString() },
            "N" => new AttributeValue { N = prop.Value.GetString() },
            "B" => new AttributeValue { B = ReadableStream(prop.Value.GetBytesFromBase64()) },
            "BOOL" => new AttributeValue { BOOL = prop.Value.GetBoolean() },
            "NULL" => new AttributeValue { NULL = prop.Value.GetBoolean() },
            "SS" => new AttributeValue { SS = ReadStringList(prop.Value) },
            "NS" => new AttributeValue { NS = ReadStringList(prop.Value) },
            "BS" => new AttributeValue { BS = ReadBinaryList(prop.Value) },
            "L" => new AttributeValue { L = ReadAttributeValueList(prop.Value) },
            "M" => new AttributeValue { M = ReadMap(prop.Value) },
            _ => throw new NotSupportedException()
        };

    private static List<string> ReadStringList(JsonElement element)
    {
        var list = new List<string>(element.GetArrayLength());
#pragma warning disable IDISP004 // foreach disposes the ArrayEnumerator
        foreach (var e in element.EnumerateArray())
            list.Add(e.GetString()!);
#pragma warning restore IDISP004
        return list;
    }

    private static List<MemoryStream> ReadBinaryList(JsonElement element)
    {
        var list = new List<MemoryStream>(element.GetArrayLength());
#pragma warning disable IDISP004 // foreach disposes the ArrayEnumerator
        foreach (var e in element.EnumerateArray())
            list.Add(ReadableStream(e.GetBytesFromBase64()));
#pragma warning restore IDISP004
        return list;
    }

    private static List<AttributeValue> ReadAttributeValueList(JsonElement element)
    {
        var list = new List<AttributeValue>(element.GetArrayLength());
#pragma warning disable IDISP004 // foreach disposes the ArrayEnumerator
        foreach (var e in element.EnumerateArray())
            list.Add(ReadAttributeValue(e));
#pragma warning restore IDISP004
        return list;
    }

    /// <summary>
    /// Detects the SDK's AlwaysSendDictionary/AlwaysSendList marker types,
    /// which indicate an intentionally-empty collection (not a default-initialized one).
    /// </summary>
    private static bool IsAlwaysSend<T>(T value) =>
        value is not null && value.GetType() != typeof(T);
}
