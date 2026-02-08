using Amazon.DynamoDBv2.Model;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text.Json;

namespace DynamoDbLite;

internal static class AttributeValueSerializer
{
    internal static string Serialize(Dictionary<string, AttributeValue> item)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        using var writer = new Utf8JsonWriter(buffer);
        WriteMap(writer, item);
        writer.Flush();
        return System.Text.Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP004:Don't ignore created IDisposable", Justification = "ArrayEnumerator is a struct; foreach disposes it")]
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
            writer.WriteBase64StringValue(value.B.ToArray());
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
                writer.WriteBase64StringValue(b.ToArray());
            writer.WriteEndArray();
        }
        else if (value.L is { Count: > 0 })
        {
            writer.WriteStartArray("L");
            foreach (var item in value.L)
            {
                WriteAttributeValue(writer, item);
            }
            writer.WriteEndArray();
        }
        else if (value.M is { Count: > 0 })
        {
            writer.WritePropertyName("M");
            WriteMap(writer, value.M);
        }

        writer.WriteEndObject();
    }

    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP004:Don't ignore created IDisposable", Justification = "ArrayEnumerator is a struct; foreach disposes it")]
    private static Dictionary<string, AttributeValue> ReadMap(JsonElement element)
    {
        var result = new Dictionary<string, AttributeValue>();
        foreach (var prop in element.EnumerateObject())
            result[prop.Name] = ReadAttributeValue(prop.Value);
        return result;
    }

    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP004:Don't ignore created IDisposable", Justification = "ArrayEnumerator is a struct; foreach disposes it")]
    private static AttributeValue ReadAttributeValue(JsonElement element)
    {
        var av = new AttributeValue();

        foreach (var prop in element.EnumerateObject())
        {
            switch (prop.Name)
            {
                case "S":
                    av.S = prop.Value.GetString();
                    break;
                case "N":
                    av.N = prop.Value.GetString();
                    break;
                case "B":
                    av.B = new MemoryStream(prop.Value.GetBytesFromBase64());
                    break;
                case "BOOL":
                    av.BOOL = prop.Value.GetBoolean();
                    break;
                case "NULL":
                    av.NULL = prop.Value.GetBoolean();
                    break;
                case "SS":
                    av.SS = ReadStringList(prop.Value);
                    break;
                case "NS":
                    av.NS = ReadStringList(prop.Value);
                    break;
                case "BS":
                    av.BS = ReadBinaryList(prop.Value);
                    break;
                case "L":
                    av.L = ReadAttributeValueList(prop.Value);
                    break;
                case "M":
                    av.M = ReadMap(prop.Value);
                    break;
            }
        }

        return av;
    }

    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP004:Don't ignore created IDisposable", Justification = "ArrayEnumerator is a struct; foreach disposes it")]
    private static List<string> ReadStringList(JsonElement element)
    {
        var list = new List<string>();
        foreach (var item in element.EnumerateArray())
            list.Add(item.GetString()!);
        return list;
    }

    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP004:Don't ignore created IDisposable", Justification = "ArrayEnumerator is a struct; foreach disposes it")]
    private static List<MemoryStream> ReadBinaryList(JsonElement element)
    {
        var list = new List<MemoryStream>();
        foreach (var item in element.EnumerateArray())
            list.Add(new MemoryStream(item.GetBytesFromBase64()));
        return list;
    }

    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP004:Don't ignore created IDisposable", Justification = "ArrayEnumerator is a struct; foreach disposes it")]
    private static List<AttributeValue> ReadAttributeValueList(JsonElement element)
    {
        var list = new List<AttributeValue>();
        foreach (var item in element.EnumerateArray())
            list.Add(ReadAttributeValue(item));
        return list;
    }
}
