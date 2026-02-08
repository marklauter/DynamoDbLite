using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite.Tests;

public sealed class AttributeValueSerializerTests
{
    [Fact]
    public void RoundTrip_StringAttribute_PreservesValue()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["name"] = new() { S = "Alice" }
        };

        var json = AttributeValueSerializer.Serialize(item);
        var result = AttributeValueSerializer.Deserialize(json);

        Assert.Equal("Alice", result["name"].S);
    }

    [Fact]
    public void RoundTrip_NumberAttribute_PreservesValue()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["age"] = new() { N = "42" }
        };

        var json = AttributeValueSerializer.Serialize(item);
        var result = AttributeValueSerializer.Deserialize(json);

        Assert.Equal("42", result["age"].N);
    }

    [Fact]
    public void RoundTrip_BoolAttribute_PreservesValue()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["active"] = new() { BOOL = true }
        };

        var json = AttributeValueSerializer.Serialize(item);
        var result = AttributeValueSerializer.Deserialize(json);

        Assert.True(result["active"].BOOL);
    }

    [Fact]
    public void RoundTrip_NullAttribute_PreservesValue()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["nothing"] = new() { NULL = true }
        };

        var json = AttributeValueSerializer.Serialize(item);
        var result = AttributeValueSerializer.Deserialize(json);

        Assert.True(result["nothing"].NULL);
    }

    [Fact]
    public void RoundTrip_BinaryAttribute_PreservesValue()
    {
        var bytes = new byte[] { 1, 2, 3, 4 };
        var item = new Dictionary<string, AttributeValue>
        {
            ["data"] = new() { B = new MemoryStream(bytes) }
        };

        var json = AttributeValueSerializer.Serialize(item);
        var result = AttributeValueSerializer.Deserialize(json);

        Assert.Equal(bytes, result["data"].B.ToArray());
    }

    [Fact]
    public void RoundTrip_StringSetAttribute_PreservesValue()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["tags"] = new() { SS = ["a", "b", "c"] }
        };

        var json = AttributeValueSerializer.Serialize(item);
        var result = AttributeValueSerializer.Deserialize(json);

        Assert.Equal(["a", "b", "c"], result["tags"].SS);
    }

    [Fact]
    public void RoundTrip_NumberSetAttribute_PreservesValue()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["scores"] = new() { NS = ["1", "2", "3"] }
        };

        var json = AttributeValueSerializer.Serialize(item);
        var result = AttributeValueSerializer.Deserialize(json);

        Assert.Equal(["1", "2", "3"], result["scores"].NS);
    }

    [Fact]
    public void RoundTrip_ListAttribute_PreservesValue()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["items"] = new()
            {
                L =
                [
                    new() { S = "hello" },
                    new() { N = "42" }
                ]
            }
        };

        var json = AttributeValueSerializer.Serialize(item);
        var result = AttributeValueSerializer.Deserialize(json);

        Assert.Equal(2, result["items"].L.Count);
        Assert.Equal("hello", result["items"].L[0].S);
        Assert.Equal("42", result["items"].L[1].N);
    }

    [Fact]
    public void RoundTrip_MapAttribute_PreservesValue()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["address"] = new()
            {
                M = new Dictionary<string, AttributeValue>
                {
                    ["city"] = new() { S = "Seattle" },
                    ["zip"] = new() { S = "98101" }
                }
            }
        };

        var json = AttributeValueSerializer.Serialize(item);
        var result = AttributeValueSerializer.Deserialize(json);

        Assert.Equal("Seattle", result["address"].M["city"].S);
        Assert.Equal("98101", result["address"].M["zip"].S);
    }

    [Fact]
    public void RoundTrip_MultipleAttributes_PreservesAll()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#123" },
            ["SK"] = new() { S = "PROFILE" },
            ["age"] = new() { N = "30" },
            ["active"] = new() { BOOL = true }
        };

        var json = AttributeValueSerializer.Serialize(item);
        var result = AttributeValueSerializer.Deserialize(json);

        Assert.Equal(4, result.Count);
        Assert.Equal("USER#123", result["PK"].S);
        Assert.Equal("PROFILE", result["SK"].S);
        Assert.Equal("30", result["age"].N);
        Assert.True(result["active"].BOOL);
    }
}
