using Amazon.DynamoDBv2.DataModel;

namespace DynamoDbLite.Tests.Models;

[DynamoDBTable("CompositeItems")]
internal class CompositeKeyItem
{
    [DynamoDBHashKey]
    public string PK { get; set; } = "";

    [DynamoDBRangeKey]
    public string SK { get; set; } = "";

    public DateTime CreatedAt { get; set; }

    public int? OptionalValue { get; set; }

    [DynamoDBProperty("custom_name")]
    public string CustomNamedProp { get; set; } = "";

    [DynamoDBIgnore]
    public string Ignored { get; set; } = "should-not-persist";
}
