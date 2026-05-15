using Amazon.DynamoDBv2.DataModel;

namespace DynamoDbLite.Tests.Models;

[DynamoDBTable("NumericKeyItems")]
internal class NumericKeyItem
{
    [DynamoDBHashKey]
    public string Category { get; set; } = "";

    [DynamoDBRangeKey]
    public int OrderNumber { get; set; }

    public string Description { get; set; } = "";
}
