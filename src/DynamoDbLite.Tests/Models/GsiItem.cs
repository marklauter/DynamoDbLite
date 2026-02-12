using Amazon.DynamoDBv2.DataModel;

namespace DynamoDbLite.Tests.Models;

[DynamoDBTable("GsiItems")]
internal class GsiItem
{
    [DynamoDBHashKey]
    public string PK { get; set; } = "";

    [DynamoDBRangeKey]
    public string SK { get; set; } = "";

    [DynamoDBGlobalSecondaryIndexHashKey("GsiIndex")]
    public string GsiPK { get; set; } = "";

    [DynamoDBGlobalSecondaryIndexRangeKey("GsiIndex")]
    public string GsiSK { get; set; } = "";

    public string Data { get; set; } = "";
}
