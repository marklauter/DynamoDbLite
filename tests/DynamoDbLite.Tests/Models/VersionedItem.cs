using Amazon.DynamoDBv2.DataModel;

namespace DynamoDbLite.Tests.Models;

[DynamoDBTable("VersionedItems")]
internal class VersionedItem
{
    [DynamoDBHashKey]
    public string Id { get; set; } = "";

    public string Data { get; set; } = "";

    [DynamoDBVersion]
    public int? VersionNumber { get; set; }
}
