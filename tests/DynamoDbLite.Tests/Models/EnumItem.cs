using Amazon.DynamoDBv2.DataModel;

namespace DynamoDbLite.Tests.Models;

internal enum ItemStatus
{
    Draft = 0,
    Published = 1,
    Archived = 2,
}

[DynamoDBTable("EnumItems")]
internal class EnumItem
{
    [DynamoDBHashKey]
    public string Id { get; set; } = "";

    public ItemStatus Status { get; set; }
}
