using Amazon.DynamoDBv2.DataModel;

namespace DynamoDbLite.Tests.Models;

[DynamoDBTable("SimpleItems")]
public class SimpleItem
{
    [DynamoDBHashKey]
    public string Id { get; set; } = "";

    public string Name { get; set; } = "";

    public int Age { get; set; }

    public double Score { get; set; }

    public bool IsActive { get; set; }
}
