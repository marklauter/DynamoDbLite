using Amazon.DynamoDBv2.DataModel;

namespace DynamoDbLite.Tests.Models;

[DynamoDBTable("CollectionItems")]
public class CollectionItem
{
    [DynamoDBHashKey]
    public string Id { get; set; } = "";

    public List<string> Tags { get; set; } = [];

    public Dictionary<string, int> Scores { get; set; } = [];

    public HashSet<string> StringSet { get; set; } = [];

    public HashSet<int> NumberSet { get; set; } = [];
}
