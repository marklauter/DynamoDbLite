namespace DynamoDbLite.SqlteStores.Models;

internal sealed record ItemRow(
    string Pk, 
    string Sk, 
    string ItemJson);
