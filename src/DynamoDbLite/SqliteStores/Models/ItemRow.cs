namespace DynamoDbLite.SqliteStores.Models;

internal sealed record ItemRow(
    string Pk,
    string Sk,
    string ItemJson);
