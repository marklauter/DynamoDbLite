namespace DynamoDbLite.SqliteStores.Models;

internal sealed record IndexItemRow(
    string Pk,
    string Sk,
    string TablePk,
    string TableSk,
    string ItemJson);
