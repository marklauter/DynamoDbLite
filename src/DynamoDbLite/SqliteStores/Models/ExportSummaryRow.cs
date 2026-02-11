namespace DynamoDbLite.SqliteStores.Models;

internal sealed record ExportSummaryRow(
    string ExportArn,
    string Status);
