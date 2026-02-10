using Microsoft.Data.Sqlite;
using System.Data.Common;

namespace DynamoDbLite.SqlteStores;

internal sealed class FileSqliteStore
    : SqliteStoreBase
{
    internal FileSqliteStore(DynamoDbLiteOptions options)
        : base(options)
    {
        using var connection = new SqliteConnection(ConnectionString);
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "PRAGMA journal_mode=WAL";
        _ = cmd.ExecuteNonQuery();
    }

    protected override async Task<DbConnection> OpenConnectionAsync(CancellationToken ct)
    {
        var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(ct).ConfigureAwait(false);
        return connection;
    }

    protected override void DisposeCore()
    {
        // No-op: file-based connections are transient
    }
}
