using Microsoft.Data.Sqlite;
using System.Data.Common;

namespace DynamoDbLite.SqliteStores;

internal sealed class InMemorySqliteStore
    : SqliteStore
{
    private readonly SqliteConnection sentinel;
    private bool disposed;

    internal InMemorySqliteStore(DynamoDbLiteOptions options)
        : base(options, createTables: false)
    {
        // The sentinel keeps the shared in-memory database alive: a Mode=Memory;Cache=Shared
        // database is reclaimed when its last connection closes, and the store opens a fresh
        // connection per operation. Concurrency is left to SQLite and the Microsoft.Data.Sqlite
        // driver, which serialize writers via retry — no in-process lock (see ADR 0008).
        sentinel = new SqliteConnection(ConnectionString);
        sentinel.Open();
        CreateTables();
    }

    protected override async Task<DbConnection> OpenConnectionAsync(CancellationToken ct)
    {
        var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(ct);
        await PrepareConnectionAsync(connection, ct);
        return connection;
    }

    public override void Dispose()
    {
        if (!disposed)
        {
            sentinel.Dispose();
            disposed = true;
        }

        base.Dispose();
    }
}
