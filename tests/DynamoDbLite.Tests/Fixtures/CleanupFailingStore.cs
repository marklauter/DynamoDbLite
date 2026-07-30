using DynamoDbLite.SqliteStores;
using Microsoft.Data.Sqlite;
using System.Data.Common;

namespace DynamoDbLite.Tests.Fixtures;

// A working in-memory store whose TTL sweep always throws. Everything else behaves normally, so a
// test can prove that a failing sweep does not fail the read that triggered it. Real SQLite will not
// fail a sweep on demand, so this is the only way to reach that path.
internal sealed class CleanupFailingStore
    : SqliteStore
{
    private bool disposed;

    // Mirrors InMemorySqliteStore: the sentinel keeps the shared in-memory database alive, because
    // a Mode=Memory;Cache=Shared database is reclaimed when its last connection closes.
    private readonly SqliteConnection sentinel;

    internal CleanupFailingStore(DynamoDbLiteOptions options)
        : base(options, createTables: false)
    {
        sentinel = new SqliteConnection(ConnectionString);
        sentinel.Open();
        CreateTables();
    }

    internal override Task CleanupExpiredItemsAsync(
        string tableName,
        CancellationToken cancellationToken = default) =>
        throw new InvalidOperationException("simulated TTL sweep failure");

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
