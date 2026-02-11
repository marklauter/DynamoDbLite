using Microsoft.Data.Sqlite;
using System.Data.Common;

namespace DynamoDbLite.SqlteStores;

internal sealed class InMemorySqliteStore
    : SqliteStore
{
    private readonly SqliteConnection sentinel;
    private readonly AsyncReaderWriterLock rwLock = new();

    internal InMemorySqliteStore(DynamoDbLiteOptions options)
        : base(options, createTables: false)
    {
        sentinel = new SqliteConnection(ConnectionString);
        sentinel.Open();
        CreateTables();
    }

    protected override async Task<DbConnection> OpenConnectionAsync(CancellationToken ct)
    {
        var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(ct).ConfigureAwait(false);
        return connection;
    }

    protected override ValueTask<IDisposable?> AcquireReadLockAsync(CancellationToken ct) =>
        rwLock.AcquireReadLockAsync(ct);

    protected override ValueTask<IDisposable?> AcquireWriteLockAsync(CancellationToken ct) =>
        rwLock.AcquireWriteLockAsync(ct);

    protected override void DisposeCore()
    {
        sentinel.Dispose();
        rwLock.Dispose();
    }
}
