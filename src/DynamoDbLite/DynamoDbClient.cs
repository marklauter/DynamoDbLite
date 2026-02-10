using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using System.Collections.Concurrent;

namespace DynamoDbLite;

public sealed partial class DynamoDbClient(DynamoDbLiteOptions? options = null)
    : DynamoDbService
    , IAmazonDynamoDB
    , IAmazonService
    , IDisposable
{
    private const int DefaultListTablesLimit = 100;
    private const int MaxTransactItems = 100;

    private readonly SqliteStore store = new(options ?? new DynamoDbLiteOptions());
    private readonly ConcurrentDictionary<string, (DateTime Expiry, TransactWriteItemsResponse Response)> transactWriteTokenCache = new();
    private bool disposed;

    public IDynamoDBv2PaginatorFactory? Paginators { get; }

    public void Dispose()
    {
        if (disposed)
            return;

        store.Dispose();
        disposed = true;
    }

    private void TriggerBackgroundCleanup(string tableName) =>
        _ = Task.Run(async () =>
        {
            try
            {
                await store.CleanupExpiredItemsAsync(tableName);
            }
            catch
            {
                // todo: logging would be good
                /* swallow */
            }
        });

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(disposed, this);
}
