using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace DynamoDbLite;

public sealed partial class DynamoDbClient(DynamoDbLiteOptions? options = null)
    : DynamoDbService
    , IAmazonDynamoDB
    , IAmazonService
    , IDisposable
{
    private const int DefaultListTablesLimit = 100;

    private readonly SqliteStore store = new(options ?? new DynamoDbLiteOptions());
    private bool disposed;

    public IDynamoDBv2PaginatorFactory? Paginators { get; }

    public void Dispose()
    {
        if (disposed)
            return;

        store.Dispose();
        disposed = true;
    }

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(disposed, this);
}
