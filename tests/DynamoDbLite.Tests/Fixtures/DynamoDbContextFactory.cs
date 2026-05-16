using Amazon.DynamoDBv2.DataModel;

namespace DynamoDbLite.Tests.Fixtures;

internal abstract class DynamoDbContextFactory
    : IDisposable
{
    public DynamoDbClient Client { get; }
    public DynamoDBContext Context { get; }
    private bool disposed;

    public DynamoDbContextFactory()
    {
        Client = CreateClient();
        Context = CreateContext(Client);
    }

    public virtual void Dispose()
    {
        if (disposed)
        {
            return;
        }

        GC.SuppressFinalize(this);

        Client.Dispose();
        Context.Dispose();
        disposed = true;
    }

    protected abstract DynamoDbClient CreateClient();

    private static DynamoDBContext CreateContext(DynamoDbClient c) =>
        new DynamoDBContextBuilder()
            .ConfigureContext(cfg => cfg.DisableFetchingTableMetadata = true)
            .WithDynamoDBClient(() => c)
            .Build();
}
