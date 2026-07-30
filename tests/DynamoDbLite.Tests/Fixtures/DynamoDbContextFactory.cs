using Amazon.DynamoDBv2.DataModel;

namespace DynamoDbLite.Tests.Fixtures;

internal abstract class DynamoDbContextFactory
    : IDisposable
{
    public DynamoDbClient Client { get; }
    public DynamoDBContext Context { get; }
    private bool disposed;

    // Construction takes a creation function rather than calling an overridable factory method:
    // a virtual call here would dispatch to a subclass before its own construction finished.
    // The factory creates the client and therefore owns disposing it.
    protected DynamoDbContextFactory(Func<DynamoDbClient> createClient)
    {
        Client = createClient();
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

    private static DynamoDBContext CreateContext(DynamoDbClient c) =>
        new DynamoDBContextBuilder()
            .ConfigureContext(cfg => cfg.DisableFetchingTableMetadata = true)
            .WithDynamoDBClient(() => c)
            .Build();
}
