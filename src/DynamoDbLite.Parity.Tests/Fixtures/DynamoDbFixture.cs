using Amazon.DynamoDBv2;
using Amazon.Runtime;
using System.Diagnostics.CodeAnalysis;
using Testcontainers.DynamoDb;

namespace DynamoDbLite.Parity.Tests.Fixtures;

[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "required for xUnit collection fixture injection")]
public sealed class DynamoDbFixture
    : IAsyncLifetime
{
    private readonly DynamoDbContainer container =
        new DynamoDbBuilder("amazon/dynamodb-local:latest")
            .WithAutoRemove(true)
            .WithCleanUp(true)
            .Build();

    private readonly SemaphoreSlim localInitLock = new(1, 1);
    private DynamoDbClient? inMem;
    private DynamoDbClient? fileBased;
    private string? fileBasedDbPath;
    private AmazonDynamoDBClient? local;

    public async ValueTask<IAmazonDynamoDB> ClientAsync(ParityBackend backend, CancellationToken ct = default) => backend switch
    {
        ParityBackend.DdbLite => inMem ?? throw new InvalidOperationException("Fixture not initialized."),
        ParityBackend.DdbLiteFile => fileBased ?? throw new InvalidOperationException("Fixture not initialized."),
        ParityBackend.DynamoDbLocal => await EnsureLocalReadyAsync(ct),
        _ => throw new ArgumentOutOfRangeException(nameof(backend), backend, null),
    };

    public ValueTask InitializeAsync()
    {
        // Eager: in-proc backends are cheap to create.
        // Lazy: the DynamoDbLocal container only starts when a test first requests it.
        inMem?.Dispose();
        inMem = new DynamoDbClient(new DynamoDbLiteOptions($"Data Source=Parity_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));

        fileBased?.Dispose();
        FileBasedTestHelper.Cleanup(fileBasedDbPath);
        var (fileClient, dbPath) = FileBasedTestHelper.CreateFileBasedClient();
        fileBased = fileClient;
        fileBasedDbPath = dbPath;

        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        // Order matters: dispose the file-based client (closing the SQLite handle)
        // before attempting to delete the .db file, otherwise File.Delete throws on Windows.
        inMem?.Dispose();
        fileBased?.Dispose();
        FileBasedTestHelper.Cleanup(fileBasedDbPath);
        local?.Dispose();
        await container.DisposeAsync();
        localInitLock.Dispose();
    }

    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP003:Dispose previous before re-assigning", Justification = "we aren't reassigning")]
    private async ValueTask<AmazonDynamoDBClient> EnsureLocalReadyAsync(CancellationToken ct)
    {
        if (local is not null)
            return local;

        await localInitLock.WaitAsync(ct);
        try
        {
            await container.StartAsync(ct);
            var credentials = new BasicAWSCredentials("test", "test");
            var config = new AmazonDynamoDBConfig { ServiceURL = container.GetConnectionString() };
            local = new AmazonDynamoDBClient(credentials, config);
            return local;
        }
        finally
        {
            _ = localInitLock.Release();
        }
    }
}
