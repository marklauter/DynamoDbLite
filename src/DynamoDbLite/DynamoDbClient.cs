using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using DynamoDbLite.SqliteStores;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace DynamoDbLite;

public sealed partial class DynamoDbClient(
    DynamoDbLiteOptions? options = null,
    ILogger<DynamoDbClient>? logger = null)
    : IAmazonDynamoDB
    , IAmazonService
    , IDisposable
{
    private const int DefaultListTablesLimit = 100;
    private const int MaxTransactItems = 100;

    private readonly SqliteStore store = CreateStore(options ?? new DynamoDbLiteOptions());
    private readonly ConcurrentDictionary<string, (DateTime Expiry, TransactWriteItemsResponse Response)> transactWriteTokenCache = new();
    private readonly ILogger<DynamoDbClient> logger = logger ?? NullLogger<DynamoDbClient>.Instance;
    private bool disposed;

    public IClientConfig Config { get; } = new AmazonDynamoDBConfig();

    public IDynamoDBv2PaginatorFactory? Paginators { get; }

    public static ClientConfig CreateDefaultClientConfig() => new AmazonDynamoDBConfig();

    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP005:Return type should indicate that the value should be disposed", Justification = "AWS defined the interface")]
    public static IAmazonService CreateDefaultServiceClient(AWSCredentials awsCredentials, ClientConfig clientConfig) =>
        new DynamoDbClient();

    public void Dispose()
    {
        if (disposed)
            return;

        store.Dispose();
        disposed = true;
    }

    private void TriggerBackgroundCleanup(string tableName) =>
        _ = CleanupExpiredItemsSafeAsync(tableName);

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Fire-and-forget background task; unhandled exceptions would crash the process")]
    private async Task CleanupExpiredItemsSafeAsync(string tableName)
    {
        try
        {
            await store.CleanupExpiredItemsAsync(tableName);
        }
        catch (Exception ex)
        {
            LogCleanupFailed(ex, tableName);
        }
    }

    [LoggerMessage(EventId = 1, Level = LogLevel.Error, Message = "Background TTL cleanup failed for table {TableName}")]
    private partial void LogCleanupFailed(Exception ex, string tableName);

    [LoggerMessage(EventId = 2, Level = LogLevel.Error, Message = "Export background task failed for export {ExportArn}")]
    private partial void LogExportFailed(Exception ex, string exportArn);

    [LoggerMessage(EventId = 3, Level = LogLevel.Error, Message = "Failed to persist FAILED status for export {ExportArn}; original error: {OriginalMessage}")]
    private partial void LogExportStatusWriteFailed(Exception ex, string exportArn, string originalMessage);

    [LoggerMessage(EventId = 4, Level = LogLevel.Error, Message = "Import background task failed for import {ImportArn}")]
    private partial void LogImportFailed(Exception ex, string importArn);

    [LoggerMessage(EventId = 5, Level = LogLevel.Error, Message = "Failed to persist FAILED status for import {ImportArn}; original error: {OriginalMessage}")]
    private partial void LogImportStatusWriteFailed(Exception ex, string importArn, string originalMessage);

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(disposed, this);

    private static SqliteStore CreateStore(DynamoDbLiteOptions options)
    {
        var cs = options.ConnectionString ?? string.Empty;
        var isMemory = cs.Contains(":memory:", StringComparison.OrdinalIgnoreCase)
            || cs.Contains("Mode=Memory", StringComparison.OrdinalIgnoreCase);
        return isMemory
            ? new InMemorySqliteStore(options)
            : new FileSqliteStore(options);
    }
}
