using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using DynamoDbLite.Paginators;
using DynamoDbLite.SqliteStores;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace DynamoDbLite;

/// <summary>
/// In-process <see cref="Amazon.DynamoDBv2.IAmazonDynamoDB"/> implementation backed by SQLite. Construct directly via
/// <c>new DynamoDbClient(options)</c> or register via <see cref="ServiceCollectionExtensions.AddDynamoDbLite"/>.
/// </summary>
public sealed partial class DynamoDbClient
    : IAmazonDynamoDB
    , IAmazonService
    , IDisposable
{
    private const int DefaultListTablesLimit = 100;
    private const int MaxTransactItems = 100;

    private readonly SqliteStore store;
    private readonly int maxBatchWriteItems;
    private readonly ConcurrentDictionary<string, (DateTime Expiry, TransactWriteItemsResponse Response)> transactWriteTokenCache = new();
    private readonly ILogger<DynamoDbClient> logger;
    private bool disposed;

    /// <summary>
    /// Creates a client over the store described by <paramref name="options"/>: an in-memory store
    /// when the connection string names one, a file-backed store otherwise.
    /// </summary>
    /// <param name="options">Connection string and store settings.</param>
    /// <param name="logger">Optional logger; defaults to a no-op logger.</param>
    public DynamoDbClient(DynamoDbLiteOptions options, ILogger<DynamoDbClient>? logger = null)
        : this(static o => CreateStore(o), options, logger)
    {
    }

    // Takes a store factory rather than a store so a test can supply one that fails on demand: the
    // failure paths on this client only run when the store throws, which real SQLite will not do to
    // order. It is a factory rather than the store itself so the client always creates what it
    // disposes — a field holding a store that is sometimes owned and sometimes injected is a
    // disposal hazard, and IDISP008 says so.
    internal DynamoDbClient(
        Func<DynamoDbLiteOptions, SqliteStore> storeFactory,
        DynamoDbLiteOptions options,
        ILogger<DynamoDbClient>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(storeFactory);
        ArgumentNullException.ThrowIfNull(options);

        store = storeFactory(options);
        this.logger = logger ?? NullLogger<DynamoDbClient>.Instance;
        maxBatchWriteItems = options.MaxBatchWriteItems;
        Paginators = new DynamoDbPaginatorFactory(this);
    }

    /// <inheritdoc/>
    public IClientConfig Config { get; } = new AmazonDynamoDBConfig();

    /// <summary>
    /// Paginator factory over this client's operations. Never <see langword="null"/> — the nullable
    /// annotation comes from <see cref="IAmazonDynamoDB"/> and cannot be removed. The factory outlives
    /// <see cref="Dispose"/>: a paginator built from a disposed client throws
    /// <see cref="ObjectDisposedException"/> when it is enumerated, not when it is built.
    /// </summary>
    public IDynamoDBv2PaginatorFactory? Paginators { get; }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (disposed)
            return;

        store.Dispose();
        disposed = true;
    }

    // TTL cleanup reclaims space; it is never load-bearing for correctness, because every read
    // path filters expired rows by ttl_epoch. The store throttles the sweep to once per table per
    // 60 seconds, so all but one call per window returns after a dictionary lookup. A sweep that
    // fails is logged rather than thrown: the caller's operation has already succeeded on its own
    // terms, and reclamation retries on the next call.
    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "TTL reclamation is opportunistic and never load-bearing; a sweep failure must not fail the caller's operation")]
    private async Task CleanupExpiredItemsSafeAsync(string tableName, CancellationToken cancellationToken)
    {
        try
        {
            await store.CleanupExpiredItemsAsync(tableName, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            LogCleanupFailed(ex, tableName);
        }
    }

    [LoggerMessage(EventId = 1, Level = LogLevel.Error, Message = "TTL cleanup failed for table {TableName}")]
    private partial void LogCleanupFailed(Exception ex, string tableName);

    [LoggerMessage(EventId = 2, Level = LogLevel.Error, Message = "Export failed for export {ExportArn}")]
    private partial void LogExportFailed(Exception ex, string exportArn);

    [LoggerMessage(EventId = 3, Level = LogLevel.Error, Message = "Failed to persist FAILED status for export {ExportArn}; original error: {OriginalMessage}")]
    private partial void LogExportStatusWriteFailed(Exception ex, string exportArn, string originalMessage);

    [LoggerMessage(EventId = 4, Level = LogLevel.Error, Message = "Import failed for import {ImportArn}")]
    private partial void LogImportFailed(Exception ex, string importArn);

    [LoggerMessage(EventId = 5, Level = LogLevel.Error, Message = "Failed to persist FAILED status for import {ImportArn}; original error: {OriginalMessage}")]
    private partial void LogImportStatusWriteFailed(Exception ex, string importArn, string originalMessage);

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(disposed, this);

    private static SqliteStore CreateStore(DynamoDbLiteOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var cs = options.ConnectionString;
        var isMemory = cs.Contains(":memory:", StringComparison.OrdinalIgnoreCase)
            || cs.Contains("Mode=Memory", StringComparison.OrdinalIgnoreCase);
        return isMemory
            ? new InMemorySqliteStore(options)
            : new FileSqliteStore(options);
    }
}
