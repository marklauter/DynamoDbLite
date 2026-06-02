using Microsoft.Data.Sqlite;

namespace DynamoDbLite;

/// <summary>
/// Configuration for <see cref="DynamoDbClient"/>. The required parameter is the SQLite connection string;
/// see <see cref="ConnectionString"/> for the in-memory and file-based forms.
/// </summary>
/// <param name="ConnectionString">
/// The SQLite connection string. No default — consumers must opt in to either an in-memory or file-based store.
/// <para>
/// <b>File-based:</b> <c>"Data Source=mydb.db"</c>. The file is created on first write if it does not exist.
/// </para>
/// <para>
/// <b>In-memory:</b> <c>"Data Source=&lt;unique-name&gt;;Mode=Memory;Cache=Shared"</c>.
/// <c>Cache=Shared</c> is required — the library opens a fresh connection per operation and would otherwise see
/// an empty database. The <c>Data Source</c> name keys the shared cache; two clients in the same process that
/// use the same name share one in-memory database, two clients that use different names do not. For test
/// isolation, suffix the name with a unique value such as <c>$"Data Source=app_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"</c>.
/// </para>
/// </param>
/// <param name="UseWriteAheadLog">
/// Enables SQLite Write-Ahead Logging on file-backed stores. Default <c>false</c>. When using
/// <see cref="DynamoDbLiteOptionsBuilder"/>, set this by calling <see cref="DynamoDbLiteOptionsBuilder.WithWriteAheadLog"/>.
/// WAL improves reader-writer concurrency by letting readers proceed while a writer holds the write lock.
/// Has no effect on in-memory stores (SQLite does not support WAL for <c>:memory:</c> databases and silently
/// falls back to the <c>memory</c> journal mode). WAL is persistent on the database file once enabled;
/// disabling later requires an explicit <c>PRAGMA journal_mode=DELETE</c> outside this library.
/// </param>
public sealed record DynamoDbLiteOptions(string ConnectionString, bool UseWriteAheadLog = false)
{
    /// <summary>
    /// Maximum number of put/delete requests a single <c>BatchWriteItemAsync</c> call accepts before it throws.
    /// Default <c>25</c>, matching the limit the real AWS DynamoDB client (<see cref="Amazon.DynamoDBv2.IAmazonDynamoDB"/>)
    /// enforces on <c>BatchWriteItem</c>. Override it to relax that cap — for example, to seed more rows per call in
    /// tests than DynamoDB would allow in production. Lowering it below 25 tightens the cap instead.
    /// <para>
    /// This shifts only the request-size validation; it does not change how DynamoDB batches or how requests map to
    /// SQLite. When using <see cref="DynamoDbLiteOptionsBuilder"/>, set it with
    /// <see cref="DynamoDbLiteOptionsBuilder.WithMaxBatchWriteItems"/>.
    /// </para>
    /// </summary>
    public int MaxBatchWriteItems { get; init; } = 25;

    /// <summary>
    /// SQLite pragmas applied to every connection the client opens for an operation, after the library's own
    /// pragmas (<c>synchronous=NORMAL</c>, <c>temp_store=MEMORY</c>) and before <see cref="ConnectionInitializer"/>.
    /// Each entry becomes <c>PRAGMA Key=Value;</c>; entries run in order, so a later entry for the same pragma wins.
    /// Default empty. When using <see cref="DynamoDbLiteOptionsBuilder"/>, add entries with
    /// <see cref="DynamoDbLiteOptionsBuilder.WithPragma"/>.
    /// <para>
    /// Applies uniformly to in-memory and file-based stores — the library does not filter by pragma or store type.
    /// Choosing pragmas that suit the store is your responsibility; for example <c>busy_timeout</c> (how long a
    /// connection waits on a held write lock before failing with <c>SQLITE_BUSY</c>) matters for file-based stores
    /// under writer contention but is largely inert for in-memory stores, whose writers are already serialized.
    /// </para>
    /// <para>
    /// Pragma values cannot be parameterized, so names and values are validated for injection safety: a name must
    /// be a SQLite identifier and a value must be a signed integer or a bare keyword. String-valued pragmas and any
    /// other custom setup belong in <see cref="ConnectionInitializer"/>.
    /// </para>
    /// </summary>
    public IReadOnlyList<KeyValuePair<string, string>> Pragmas { get; init; } = [];

    /// <summary>
    /// Optional callback invoked on every connection the client opens for an operation, after <see cref="Pragmas"/>
    /// are applied. The connection is open; run any setup the SQLite provider supports (pragmas, custom functions,
    /// collations). SQLite is synchronous under Microsoft.Data.Sqlite, so synchronous command execution here is fine.
    /// Default <see langword="null"/>. The builder equivalent is
    /// <see cref="DynamoDbLiteOptionsBuilder.WithConnectionInitializer"/>.
    /// <para>
    /// Runs only on per-operation connections, not on transient setup connections (schema creation, the one-time WAL
    /// enable, or the in-memory keep-alive connection). Because it runs on each pooled open, keep it cheap.
    /// </para>
    /// </summary>
    public Action<SqliteConnection>? ConnectionInitializer { get; init; }
}
