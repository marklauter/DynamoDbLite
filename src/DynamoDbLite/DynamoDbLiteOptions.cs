namespace DynamoDbLite;

/// <summary>
/// Configuration for <see cref="DynamoDbClient"/>. The required parameter is the SQLite connection string;
/// see <see cref="ConnectionString"/> for the in-memory and file-based forms.
/// </summary>
public sealed record DynamoDbLiteOptions(
    /// <summary>
    /// The SQLite connection string. No default — consumers must opt in to either an in-memory or file-based store.
    ///
    /// <para>
    /// <b>File-based:</b> <c>"Data Source=mydb.db"</c>. The file is created on first write if it does not exist.
    /// </para>
    ///
    /// <para>
    /// <b>In-memory:</b> <c>"Data Source=&lt;unique-name&gt;;Mode=Memory;Cache=Shared"</c>.
    /// <c>Cache=Shared</c> is required — the library opens a fresh connection per operation and would otherwise see
    /// an empty database. The <c>Data Source</c> name keys the shared cache; two clients in the same process that
    /// use the same name share one in-memory database, two clients that use different names do not. For test
    /// isolation, suffix the name with a unique value such as <c>$"Data Source=app_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"</c>.
    /// </para>
    /// </summary>
    string ConnectionString,
    /// <summary>
    /// Enables SQLite Write-Ahead Logging on file-backed stores. Default <c>false</c>. When using
    /// <see cref="DynamoDbLiteOptionsBuilder"/>, set this by calling <see cref="DynamoDbLiteOptionsBuilder.WithWriteAheadLog"/>.
    /// WAL improves reader-writer concurrency by letting readers proceed while a writer holds the write lock.
    /// Has no effect on in-memory stores (SQLite does not support WAL for <c>:memory:</c> databases and silently
    /// falls back to the <c>memory</c> journal mode). WAL is persistent on the database file once enabled;
    /// disabling later requires an explicit <c>PRAGMA journal_mode=DELETE</c> outside this library.
    /// </summary>
    bool UseWriteAheadLog = false);
