namespace DynamoDbLite;

/// <summary>
/// Configuration for <see cref="DynamoDbClient"/>. The single required parameter is the SQLite connection string;
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
    string ConnectionString);
