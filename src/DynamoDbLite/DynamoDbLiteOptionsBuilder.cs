using Microsoft.Data.Sqlite;

namespace DynamoDbLite;

/// <summary>
/// Fluent builder for <see cref="DynamoDbLiteOptions"/>. <see cref="WithConnectionString"/> validates the
/// SQLite connection string and throws <see cref="DynamoDbLiteConfigurationException"/> on a malformed value.
/// </summary>
public sealed class DynamoDbLiteOptionsBuilder
{
    private string? ConnectionString { get; set; }
    private bool UseWriteAheadLog { get; set; }
    private int MaxBatchWriteItems { get; set; } = 25;
    private List<KeyValuePair<string, string>> Pragmas { get; } = [];
    private Action<SqliteConnection>? ConnectionInitializer { get; set; }

    /// <summary>
    /// Sets the SQLite connection string after validating it parses as a SQLite connection string.
    /// See <see cref="DynamoDbLiteOptions.ConnectionString"/> for the in-memory and file-based forms,
    /// and for the test-isolation guidance around <c>Cache=Shared</c>.
    /// </summary>
    /// <param name="connectionString">A non-empty SQLite connection string.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="DynamoDbLiteConfigurationException">The connection string is null, empty, whitespace, or not a valid SQLite connection string.</exception>
    public DynamoDbLiteOptionsBuilder WithConnectionString(string connectionString)
    {
        ConnectionString = NormalizeConnectionString(connectionString);
        return this;
    }

    /// <summary>
    /// Enables SQLite Write-Ahead Logging for file-backed stores. WAL is off by default; call this to opt in.
    /// See <see cref="DynamoDbLiteOptions.UseWriteAheadLog"/> for the in-memory behavior and persistence semantics.
    /// </summary>
    /// <returns>This builder, for chaining.</returns>
    public DynamoDbLiteOptionsBuilder WithWriteAheadLog()
    {
        UseWriteAheadLog = true;
        return this;
    }

    /// <summary>
    /// Sets the maximum number of put/delete requests a single <c>BatchWriteItemAsync</c> call accepts. Defaults to
    /// <c>25</c> (AWS DynamoDB parity); raise it to seed more rows per call than DynamoDB allows, or lower it to
    /// tighten the cap. See <see cref="DynamoDbLiteOptions.MaxBatchWriteItems"/> for the validation semantics.
    /// </summary>
    /// <param name="maxBatchWriteItems">The per-call limit. Must be at least 1.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="DynamoDbLiteConfigurationException"><paramref name="maxBatchWriteItems"/> is less than 1.</exception>
    public DynamoDbLiteOptionsBuilder WithMaxBatchWriteItems(int maxBatchWriteItems)
    {
        if (maxBatchWriteItems < 1)
            throw new DynamoDbLiteConfigurationException(
                $"MaxBatchWriteItems must be at least 1, but was {maxBatchWriteItems}.");

        MaxBatchWriteItems = maxBatchWriteItems;
        return this;
    }

    /// <summary>
    /// Adds a SQLite pragma applied to every connection the client opens for an operation. Repeatable; entries run
    /// in call order after the library's own pragmas, so a later call for the same pragma wins. See
    /// <see cref="DynamoDbLiteOptions.Pragmas"/> for application semantics and the in-memory/file note.
    /// </summary>
    /// <param name="name">A pragma name: a SQLite identifier matching <c>[A-Za-z_][A-Za-z0-9_]*</c>.</param>
    /// <param name="value">A pragma value: a signed integer (e.g. <c>5000</c>) or a bare keyword (e.g. <c>NORMAL</c>).
    /// Pragma values cannot be parameterized; string-valued pragmas and other custom setup belong in
    /// <see cref="WithConnectionInitializer"/>.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="DynamoDbLiteConfigurationException">The name or value is null, empty, or contains characters outside the allowed set.</exception>
    public DynamoDbLiteOptionsBuilder WithPragma(string name, string value)
    {
        PragmaValidator.Validate(name, value);
        Pragmas.Add(new KeyValuePair<string, string>(name, value));
        return this;
    }

    /// <summary>
    /// Sets a callback invoked on every connection the client opens for an operation, after any
    /// <see cref="WithPragma"/> pragmas. Use it for setup that does not fit a simple <c>PRAGMA name=value</c> —
    /// string-valued pragmas, custom functions, collations. See <see cref="DynamoDbLiteOptions.ConnectionInitializer"/>
    /// for the run boundary and cost note. Calling this more than once replaces the previous callback.
    /// </summary>
    /// <param name="configure">Callback receiving the open <see cref="SqliteConnection"/>.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="configure"/> is null.</exception>
    public DynamoDbLiteOptionsBuilder WithConnectionInitializer(Action<SqliteConnection> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        ConnectionInitializer = configure;
        return this;
    }

    internal DynamoDbLiteOptions Build() =>
        new(ConnectionString
            ?? throw new DynamoDbLiteConfigurationException(
                "Connection string was not configured. Call WithConnectionString before Build."),
            UseWriteAheadLog)
        {
            MaxBatchWriteItems = MaxBatchWriteItems,
            Pragmas = [.. Pragmas],
            ConnectionInitializer = ConnectionInitializer,
        };

    /// <summary>
    /// Validates that <paramref name="connectionString"/> is a non-empty, well-formed SQLite connection string,
    /// and returns the parser-canonical form (keys cased, whitespace normalized).
    /// </summary>
    /// <param name="connectionString">A non-empty SQLite connection string.</param>
    /// <returns>The canonical form of the input connection string.</returns>
    /// <exception cref="DynamoDbLiteConfigurationException">The connection string is null, empty, whitespace, or not a valid SQLite connection string.</exception>
    private static string NormalizeConnectionString(string connectionString)
    {
        try
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
            return new SqliteConnectionStringBuilder(connectionString).ConnectionString;
        }
        catch (ArgumentException ex)
        {
            throw new DynamoDbLiteConfigurationException(
                $"Invalid SQLite connection string: '{connectionString ?? "<null>"}'.",
                ex);
        }
    }
}
