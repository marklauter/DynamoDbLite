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

    internal DynamoDbLiteOptions Build() =>
        new(ConnectionString
            ?? throw new DynamoDbLiteConfigurationException(
                "Connection string was not configured. Call WithConnectionString before Build."),
            UseWriteAheadLog);

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
