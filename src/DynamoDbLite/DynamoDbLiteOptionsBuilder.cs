using Microsoft.Data.Sqlite;

namespace DynamoDbLite;

/// <summary>
/// Fluent builder for <see cref="DynamoDbLiteOptions"/>. <see cref="WithConnectionString"/> validates the
/// SQLite connection string and throws <see cref="DynamoDbLiteConfigurationException"/> on a malformed value.
/// </summary>
public sealed class DynamoDbLiteOptionsBuilder
{
    private string? ConnectionString { get; set; }

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
        ConnectionString = ValidateConnectionString(connectionString);
        return this;
    }

    internal DynamoDbLiteOptions Build() =>
        new(ConnectionString
            ?? throw new DynamoDbLiteConfigurationException(
                "Connection string was not configured. Call WithConnectionString before Build."));

    /// <summary>
    /// Validates that <paramref name="connectionString"/> is a non-empty, well-formed SQLite connection string.
    /// Returns the input unchanged on success so the call can be used in expression position.
    /// </summary>
    /// <param name="connectionString">A non-empty SQLite connection string.</param>
    /// <returns>The same connection string, unchanged.</returns>
    /// <exception cref="DynamoDbLiteConfigurationException">The connection string is null, empty, whitespace, or not a valid SQLite connection string.</exception>
    public static string ValidateConnectionString(string connectionString)
    {
        try
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
            _ = new SqliteConnectionStringBuilder(connectionString);
            return connectionString;
        }
        catch (ArgumentException ex)
        {
            throw new DynamoDbLiteConfigurationException(
                $"Invalid SQLite connection string: '{connectionString ?? "<null>"}'.",
                ex);
        }
    }
}
