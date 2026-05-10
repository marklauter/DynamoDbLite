using Microsoft.Data.Sqlite;

namespace DynamoDbLite;

public sealed class DynamoDbLiteOptionsBuilder
{
    private string ConnectionString { get; set; } = "Data Source=DynamoDbLite;Mode=Memory;Cache=Shared";

    public DynamoDbLiteOptionsBuilder WithConnectionString(string connectionString)
    {
        ConnectionString = Validate(connectionString);
        return this;
    }

    internal DynamoDbLiteOptions Build() => new(ConnectionString);

    private static string Validate(string connectionString)
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
