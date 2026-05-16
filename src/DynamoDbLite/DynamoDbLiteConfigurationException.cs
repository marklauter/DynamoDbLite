namespace DynamoDbLite;

/// <summary>
/// Thrown when DynamoDbLite configuration is invalid — e.g. a missing or malformed connection string passed to
/// <see cref="DynamoDbLiteOptionsBuilder.WithConnectionString"/> or to <see cref="ServiceCollectionExtensions.AddDynamoDbLite"/>.
/// </summary>
public sealed class DynamoDbLiteConfigurationException
    : Exception
{
    /// <summary>Initializes a new instance with no message.</summary>
    public DynamoDbLiteConfigurationException()
    {
    }

    /// <summary>Initializes a new instance with the supplied message.</summary>
    public DynamoDbLiteConfigurationException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance with the supplied message and inner exception.</summary>
    public DynamoDbLiteConfigurationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
