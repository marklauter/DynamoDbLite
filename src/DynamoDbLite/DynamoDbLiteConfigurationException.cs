namespace DynamoDbLite;

public sealed class DynamoDbLiteConfigurationException
    : Exception
{
    public DynamoDbLiteConfigurationException()
    {
    }

    public DynamoDbLiteConfigurationException(string message)
        : base(message)
    {
    }

    public DynamoDbLiteConfigurationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
