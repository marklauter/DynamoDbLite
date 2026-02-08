namespace DynamoDbLite;

public sealed record DynamoDbLiteOptions(
    string ConnectionString = "Data Source=DynamoDbLite;Mode=Memory;Cache=Shared");
