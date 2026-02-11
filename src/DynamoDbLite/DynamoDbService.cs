using Amazon.DynamoDBv2;
using Amazon.Runtime;

namespace DynamoDbLite;

public class DynamoDbService
    : IAmazonService
{
    public IClientConfig Config { get; } = new AmazonDynamoDBConfig();

    public static ClientConfig CreateDefaultClientConfig() => new AmazonDynamoDBConfig();

#pragma warning disable IDISP005 // Return type should indicate that the value should be disposed
    public static IAmazonService CreateDefaultServiceClient(AWSCredentials awsCredentials, ClientConfig clientConfig) =>
        new DynamoDbClient();
#pragma warning restore IDISP005
}
