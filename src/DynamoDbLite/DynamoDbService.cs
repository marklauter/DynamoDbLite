using Amazon.DynamoDBv2;
using Amazon.Runtime;

namespace DynamoDbLite;

public class DynamoDbService
    : IAmazonService
{
    public IClientConfig Config { get; } = new AmazonDynamoDBConfig();

    public static ClientConfig CreateDefaultClientConfig() => new AmazonDynamoDBConfig();

    public static IAmazonService CreateDefaultServiceClient(AWSCredentials awsCredentials, ClientConfig clientConfig) =>
        new DynamoDbClient();
}
