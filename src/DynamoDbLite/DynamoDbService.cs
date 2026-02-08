using Amazon.Runtime;

namespace DynamoDbLite;

public class DynamoDbService
    : IAmazonService
{
    public IClientConfig? Config { get; }

    public static ClientConfig CreateDefaultClientConfig() => throw new NotImplementedException();

    public static IAmazonService CreateDefaultServiceClient(AWSCredentials awsCredentials, ClientConfig clientConfig) => throw new NotImplementedException();
}
