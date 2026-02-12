using Amazon.DynamoDBv2;
using Amazon.Runtime;
using System.Diagnostics.CodeAnalysis;

namespace DynamoDbLite;

public class DynamoDbService
    : IAmazonService
{
    public IClientConfig Config { get; } = new AmazonDynamoDBConfig();

    public static ClientConfig CreateDefaultClientConfig() => new AmazonDynamoDBConfig();

    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP005:Return type should indicate that the value should be disposed", Justification = "AWS defined the interface")]
    public static IAmazonService CreateDefaultServiceClient(AWSCredentials awsCredentials, ClientConfig clientConfig) =>
        new DynamoDbClient();
}
