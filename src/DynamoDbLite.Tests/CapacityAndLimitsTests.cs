using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite.Tests;

public sealed class CapacityAndLimitsTests : IAsyncLifetime
{
    private readonly DynamoDbClient client = new(new DynamoDbLiteOptions(
        $"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));

    public async ValueTask InitializeAsync() => _ = await client.CreateTableAsync(new CreateTableRequest
    {
        TableName = "TestTable",
        KeySchema =
            [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
        AttributeDefinitions =
            [new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }]
    }, TestContext.Current.CancellationToken);

    public ValueTask DisposeAsync()
    {
        client.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task DescribeEndpoints_Returns_Localhost_Endpoint()
    {
        var response = await client.DescribeEndpointsAsync(
            new DescribeEndpointsRequest(), TestContext.Current.CancellationToken);

        var endpoint = Assert.Single(response.Endpoints);
        Assert.Equal("dynamodb.localhost", endpoint.Address);
        Assert.Equal(1440, endpoint.CachePeriodInMinutes);
    }

    [Fact]
    public async Task DescribeLimits_Returns_Default_Capacity_Values()
    {
        var response = await client.DescribeLimitsAsync(
            new DescribeLimitsRequest(), TestContext.Current.CancellationToken);

        Assert.Equal(80_000, response.AccountMaxReadCapacityUnits);
        Assert.Equal(80_000, response.AccountMaxWriteCapacityUnits);
        Assert.Equal(40_000, response.TableMaxReadCapacityUnits);
        Assert.Equal(40_000, response.TableMaxWriteCapacityUnits);
    }

    [Fact]
    public void DetermineServiceOperationEndpoint_Returns_Localhost_Url()
    {
        var endpoint = client.DetermineServiceOperationEndpoint(
            new DescribeEndpointsRequest());

        Assert.Equal("http://dynamodb.localhost", endpoint.URL);
    }
}
