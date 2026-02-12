using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

public sealed class CapacityAndLimitsTests
    : DynamoDbClientFixture
{
    protected override async ValueTask SetupAsync(CancellationToken ct)
    {
        await CreateHashOnlyTableAsync(Client(StoreType.MemoryBased), "TestTable", ct);
        await CreateHashOnlyTableAsync(Client(StoreType.FileBased), "TestTable", ct);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DescribeEndpoints_Returns_Localhost_Endpoint(StoreType st)
    {
        var client = Client(st);

        var response = await client.DescribeEndpointsAsync(
            new DescribeEndpointsRequest(), TestContext.Current.CancellationToken);

        var endpoint = Assert.Single(response.Endpoints);
        Assert.Equal("dynamodb.localhost", endpoint.Address);
        Assert.Equal(1440, endpoint.CachePeriodInMinutes);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DescribeLimits_Returns_Default_Capacity_Values(StoreType st)
    {
        var client = Client(st);

        var response = await client.DescribeLimitsAsync(
            new DescribeLimitsRequest(), TestContext.Current.CancellationToken);

        Assert.Equal(80_000, response.AccountMaxReadCapacityUnits);
        Assert.Equal(80_000, response.AccountMaxWriteCapacityUnits);
        Assert.Equal(40_000, response.TableMaxReadCapacityUnits);
        Assert.Equal(40_000, response.TableMaxWriteCapacityUnits);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public void DetermineServiceOperationEndpoint_Returns_Localhost_Url(StoreType st)
    {
        var client = Client(st);

        var endpoint = client.DetermineServiceOperationEndpoint(
            new DescribeEndpointsRequest());

        Assert.Equal("http://dynamodb.localhost", endpoint.URL);
    }
}
