using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace DynamoDbLite;

public sealed partial class DynamoDbClient
{
    /// <inheritdoc/>
    public Task<DescribeEndpointsResponse> DescribeEndpointsAsync(DescribeEndpointsRequest request, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);

        return Task.FromResult(new DescribeEndpointsResponse
        {
            Endpoints = [new Endpoint { Address = "dynamodb.localhost", CachePeriodInMinutes = 1440 }],
            HttpStatusCode = System.Net.HttpStatusCode.OK
        });
    }

    /// <inheritdoc/>
    public Task<DescribeLimitsResponse> DescribeLimitsAsync(DescribeLimitsRequest request, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);

        return Task.FromResult(new DescribeLimitsResponse
        {
            AccountMaxReadCapacityUnits = 80_000,
            AccountMaxWriteCapacityUnits = 80_000,
            TableMaxReadCapacityUnits = 40_000,
            TableMaxWriteCapacityUnits = 40_000,
            HttpStatusCode = System.Net.HttpStatusCode.OK
        });
    }

    /// <inheritdoc/>
    public Amazon.Runtime.Endpoints.Endpoint DetermineServiceOperationEndpoint(AmazonWebServiceRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        return new Amazon.Runtime.Endpoints.Endpoint("http://dynamodb.localhost");
    }
}
