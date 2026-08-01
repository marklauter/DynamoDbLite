using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace DynamoDbLite.Paginators;

internal sealed class ScanPaginator(IPaginator<ScanResponse> source)
    : IScanPaginator
{
    public IPaginatedEnumerable<ScanResponse> Responses { get; } = new PaginatedResponse<ScanResponse>(source);
}
