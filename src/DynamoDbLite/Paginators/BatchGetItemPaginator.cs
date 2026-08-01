using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace DynamoDbLite.Paginators;

internal sealed class BatchGetItemPaginator(IPaginator<BatchGetItemResponse> source)
    : IBatchGetItemPaginator
{
    public IPaginatedEnumerable<BatchGetItemResponse> Responses { get; } = new PaginatedResponse<BatchGetItemResponse>(source);
}
