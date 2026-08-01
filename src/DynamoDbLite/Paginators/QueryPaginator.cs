using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace DynamoDbLite.Paginators;

internal sealed class QueryPaginator(IPaginator<QueryResponse> source)
    : IQueryPaginator
{
    public IPaginatedEnumerable<QueryResponse> Responses { get; } = new PaginatedResponse<QueryResponse>(source);
}
