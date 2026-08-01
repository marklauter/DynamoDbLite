using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace DynamoDbLite.Paginators;

internal sealed class ListExportsPaginator(IPaginator<ListExportsResponse> source)
    : IListExportsPaginator
{
    public IPaginatedEnumerable<ListExportsResponse> Responses { get; } = new PaginatedResponse<ListExportsResponse>(source);
}
