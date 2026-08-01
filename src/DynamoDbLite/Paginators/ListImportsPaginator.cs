using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace DynamoDbLite.Paginators;

internal sealed class ListImportsPaginator(IPaginator<ListImportsResponse> source)
    : IListImportsPaginator
{
    public IPaginatedEnumerable<ListImportsResponse> Responses { get; } = new PaginatedResponse<ListImportsResponse>(source);
}
