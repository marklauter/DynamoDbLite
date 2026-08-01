using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace DynamoDbLite.Paginators;

internal sealed class ListTablesPaginator(IPaginator<ListTablesResponse> source)
    : IListTablesPaginator
{
    public IPaginatedEnumerable<ListTablesResponse> Responses { get; } = new PaginatedResponse<ListTablesResponse>(source);

    public IPaginatedEnumerable<string> TableNames { get; } =
        new PaginatedResultKeyResponse<ListTablesResponse, string>(source, static response => response.TableNames ?? []);
}
