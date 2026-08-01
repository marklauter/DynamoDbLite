using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace DynamoDbLite.Paginators;

internal sealed class ListContributorInsightsPaginator(IPaginator<ListContributorInsightsResponse> source)
    : IListContributorInsightsPaginator
{
    public IPaginatedEnumerable<ListContributorInsightsResponse> Responses { get; } = new PaginatedResponse<ListContributorInsightsResponse>(source);
}
