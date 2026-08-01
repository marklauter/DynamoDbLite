using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite.Paginators;

/// <summary>
/// Builds paginators over <paramref name="client"/>. Construction issues no call: every factory
/// method hands back a paginator whose first request goes out on first enumeration.
/// </summary>
/// <param name="client">The client whose operations the paginators drive.</param>
internal sealed class DynamoDbPaginatorFactory(IAmazonDynamoDB client)
    : IDynamoDBv2PaginatorFactory
{
    public IScanPaginator Scan(ScanRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        return new ScanPaginator(new OperationPaginator<ScanRequest, ScanResponse, Dictionary<string, AttributeValue>>(
            request,
            request.ExclusiveStartKey,
            client.ScanAsync,
            static response => response.LastEvaluatedKey is { Count: > 0 } key ? key : null,
            static (r, token) => r.ExclusiveStartKey = token));
    }

    public IQueryPaginator Query(QueryRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        return new QueryPaginator(new OperationPaginator<QueryRequest, QueryResponse, Dictionary<string, AttributeValue>>(
            request,
            request.ExclusiveStartKey,
            client.QueryAsync,
            static response => response.LastEvaluatedKey is { Count: > 0 } key ? key : null,
            static (r, token) => r.ExclusiveStartKey = token));
    }

    public IListTablesPaginator ListTables(ListTablesRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        return new ListTablesPaginator(new OperationPaginator<ListTablesRequest, ListTablesResponse, string>(
            request,
            request.ExclusiveStartTableName,
            client.ListTablesAsync,
            static response => response.LastEvaluatedTableName is { Length: > 0 } name ? name : null,
            static (r, token) => r.ExclusiveStartTableName = token));
    }

    public IListExportsPaginator ListExports(ListExportsRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        return new ListExportsPaginator(new OperationPaginator<ListExportsRequest, ListExportsResponse, string>(
            request,
            request.NextToken,
            client.ListExportsAsync,
            static response => response.NextToken is { Length: > 0 } next ? next : null,
            static (r, token) => r.NextToken = token));
    }

    public IListImportsPaginator ListImports(ListImportsRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        return new ListImportsPaginator(new OperationPaginator<ListImportsRequest, ListImportsResponse, string>(
            request,
            request.NextToken,
            client.ListImportsAsync,
            static response => response.NextToken is { Length: > 0 } next ? next : null,
            static (r, token) => r.NextToken = token));
    }

    public IListContributorInsightsPaginator ListContributorInsights(ListContributorInsightsRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        return new ListContributorInsightsPaginator(new OperationPaginator<ListContributorInsightsRequest, ListContributorInsightsResponse, string>(
            request,
            request.NextToken,
            client.ListContributorInsightsAsync,
            static response => response.NextToken is { Length: > 0 } next ? next : null,
            static (r, token) => r.NextToken = token));
    }

    public IBatchGetItemPaginator BatchGetItem(BatchGetItemRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        return new BatchGetItemPaginator(new OperationPaginator<BatchGetItemRequest, BatchGetItemResponse, Dictionary<string, KeysAndAttributes>>(
            request,
            request.RequestItems,
            client.BatchGetItemAsync,
            static response => response.UnprocessedKeys is { Count: > 0 } keys ? keys : null,
            static (r, token) => r.RequestItems = token));
    }
}
