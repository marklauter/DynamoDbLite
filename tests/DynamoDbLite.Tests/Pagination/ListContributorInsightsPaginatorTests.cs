using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;
using System.Diagnostics.CodeAnalysis;
using static DynamoDbLite.Tests.Pagination.PaginatorTestSupport;

namespace DynamoDbLite.Tests.Pagination;

// DynamoDbLite does not support ListContributorInsights. The paginator carries no special case for
// it: construction succeeds, and the underlying operation's NotSupportedException surfaces on first
// enumeration. Only the exception type is asserted, never its message.
public sealed class ListContributorInsightsPaginatorTests
    : DynamoDbClientFixture
{
    // ── Laziness ────────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public void ListContributorInsights_Constructed_DoesNotThrow(StoreType st)
    {
        var paginator = Client(st).Paginators!.ListContributorInsights(new ListContributorInsightsRequest());

        Assert.NotNull(paginator);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP016:Don't use disposed instance", Justification = "Constructing a paginator on a disposed client is the behavior under test: construction issues no call, so it must not throw.")]
    public void ListContributorInsights_OnDisposedClient_ConstructsWithoutIssuingCall(StoreType st)
    {
        var client = Client(st);
        client.Dispose();

        var paginator = client.Paginators!.ListContributorInsights(new ListContributorInsightsRequest());

        Assert.NotNull(paginator);
    }

    // ── Unsupported operation ───────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task ListContributorInsights_Enumerated_ThrowsNotSupported(StoreType st)
    {
        var paginator = Client(st).Paginators!.ListContributorInsights(new ListContributorInsightsRequest());

        _ = await Assert.ThrowsAsync<NotSupportedException>(
            () => CollectAsync(paginator.Responses, TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task ListContributorInsights_ReEnumerated_ThrowsNotSupportedAgain(StoreType st)
    {
        var paginator = Client(st).Paginators!.ListContributorInsights(new ListContributorInsightsRequest());

        _ = await Assert.ThrowsAsync<NotSupportedException>(
            () => CollectAsync(paginator.Responses, TestContext.Current.CancellationToken));
        _ = await Assert.ThrowsAsync<NotSupportedException>(
            () => CollectAsync(paginator.Responses, TestContext.Current.CancellationToken));
    }
}
