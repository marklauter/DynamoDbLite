using DynamoDbLite.Tests.Fixtures;
using System.Diagnostics.CodeAnalysis;

namespace DynamoDbLite.Tests.Pagination;

// IAmazonDynamoDB declares Paginators nullable, but DynamoDbLite never returns null from it:
// the idiomatic client.Paginators.Scan(req) must work for the client's whole lifetime.
public sealed class PaginatorFactoryTests
    : DynamoDbClientFixture
{
    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public void Paginators_OnLiveClient_IsNotNull(StoreType st) =>
        Assert.NotNull(Client(st).Paginators);

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public void Paginators_ReadTwice_IsNotNullBothTimes(StoreType st)
    {
        var client = Client(st);

        Assert.NotNull(client.Paginators);
        Assert.NotNull(client.Paginators);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP016:Don't use disposed instance", Justification = "Using the client after disposal is the behavior under test: Paginators stays non-null for the client's whole lifetime, disposal included.")]
    public void Paginators_AfterDispose_IsNotNull(StoreType st)
    {
        var client = Client(st);
        client.Dispose();

        Assert.NotNull(client.Paginators);
    }
}
