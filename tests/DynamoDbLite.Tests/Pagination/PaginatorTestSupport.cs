namespace DynamoDbLite.Tests.Pagination;

// Drains an async sequence into a materialized list so a test can assert over pages as data.
// Hand-rolled rather than taken from a LINQ-over-IAsyncEnumerable helper: the token has to reach
// GetAsyncEnumerator, and an explicit WithCancellation makes that visible at the call site.
internal static class PaginatorTestSupport
{
    internal static async Task<IReadOnlyList<T>> CollectAsync<T>(IAsyncEnumerable<T> source, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(source);

        var collected = new List<T>();
        await foreach (var item in source.WithCancellation(ct))
            collected.Add(item);

        return collected;
    }

    // Begins an enumeration, pulls one element, then disposes without draining. A paginator is
    // consumed when enumeration begins, so this is enough to consume it.
    internal static async Task BeginAndAbandonAsync<T>(IAsyncEnumerable<T> source, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(source);

        var enumerator = source.GetAsyncEnumerator(ct);
        await using (enumerator.ConfigureAwait(false))
            _ = await enumerator.MoveNextAsync();
    }
}
