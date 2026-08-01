using Amazon.Runtime;
using System.Runtime.CompilerServices;

namespace DynamoDbLite.Paginators;

/// <summary>
/// Drives one DynamoDB operation as a page sequence: call, yield the response, feed the response's
/// continuation token into the next call, stop when the token comes back absent.
/// </summary>
/// <typeparam name="TRequest">The operation's request type.</typeparam>
/// <typeparam name="TResponse">The operation's response type.</typeparam>
/// <typeparam name="TToken">The continuation token's type — a key dictionary or a string.</typeparam>
/// <param name="request">The caller's request instance. See the note on <see cref="PaginateAsync"/>.</param>
/// <param name="initialToken">The token the caller supplied on <paramref name="request"/>, possibly none.</param>
/// <param name="invoke">Issues the operation.</param>
/// <param name="nextToken">
/// Projects the response's continuation token, normalizing absence — null, an empty dictionary, and
/// an empty string all become <see langword="null"/>, which ends the enumeration.
/// </param>
/// <param name="writeToken">Places a token on the request for the next call.</param>
internal sealed class OperationPaginator<TRequest, TResponse, TToken>(
    TRequest request,
    TToken? initialToken,
    Func<TRequest, CancellationToken, Task<TResponse>> invoke,
    Func<TResponse, TToken?> nextToken,
    Action<TRequest, TToken?> writeToken)
    : IPaginator<TResponse>
    where TRequest : class
    where TToken : class
{
    /// <summary>
    /// Enumerates the operation's pages, starting from the caller's original token every time — so a
    /// paginator enumerated twice yields the same pages twice.
    /// </summary>
    /// <param name="cancellationToken">Reaches every underlying call.</param>
    /// <remarks>
    /// Deliberate deviation: the token is written onto the request instance the caller supplied rather
    /// than onto a per-page clone. The AWS SDK's own paginators do the same, and cloning would mean
    /// hand-copying every property of each request type — code that silently rots as the SDK adds
    /// properties. The cost is that one request instance must not back two concurrent enumerations.
    /// </remarks>
    public async IAsyncEnumerable<TResponse> PaginateAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var token = initialToken;
        do
        {
            writeToken(request, token);
            var response = await invoke(request, cancellationToken);
            token = nextToken(response);
            yield return response;
        }
        while (token is not null);
    }
}
