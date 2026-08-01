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
/// <param name="request">
/// The caller's request instance, carrying whatever continuation token the caller supplied — possibly
/// none. The first call goes out with the request exactly as given. See the note on
/// <see cref="PaginateAsync"/>.
/// </param>
/// <param name="invoke">Issues the operation.</param>
/// <param name="nextToken">
/// Projects the response's continuation token, normalizing absence — null, an empty dictionary, and
/// an empty string all become <see langword="null"/>, which ends the enumeration.
/// </param>
/// <param name="writeToken">Places a token on the request for the next call.</param>
internal sealed class OperationPaginator<TRequest, TResponse, TToken>(
    TRequest request,
    Func<TRequest, CancellationToken, Task<TResponse>> invoke,
    Func<TResponse, TToken?> nextToken,
    Action<TRequest, TToken?> writeToken)
    : IPaginator<TResponse>
    where TRequest : class
    where TToken : class
{
    private int consumed;

    /// <summary>
    /// Enumerates the operation's pages, once. The paginator is single-use, matching the AWS SDK's own
    /// paginators: consumption is marked when enumeration begins, so beginning a second enumeration
    /// throws even when the first was abandoned partway through. Reading a paginator's
    /// <c>Responses</c> property consumes nothing; enumerating it does.
    /// </summary>
    /// <param name="cancellationToken">Reaches every underlying call.</param>
    /// <exception cref="InvalidOperationException">The paginator has already been enumerated.</exception>
    /// <remarks>
    /// Deliberate deviation: the token is written onto the request instance the caller supplied rather
    /// than onto a per-page clone. The AWS SDK's own paginators do the same, and cloning would mean
    /// hand-copying every property of each request type — code that silently rots as the SDK adds
    /// properties. The cost is that a request instance is spent once it has backed an enumeration:
    /// it still carries the last continuation token, so reusing it for a later paginator resumes
    /// where the previous enumeration stopped rather than starting over. That applies to sequential
    /// reuse, not only to concurrent use. The AWS SDK's own paginators degrade identically.
    /// </remarks>
    public async IAsyncEnumerable<TResponse> PaginateAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref consumed, 1) != 0)
            throw new InvalidOperationException(
                "Paginator has already been consumed and cannot be reused. Please create a new instance.");

        while (true)
        {
            // The token is threaded into every underlying call so that cancellation bounds the call
            // that is actually in flight. This propagation is NOT observable through the AWS-public
            // surface: PaginatedResponse<T> checks the token itself after pulling each page, so an
            // implementation that dropped the token entirely would still surface
            // OperationCanceledException from the enumeration and look identical from outside. It is
            // kept because it is correct, not because a test can pin it — no test through this
            // surface can.
            var response = await invoke(request, cancellationToken);
            var token = nextToken(response);
            yield return response;

            if (token is null)
                yield break;

            writeToken(request, token);
        }
    }
}
