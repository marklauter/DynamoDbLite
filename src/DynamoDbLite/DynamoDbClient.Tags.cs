using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.SqliteStores;

namespace DynamoDbLite;

public sealed partial class DynamoDbClient
{
    private const int MaxTagsPerResource = 50;
    private const int MaxTagKeyLength = 128;
    private const int MaxTagValueLength = 256;

    /// <inheritdoc/>
    public async Task<TagResourceResponse> TagResourceAsync(TagResourceRequest request, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);

        var tableName = SqliteStore.ExtractTableNameFromArn(request.ResourceArn);

        if (!await store.TableExistsAsync(tableName, cancellationToken))
            throw new ResourceNotFoundException($"Requested resource not found: Table: {tableName} not found");

        if (request.Tags is not { Count: > 0 })
            return new TagResourceResponse { HttpStatusCode = System.Net.HttpStatusCode.OK };

        ValidateTags(request.Tags);

        var existing = await store.GetTagsAsync(tableName, cancellationToken);
        var merged = new Dictionary<string, string>(existing.Count + request.Tags.Count);
        foreach (var (key, value) in existing)
            merged[key] = value;
        foreach (var tag in request.Tags)
            merged[tag.Key] = tag.Value;

        if (merged.Count > MaxTagsPerResource)
            throw new AmazonDynamoDBException(
                $"One or more parameter values were invalid: Too many tags: {merged.Count}, maximum is {MaxTagsPerResource}");

        await store.SetTagsAsync(tableName,
            [.. request.Tags.Select(static t => (t.Key, t.Value))], cancellationToken);

        return new TagResourceResponse { HttpStatusCode = System.Net.HttpStatusCode.OK };
    }

    /// <inheritdoc/>
    public async Task<UntagResourceResponse> UntagResourceAsync(UntagResourceRequest request, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);

        var tableName = SqliteStore.ExtractTableNameFromArn(request.ResourceArn);

        if (!await store.TableExistsAsync(tableName, cancellationToken))
            throw new ResourceNotFoundException($"Requested resource not found: Table: {tableName} not found");

        await store.RemoveTagsAsync(tableName, request.TagKeys, cancellationToken);

        return new UntagResourceResponse { HttpStatusCode = System.Net.HttpStatusCode.OK };
    }

    /// <inheritdoc/>
    public async Task<ListTagsOfResourceResponse> ListTagsOfResourceAsync(ListTagsOfResourceRequest request, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);

        var tableName = SqliteStore.ExtractTableNameFromArn(request.ResourceArn);

        if (!await store.TableExistsAsync(tableName, cancellationToken))
            throw new ResourceNotFoundException($"Requested resource not found: Table: {tableName} not found");

        var tags = await store.GetTagsAsync(tableName, cancellationToken);

        return new ListTagsOfResourceResponse
        {
            Tags = [.. tags.Select(static t => new Tag { Key = t.Key, Value = t.Value })],
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }

    private static void ValidateTags(List<Tag>? tags)
    {
        if (tags is not { Count: > 0 })
            return;

        foreach (var tag in tags)
        {
            if (tag.Key is not null && tag.Key.Length > MaxTagKeyLength)
                throw new AmazonDynamoDBException(
                    $"One or more parameter values were invalid: Tag key exceeds maximum length of {MaxTagKeyLength}");
            if (tag.Value is not null && tag.Value.Length > MaxTagValueLength)
                throw new AmazonDynamoDBException(
                    $"One or more parameter values were invalid: Tag value exceeds maximum length of {MaxTagValueLength}");
        }
    }
}
