using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace DynamoDbLite;

// ── Administrative Operations (Not Yet Implemented) ─────────────────
public sealed partial class DynamoDbClient
{
    // ── Tags ─────────────────────────────────────────────────────────

    private const int MaxTagsPerResource = 50;
    private const int MaxTagKeyLength = 128;
    private const int MaxTagValueLength = 256;

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
            request.Tags.Select(static t => (t.Key, t.Value)).ToList(), cancellationToken);

        return new TagResourceResponse { HttpStatusCode = System.Net.HttpStatusCode.OK };
    }

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
            Tags = tags.Select(static t => new Tag { Key = t.Key, Value = t.Value }).ToList(),
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

    // ── Resource Policy ──────────────────────────────────────────────
    public Task<DeleteResourcePolicyResponse> DeleteResourcePolicyAsync(DeleteResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<GetResourcePolicyResponse> GetResourcePolicyAsync(GetResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<PutResourcePolicyResponse> PutResourcePolicyAsync(PutResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();

    // ── Contributor Insights ─────────────────────────────────────────
    public Task<DescribeContributorInsightsResponse> DescribeContributorInsightsAsync(DescribeContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ListContributorInsightsResponse> ListContributorInsightsAsync(ListContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<UpdateContributorInsightsResponse> UpdateContributorInsightsAsync(UpdateContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();

    // ── Time To Live ─────────────────────────────────────────────────
    public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = default) =>
        DescribeTimeToLiveAsync(new DescribeTimeToLiveRequest { TableName = tableName }, cancellationToken);

    public async Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(DescribeTimeToLiveRequest request, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableName);

        if (!await store.TableExistsAsync(request.TableName, cancellationToken))
            throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        var ttlAttr = await store.GetTtlAttributeNameAsync(request.TableName, cancellationToken);

        return new DescribeTimeToLiveResponse
        {
            TimeToLiveDescription = new TimeToLiveDescription
            {
                TimeToLiveStatus = ttlAttr is not null ? TimeToLiveStatus.ENABLED : TimeToLiveStatus.DISABLED,
                AttributeName = ttlAttr
            },
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }

    public async Task<UpdateTimeToLiveResponse> UpdateTimeToLiveAsync(UpdateTimeToLiveRequest request, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.TimeToLiveSpecification);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableName);

        if (!await store.TableExistsAsync(request.TableName, cancellationToken))
            throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        var currentAttr = await store.GetTtlAttributeNameAsync(request.TableName, cancellationToken);
        var spec = request.TimeToLiveSpecification;

        if (spec.Enabled is not true and not false)
            throw new AmazonDynamoDBException(
                "One or more parameter values were invalid: TimeToLiveSpecification.Enabled must be specified");

        if (spec.Enabled is true)
        {
            if (currentAttr is not null)
                throw new AmazonDynamoDBException(
                    "TimeToLive is already enabled on the table");

            ArgumentException.ThrowIfNullOrWhiteSpace(spec.AttributeName);
            await store.SetTtlConfigAsync(request.TableName, spec.AttributeName, cancellationToken);
            await BackfillTtlEpochAsync(request.TableName, spec.AttributeName, cancellationToken);
        }
        else
        {
            if (currentAttr is null)
                throw new AmazonDynamoDBException(
                    "TimeToLive is already disabled on the table");

            await store.RemoveTtlConfigAsync(request.TableName, cancellationToken);
            await store.ClearTtlEpochAsync(request.TableName, cancellationToken);
        }

        return new UpdateTimeToLiveResponse
        {
            TimeToLiveSpecification = spec,
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }

    private async Task BackfillTtlEpochAsync(string tableName, string ttlAttributeName, CancellationToken cancellationToken)
    {
        var items = await store.GetAllItemsAsync(tableName, cancellationToken);
        var updates = new List<(string Pk, string Sk, double? TtlEpoch)>(items.Count);
        foreach (var row in items)
        {
            var item = AttributeValueSerializer.Deserialize(row.ItemJson);
            var ttlEpoch = TtlHelper.ExtractTtlEpoch(item, ttlAttributeName);
            updates.Add((row.Pk, row.Sk, ttlEpoch));
        }

        await store.BatchUpdateTtlEpochAsync(tableName, updates, cancellationToken);

        var indexes = await store.GetIndexDefinitionsAsync(tableName, cancellationToken);
        foreach (var idx in indexes)
            await store.BackfillIndexTtlEpochAsync(tableName, idx.IndexName, ttlAttributeName, cancellationToken);
    }

    // ── Endpoints & Limits ───────────────────────────────────────────
    public Task<DescribeEndpointsResponse> DescribeEndpointsAsync(DescribeEndpointsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeLimitsResponse> DescribeLimitsAsync(DescribeLimitsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Amazon.Runtime.Endpoints.Endpoint DetermineServiceOperationEndpoint(AmazonWebServiceRequest request) => throw new NotImplementedException();

    // ── Update Table ─────────────────────────────────────────────────

    public async Task<UpdateTableResponse> UpdateTableAsync(
        string tableName,
        ProvisionedThroughput provisionedThroughput,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var description = await store.GetTableDescriptionAsync(tableName, cancellationToken)
            ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {tableName} not found");

        return new UpdateTableResponse
        {
            TableDescription = description,
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }

    public async Task<UpdateTableResponse> UpdateTableAsync(
        UpdateTableRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableName);

        var description = await store.GetTableDescriptionAsync(request.TableName, cancellationToken)
            ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        if (request.GlobalSecondaryIndexUpdates is { Count: > 0 })
        {
            var tableKeyInfo = await store.GetKeySchemaAsync(request.TableName, cancellationToken)!;
            var allIndexes = await store.GetIndexDefinitionsAsync(request.TableName, cancellationToken);
            var gsiDefs = allIndexes.Where(static i => i.IsGlobal).ToList();

            // Merge new attribute definitions if provided
            var attrDefs = request.AttributeDefinitions is { Count: > 0 }
                ? request.AttributeDefinitions
                : tableKeyInfo!.AttributeDefinitions;

            foreach (var update in request.GlobalSecondaryIndexUpdates)
            {
                if (update.Create is not null)
                {
                    if (gsiDefs.Count >= 5)
                        throw new AmazonDynamoDBException(
                            "One or more parameter values were invalid: GlobalSecondaryIndex count exceeds the per-table limit of 5");

                    if (gsiDefs.Any(g => g.IndexName == update.Create.IndexName))
                        throw new AmazonDynamoDBException(
                            $"One or more parameter values were invalid: Duplicate index name: {update.Create.IndexName}");

                    // Validate key schema attributes exist in attribute definitions
                    var definedNames = attrDefs.Select(static a => a.AttributeName).ToHashSet();
                    foreach (var key in update.Create.KeySchema)
                    {
                        if (!definedNames.Contains(key.AttributeName))
                            throw new AmazonDynamoDBException(
                                $"One or more parameter values were invalid: Index key attribute {key.AttributeName} is not defined in AttributeDefinitions");
                    }

                    var projectionType = update.Create.Projection?.ProjectionType?.Value ?? "ALL";
                    var nonKeyAttrs = update.Create.Projection?.NonKeyAttributes;
                    var newIdx = new IndexDefinition(
                        update.Create.IndexName, true, update.Create.KeySchema, projectionType, nonKeyAttrs);
                    gsiDefs.Add(newIdx);

                    // Read existing items and TTL config before starting transaction to avoid schema lock
                    var existingItems = await store.GetAllItemsAsync(request.TableName, cancellationToken);
                    var ttlAttrForBackfill = await store.GetTtlAttributeNameAsync(request.TableName, cancellationToken);

                    // Create index table and backfill
                    using var connection = await store.OpenConnectionAsync(cancellationToken);
                    using var transaction = await connection.BeginTransactionAsync(cancellationToken);

                    await SqliteStore.CreateIndexTableAsync(connection, request.TableName, newIdx.IndexName, transaction);
                    foreach (var existingRow in existingItems)
                    {
                        var item = AttributeValueSerializer.Deserialize(existingRow.ItemJson);
                        var keys = KeyHelper.TryExtractIndexKeys(item, newIdx.KeySchema, attrDefs);
                        if (keys is not null)
                        {
                            var skNum = SqliteStore.ComputeIndexSkNum(keys.Value.Sk, newIdx.KeySchema, attrDefs);
                            var ttlEpoch = ttlAttrForBackfill is not null ? TtlHelper.ExtractTtlEpoch(item, ttlAttrForBackfill) : null;
                            await SqliteStore.UpsertIndexEntryAsync(
                                connection, transaction, request.TableName, newIdx.IndexName,
                                keys.Value.Pk, keys.Value.Sk, skNum,
                                existingRow.Pk, existingRow.Sk, existingRow.ItemJson, ttlEpoch);
                        }
                    }

                    var updatedAttrDefs = request.AttributeDefinitions is { Count: > 0 }
                        ? request.AttributeDefinitions
                        : null;
                    await store.UpdateIndexMetadataInTransactionAsync(
                        connection, transaction, request.TableName, gsiDefs, updatedAttrDefs);
                    await transaction.CommitAsync(cancellationToken);
                }
                else if (update.Delete is not null)
                {
                    var toRemove = gsiDefs.FirstOrDefault(g => g.IndexName == update.Delete.IndexName)
                        ?? throw new AmazonDynamoDBException(
                            $"The table does not have the specified index: {update.Delete.IndexName}");

                    _ = gsiDefs.Remove(toRemove);

                    using var connection = await store.OpenConnectionAsync(cancellationToken);
                    using var transaction = await connection.BeginTransactionAsync(cancellationToken);

                    await SqliteStore.DropIndexTableAsync(connection, request.TableName, update.Delete.IndexName, transaction);
                    await store.UpdateIndexMetadataInTransactionAsync(
                        connection, transaction, request.TableName, gsiDefs);
                    await transaction.CommitAsync(cancellationToken);
                }
                // Update (throughput changes) → no-op for local emulator
            }

            description = await store.GetTableDescriptionAsync(request.TableName, cancellationToken);
        }

        return new UpdateTableResponse
        {
            TableDescription = description,
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }
}
