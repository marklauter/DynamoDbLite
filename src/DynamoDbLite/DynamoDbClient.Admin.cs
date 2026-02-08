using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using System.Globalization;

namespace DynamoDbLite;

// ── Administrative Operations (Not Yet Implemented) ─────────────────
public sealed partial class DynamoDbClient
{
    // ── Tags ─────────────────────────────────────────────────────────
    public Task<ListTagsOfResourceResponse> ListTagsOfResourceAsync(ListTagsOfResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<TagResourceResponse> TagResourceAsync(TagResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<UntagResourceResponse> UntagResourceAsync(UntagResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();

    // ── Resource Policy ──────────────────────────────────────────────
    public Task<DeleteResourcePolicyResponse> DeleteResourcePolicyAsync(DeleteResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<GetResourcePolicyResponse> GetResourcePolicyAsync(GetResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<PutResourcePolicyResponse> PutResourcePolicyAsync(PutResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();

    // ── Contributor Insights ─────────────────────────────────────────
    public Task<DescribeContributorInsightsResponse> DescribeContributorInsightsAsync(DescribeContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ListContributorInsightsResponse> ListContributorInsightsAsync(ListContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<UpdateContributorInsightsResponse> UpdateContributorInsightsAsync(UpdateContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();

    // ── Time To Live ─────────────────────────────────────────────────
    public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(DescribeTimeToLiveRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<UpdateTimeToLiveResponse> UpdateTimeToLiveAsync(UpdateTimeToLiveRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();

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

                    // Read existing items before starting transaction to avoid schema lock
                    var existingItems = await store.GetAllItemsAsync(request.TableName, cancellationToken);

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
                            var skNum = ComputeIndexSkNum(keys.Value.Sk, newIdx.KeySchema, attrDefs);
                            await SqliteStore.UpsertIndexEntryAsync(
                                connection, transaction, request.TableName, newIdx.IndexName,
                                keys.Value.Pk, keys.Value.Sk, skNum,
                                existingRow.Pk, existingRow.Sk, existingRow.ItemJson);
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

    private static double? ComputeIndexSkNum(
        string sk,
        List<KeySchemaElement> keySchema,
        List<AttributeDefinition> attributeDefinitions)
    {
        var rangeKey = keySchema.FirstOrDefault(static k => k.KeyType == KeyType.RANGE);
        if (rangeKey is null)
            return null;

        var attrDef = attributeDefinitions.First(a => a.AttributeName == rangeKey.AttributeName);
        return attrDef.AttributeType == ScalarAttributeType.N
            ? double.Parse(sk, CultureInfo.InvariantCulture)
            : null;
    }
}
