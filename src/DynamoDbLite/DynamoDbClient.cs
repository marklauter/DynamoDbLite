using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace DynamoDbLite;

public sealed class DynamoDbClient(DynamoDbLiteOptions? options = null)
    : DynamoDbService
    , IAmazonDynamoDB
    , IAmazonService
    , IDisposable
{
    private const int DefaultListTablesLimit = 100;

    private readonly SqliteStore store = new(options ?? new DynamoDbLiteOptions());
    private bool disposed;

    public IDynamoDBv2PaginatorFactory? Paginators { get; }

    // ── Table Management ───────────────────────────────────────────────

    public Task<CreateTableResponse> CreateTableAsync(
        string tableName,
        List<KeySchemaElement> keySchema,
        List<AttributeDefinition> attributeDefinitions,
        ProvisionedThroughput provisionedThroughput,
        CancellationToken cancellationToken = default) =>
        CreateTableAsync(new CreateTableRequest
        {
            TableName = tableName,
            KeySchema = keySchema,
            AttributeDefinitions = attributeDefinitions,
            ProvisionedThroughput = provisionedThroughput
        }, cancellationToken);

    public async Task<CreateTableResponse> CreateTableAsync(
        CreateTableRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableName);

        ValidateKeySchema(request.KeySchema, request.AttributeDefinitions);

        if (await store.TableExistsAsync(request.TableName, cancellationToken))
            throw new ResourceInUseException($"Table already exists: {request.TableName}");

        await store.CreateTableAsync(
            request.TableName,
            request.KeySchema,
            request.AttributeDefinitions,
            request.ProvisionedThroughput,
            cancellationToken);

        var description = await store.GetTableDescriptionAsync(request.TableName, cancellationToken);

        return new CreateTableResponse
        {
            TableDescription = description,
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }

    public Task<DeleteTableResponse> DeleteTableAsync(
        string tableName,
        CancellationToken cancellationToken = default) =>
        DeleteTableAsync(new DeleteTableRequest { TableName = tableName }, cancellationToken);

    public async Task<DeleteTableResponse> DeleteTableAsync(
        DeleteTableRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableName);

        var description = await store.GetTableDescriptionAsync(request.TableName, cancellationToken)
            ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        await store.DeleteTableAsync(request.TableName, cancellationToken);

        description.TableStatus = TableStatus.DELETING;

        return new DeleteTableResponse
        {
            TableDescription = description,
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }

    public Task<DescribeTableResponse> DescribeTableAsync(
        string tableName,
        CancellationToken cancellationToken = default) =>
        DescribeTableAsync(new DescribeTableRequest { TableName = tableName }, cancellationToken);

    public async Task<DescribeTableResponse> DescribeTableAsync(
        DescribeTableRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableName);

        var description = await store.GetTableDescriptionAsync(request.TableName, cancellationToken)
            ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        return new DescribeTableResponse
        {
            Table = description,
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }

    public Task<ListTablesResponse> ListTablesAsync(
        CancellationToken cancellationToken = default) =>
        ListTablesAsync(new ListTablesRequest(), cancellationToken);

    public Task<ListTablesResponse> ListTablesAsync(
        string exclusiveStartTableName,
        CancellationToken cancellationToken = default) =>
        ListTablesAsync(new ListTablesRequest
        {
            ExclusiveStartTableName = exclusiveStartTableName
        }, cancellationToken);

    public Task<ListTablesResponse> ListTablesAsync(
        string exclusiveStartTableName,
        int? limit,
        CancellationToken cancellationToken = default) =>
        ListTablesAsync(new ListTablesRequest
        {
            ExclusiveStartTableName = exclusiveStartTableName,
            Limit = limit ?? DefaultListTablesLimit
        }, cancellationToken);

    public Task<ListTablesResponse> ListTablesAsync(
        int? limit,
        CancellationToken cancellationToken = default) =>
        ListTablesAsync(new ListTablesRequest { Limit = limit ?? DefaultListTablesLimit }, cancellationToken);

    public async Task<ListTablesResponse> ListTablesAsync(
        ListTablesRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);

        var limit = request.Limit is > 0 ? request.Limit.Value : DefaultListTablesLimit;
        var fetchLimit = limit + 1;

        var names = await store.ListTableNamesAsync(
            string.IsNullOrEmpty(request.ExclusiveStartTableName)
                ? null
                : request.ExclusiveStartTableName,
            fetchLimit,
            cancellationToken);

        string? lastEvaluatedTableName = null;
        if (names.Count > limit)
        {
            lastEvaluatedTableName = names[limit - 1];
            names = names.GetRange(0, limit);
        }

        return new ListTablesResponse
        {
            TableNames = names,
            LastEvaluatedTableName = lastEvaluatedTableName,
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }

    // ── Item CRUD ──────────────────────────────────────────────────────

    public Task<PutItemResponse> PutItemAsync(
        string tableName,
        Dictionary<string, AttributeValue> item,
        CancellationToken cancellationToken = default) =>
        PutItemAsync(new PutItemRequest { TableName = tableName, Item = item }, cancellationToken);

    public Task<PutItemResponse> PutItemAsync(
        string tableName,
        Dictionary<string, AttributeValue> item,
        ReturnValue returnValues,
        CancellationToken cancellationToken = default) =>
        PutItemAsync(new PutItemRequest { TableName = tableName, Item = item, ReturnValues = returnValues }, cancellationToken);

    public async Task<PutItemResponse> PutItemAsync(
        PutItemRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableName);
        ArgumentNullException.ThrowIfNull(request.Item);

        var keyInfo = await store.GetKeySchemaAsync(request.TableName, cancellationToken)
            ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        KeyHelper.ValidateKeyTypes(request.Item, keyInfo.KeySchema, keyInfo.AttributeDefinitions);
        var (pk, sk) = KeyHelper.ExtractKeys(request.Item, keyInfo.KeySchema, keyInfo.AttributeDefinitions);
        var itemJson = AttributeValueSerializer.Serialize(request.Item);

        // Condition expression check
        if (!string.IsNullOrEmpty(request.ConditionExpression))
        {
            var existingJson = await store.GetItemAsync(request.TableName, pk, sk, cancellationToken);
            var existingItem = existingJson is not null
                ? AttributeValueSerializer.Deserialize(existingJson)
                : null;

            var conditionAst = Expressions.ConditionExpressionParser.Parse(request.ConditionExpression);
            var conditionResult = Expressions.ConditionExpressionEvaluator.Evaluate(
                conditionAst, existingItem, request.ExpressionAttributeNames, request.ExpressionAttributeValues);

            if (!conditionResult)
                throw new ConditionalCheckFailedException("The conditional request failed");
        }

        var oldJson = await store.PutItemAsync(request.TableName, pk, sk, itemJson, cancellationToken);

        var response = new PutItemResponse { HttpStatusCode = System.Net.HttpStatusCode.OK };

        if (request.ReturnValues == ReturnValue.ALL_OLD && oldJson is not null)
            response.Attributes = AttributeValueSerializer.Deserialize(oldJson);

        return response;
    }

    public Task<GetItemResponse> GetItemAsync(
        string tableName,
        Dictionary<string, AttributeValue> key,
        CancellationToken cancellationToken = default) =>
        GetItemAsync(new GetItemRequest { TableName = tableName, Key = key }, cancellationToken);

    public Task<GetItemResponse> GetItemAsync(
        string tableName,
        Dictionary<string, AttributeValue> key,
        bool? consistentRead,
        CancellationToken cancellationToken = default) =>
        GetItemAsync(new GetItemRequest { TableName = tableName, Key = key, ConsistentRead = consistentRead }, cancellationToken);

    public async Task<GetItemResponse> GetItemAsync(
        GetItemRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableName);
        ArgumentNullException.ThrowIfNull(request.Key);

        var keyInfo = await store.GetKeySchemaAsync(request.TableName, cancellationToken)
            ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        var (pk, sk) = KeyHelper.ExtractKeys(request.Key, keyInfo.KeySchema, keyInfo.AttributeDefinitions);
        var itemJson = await store.GetItemAsync(request.TableName, pk, sk, cancellationToken);

        var response = new GetItemResponse { HttpStatusCode = System.Net.HttpStatusCode.OK };

        if (itemJson is not null)
        {
            var item = AttributeValueSerializer.Deserialize(itemJson);

            if (!string.IsNullOrEmpty(request.ProjectionExpression))
            {
                var paths = Expressions.ProjectionExpressionParser.Parse(
                    request.ProjectionExpression, request.ExpressionAttributeNames);
                item = Expressions.ProjectionExpressionEvaluator.Apply(item, paths);
            }

            response.Item = item;
            response.IsItemSet = true;
        }

        return response;
    }

    public Task<DeleteItemResponse> DeleteItemAsync(
        string tableName,
        Dictionary<string, AttributeValue> key,
        CancellationToken cancellationToken = default) =>
        DeleteItemAsync(new DeleteItemRequest { TableName = tableName, Key = key }, cancellationToken);

    public Task<DeleteItemResponse> DeleteItemAsync(
        string tableName,
        Dictionary<string, AttributeValue> key,
        ReturnValue returnValues,
        CancellationToken cancellationToken = default) =>
        DeleteItemAsync(new DeleteItemRequest { TableName = tableName, Key = key, ReturnValues = returnValues }, cancellationToken);

    public async Task<DeleteItemResponse> DeleteItemAsync(
        DeleteItemRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableName);
        ArgumentNullException.ThrowIfNull(request.Key);

        var keyInfo = await store.GetKeySchemaAsync(request.TableName, cancellationToken)
            ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        var (pk, sk) = KeyHelper.ExtractKeys(request.Key, keyInfo.KeySchema, keyInfo.AttributeDefinitions);

        // Condition expression check
        if (!string.IsNullOrEmpty(request.ConditionExpression))
        {
            var existingJson = await store.GetItemAsync(request.TableName, pk, sk, cancellationToken);
            var existingItem = existingJson is not null
                ? AttributeValueSerializer.Deserialize(existingJson)
                : null;

            var conditionAst = Expressions.ConditionExpressionParser.Parse(request.ConditionExpression);
            var conditionResult = Expressions.ConditionExpressionEvaluator.Evaluate(
                conditionAst, existingItem, request.ExpressionAttributeNames, request.ExpressionAttributeValues);

            if (!conditionResult)
                throw new ConditionalCheckFailedException("The conditional request failed");
        }

        var oldJson = await store.DeleteItemAsync(request.TableName, pk, sk, cancellationToken);

        var response = new DeleteItemResponse { HttpStatusCode = System.Net.HttpStatusCode.OK };

        if (request.ReturnValues == ReturnValue.ALL_OLD && oldJson is not null)
            response.Attributes = AttributeValueSerializer.Deserialize(oldJson);

        return response;
    }

    public async Task<UpdateItemResponse> UpdateItemAsync(
        UpdateItemRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableName);
        ArgumentNullException.ThrowIfNull(request.Key);

        var keyInfo = await store.GetKeySchemaAsync(request.TableName, cancellationToken)
            ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        var (pk, sk) = KeyHelper.ExtractKeys(request.Key, keyInfo.KeySchema, keyInfo.AttributeDefinitions);
        var existingJson = await store.GetItemAsync(request.TableName, pk, sk, cancellationToken);
        var existingItem = existingJson is not null
            ? AttributeValueSerializer.Deserialize(existingJson)
            : new Dictionary<string, AttributeValue>(request.Key);

        // Condition expression check
        if (!string.IsNullOrEmpty(request.ConditionExpression))
        {
            var conditionAst = Expressions.ConditionExpressionParser.Parse(request.ConditionExpression);
            var itemForCondition = existingJson is not null
                ? AttributeValueSerializer.Deserialize(existingJson)
                : null;
            var conditionResult = Expressions.ConditionExpressionEvaluator.Evaluate(
                conditionAst, itemForCondition, request.ExpressionAttributeNames, request.ExpressionAttributeValues);

            if (!conditionResult)
                throw new ConditionalCheckFailedException("The conditional request failed");
        }

        var oldItem = existingJson is not null
            ? AttributeValueSerializer.Deserialize(existingJson)
            : null;

        Dictionary<string, AttributeValue>? updatedOldAttrs = null;
        Dictionary<string, AttributeValue>? updatedNewAttrs = null;

        if (!string.IsNullOrEmpty(request.UpdateExpression))
        {
            var updateAst = Expressions.UpdateExpressionParser.Parse(request.UpdateExpression);
            var (newItem, modifiedKeys) = Expressions.UpdateExpressionEvaluator.Apply(
                updateAst, existingItem, request.ExpressionAttributeNames, request.ExpressionAttributeValues);

            // Validate key attributes are unchanged
            foreach (var key in keyInfo.KeySchema)
            {
                if (modifiedKeys.Contains(key.AttributeName))
                    throw new AmazonDynamoDBException(
                        $"One or more parameter values were invalid: Cannot update attribute {key.AttributeName}. This attribute is part of the key");
            }

            if (request.ReturnValues == ReturnValue.UPDATED_OLD && oldItem is not null)
            {
                updatedOldAttrs = new Dictionary<string, AttributeValue>();
                foreach (var k in modifiedKeys)
                    if (oldItem.TryGetValue(k, out var v))
                        updatedOldAttrs[k] = v;
            }

            if (request.ReturnValues == ReturnValue.UPDATED_NEW)
            {
                updatedNewAttrs = new Dictionary<string, AttributeValue>();
                foreach (var k in modifiedKeys)
                    if (newItem.TryGetValue(k, out var v))
                        updatedNewAttrs[k] = v;
            }

            existingItem = newItem;
        }

        var itemJson = AttributeValueSerializer.Serialize(existingItem);
        _ = await store.PutItemAsync(request.TableName, pk, sk, itemJson, cancellationToken);

        var response = new UpdateItemResponse { HttpStatusCode = System.Net.HttpStatusCode.OK };

        if (request.ReturnValues == ReturnValue.ALL_OLD && oldItem is not null)
            response.Attributes = oldItem;
        else if (request.ReturnValues == ReturnValue.ALL_NEW)
            response.Attributes = existingItem;
        else if (request.ReturnValues == ReturnValue.UPDATED_OLD && updatedOldAttrs is not null)
            response.Attributes = updatedOldAttrs;
        else if (request.ReturnValues == ReturnValue.UPDATED_NEW && updatedNewAttrs is not null)
            response.Attributes = updatedNewAttrs;

        return response;
    }

    public Task<UpdateItemResponse> UpdateItemAsync(
        string tableName,
        Dictionary<string, AttributeValue> key,
        Dictionary<string, AttributeValueUpdate> attributeUpdates,
        CancellationToken cancellationToken = default) =>
        throw new NotImplementedException("Legacy AttributeValueUpdate API is not supported. Use UpdateExpression instead.");

    public Task<UpdateItemResponse> UpdateItemAsync(
        string tableName,
        Dictionary<string, AttributeValue> key,
        Dictionary<string, AttributeValueUpdate> attributeUpdates,
        ReturnValue returnValues,
        CancellationToken cancellationToken = default) =>
        throw new NotImplementedException("Legacy AttributeValueUpdate API is not supported. Use UpdateExpression instead.");

    // ── Not Yet Implemented ────────────────────────────────────────────

    public Task<BatchExecuteStatementResponse> BatchExecuteStatementAsync(BatchExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, ReturnConsumedCapacity returnConsumedCapacity, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<BatchGetItemResponse> BatchGetItemAsync(BatchGetItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<BatchWriteItemResponse> BatchWriteItemAsync(Dictionary<string, List<WriteRequest>> requestItems, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<BatchWriteItemResponse> BatchWriteItemAsync(BatchWriteItemRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<CreateBackupResponse> CreateBackupAsync(CreateBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<CreateGlobalTableResponse> CreateGlobalTableAsync(CreateGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DeleteBackupResponse> DeleteBackupAsync(DeleteBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DeleteResourcePolicyResponse> DeleteResourcePolicyAsync(DeleteResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeBackupResponse> DescribeBackupAsync(DescribeBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeContinuousBackupsResponse> DescribeContinuousBackupsAsync(DescribeContinuousBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeContributorInsightsResponse> DescribeContributorInsightsAsync(DescribeContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeEndpointsResponse> DescribeEndpointsAsync(DescribeEndpointsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeExportResponse> DescribeExportAsync(DescribeExportRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeGlobalTableResponse> DescribeGlobalTableAsync(DescribeGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeGlobalTableSettingsResponse> DescribeGlobalTableSettingsAsync(DescribeGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeImportResponse> DescribeImportAsync(DescribeImportRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeKinesisStreamingDestinationResponse> DescribeKinesisStreamingDestinationAsync(DescribeKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeLimitsResponse> DescribeLimitsAsync(DescribeLimitsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeTableReplicaAutoScalingResponse> DescribeTableReplicaAutoScalingAsync(DescribeTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(DescribeTimeToLiveRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Amazon.Runtime.Endpoints.Endpoint DetermineServiceOperationEndpoint(AmazonWebServiceRequest request) => throw new NotImplementedException();
    public Task<DisableKinesisStreamingDestinationResponse> DisableKinesisStreamingDestinationAsync(DisableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<EnableKinesisStreamingDestinationResponse> EnableKinesisStreamingDestinationAsync(EnableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ExecuteTransactionResponse> ExecuteTransactionAsync(ExecuteTransactionRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ExportTableToPointInTimeResponse> ExportTableToPointInTimeAsync(ExportTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<GetResourcePolicyResponse> GetResourcePolicyAsync(GetResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ImportTableResponse> ImportTableAsync(ImportTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ListBackupsResponse> ListBackupsAsync(ListBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ListContributorInsightsResponse> ListContributorInsightsAsync(ListContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ListExportsResponse> ListExportsAsync(ListExportsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ListGlobalTablesResponse> ListGlobalTablesAsync(ListGlobalTablesRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ListImportsResponse> ListImportsAsync(ListImportsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ListTagsOfResourceResponse> ListTagsOfResourceAsync(ListTagsOfResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<PutResourcePolicyResponse> PutResourcePolicyAsync(PutResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<QueryResponse> QueryAsync(QueryRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<RestoreTableFromBackupResponse> RestoreTableFromBackupAsync(RestoreTableFromBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<RestoreTableToPointInTimeResponse> RestoreTableToPointInTimeAsync(RestoreTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ScanResponse> ScanAsync(string tableName, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ScanResponse> ScanAsync(ScanRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<TagResourceResponse> TagResourceAsync(TagResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<TransactWriteItemsResponse> TransactWriteItemsAsync(TransactWriteItemsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<UntagResourceResponse> UntagResourceAsync(UntagResourceRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<UpdateContinuousBackupsResponse> UpdateContinuousBackupsAsync(UpdateContinuousBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<UpdateContributorInsightsResponse> UpdateContributorInsightsAsync(UpdateContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<UpdateGlobalTableResponse> UpdateGlobalTableAsync(UpdateGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<UpdateGlobalTableSettingsResponse> UpdateGlobalTableSettingsAsync(UpdateGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<UpdateKinesisStreamingDestinationResponse> UpdateKinesisStreamingDestinationAsync(UpdateKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<UpdateTableResponse> UpdateTableAsync(string tableName, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<UpdateTableResponse> UpdateTableAsync(UpdateTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<UpdateTableReplicaAutoScalingResponse> UpdateTableReplicaAutoScalingAsync(UpdateTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<UpdateTimeToLiveResponse> UpdateTimeToLiveAsync(UpdateTimeToLiveRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();

    // ── Lifecycle ──────────────────────────────────────────────────────

    public void Dispose()
    {
        if (disposed)
            return;

        store.Dispose();
        disposed = true;
    }

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(disposed, this);

    // ── Validation ─────────────────────────────────────────────────────

    private static void ValidateKeySchema(
        List<KeySchemaElement> keySchema,
        List<AttributeDefinition> attributeDefinitions)
    {
        ArgumentNullException.ThrowIfNull(keySchema);
        ArgumentNullException.ThrowIfNull(attributeDefinitions);

        if (keySchema.Count is 0 or > 2)
            throw new AmazonDynamoDBException(
                "1 or 2 key schema elements are required.");

        var hashKeys = keySchema.Where(static k => k.KeyType == KeyType.HASH).ToList();
        if (hashKeys.Count is not 1)
            throw new AmazonDynamoDBException(
                "Exactly one HASH key is required in the key schema.");

        var rangeKeys = keySchema.Where(static k => k.KeyType == KeyType.RANGE).ToList();
        if (rangeKeys.Count > 1)
            throw new AmazonDynamoDBException(
                "At most one RANGE key is allowed in the key schema.");

        var keyAttributeNames = keySchema.Select(static k => k.AttributeName).ToHashSet();
        var definedAttributeNames = attributeDefinitions.Select(static a => a.AttributeName).ToHashSet();

        if (!keyAttributeNames.IsSubsetOf(definedAttributeNames))
        {
            var missing = keyAttributeNames.Except(definedAttributeNames);
            throw new AmazonDynamoDBException(
                $"Key schema attribute(s) not defined in attribute definitions: {string.Join(", ", missing)}");
        }
    }
}
