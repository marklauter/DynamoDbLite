using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Expressions;
using DynamoDbLite.SqliteStores;
using DynamoDbLite.SqliteStores.Models;
using System.Net;

namespace DynamoDbLite;

public sealed partial class DynamoDbClient
{
    /// <inheritdoc/>
    public Task<BatchGetItemResponse> BatchGetItemAsync(
        Dictionary<string, KeysAndAttributes> requestItems,
        ReturnConsumedCapacity returnConsumedCapacity,
        CancellationToken cancellationToken = default) =>
        BatchGetItemAsync(new BatchGetItemRequest { RequestItems = requestItems, ReturnConsumedCapacity = returnConsumedCapacity }, cancellationToken);

    /// <inheritdoc/>
    public Task<BatchGetItemResponse> BatchGetItemAsync(
        Dictionary<string, KeysAndAttributes> requestItems,
        CancellationToken cancellationToken = default) =>
        BatchGetItemAsync(new BatchGetItemRequest { RequestItems = requestItems }, cancellationToken);

    /// <inheritdoc/>
    public async Task<BatchGetItemResponse> BatchGetItemAsync(
        BatchGetItemRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.RequestItems);

        if (request.RequestItems.Count == 0)
            throw new AmazonDynamoDBException("1 validation error detected: Value at 'requestItems' failed to satisfy constraint: Member must have length greater than or equal to 1");

        var totalKeys = request.RequestItems.Values.Sum(static ka => ka.Keys.Count);
        if (totalKeys > 100)
            throw new AmazonDynamoDBException("Too many items requested for the BatchGetItem call");

        var nowEpoch = SqliteStore.NowEpoch();

        var allKeys = new List<(string TableName, string Pk, string Sk)>(totalKeys);
        var keyInfoByTable = new Dictionary<string, KeySchemaInfo>();
        var projectionPathsByTable = new Dictionary<string, IReadOnlyList<Expressions.AttributePath>>();

        foreach (var (tableName, keysAndAttributes) in request.RequestItems)
        {
            var keyInfo = await store.GetKeySchemaAsync(tableName, cancellationToken)
                ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {tableName} not found");

            keyInfoByTable[tableName] = keyInfo;

            // Parse the projection expression upfront so a malformed expression throws ValidationException
            // before any item lookup, matching GetItem behavior and real DynamoDB's validation order.
            if (!string.IsNullOrEmpty(keysAndAttributes.ProjectionExpression))
                projectionPathsByTable[tableName] = Expressions.ProjectionExpressionParser.Parse(
                    keysAndAttributes.ProjectionExpression, keysAndAttributes.ExpressionAttributeNames);

            foreach (var key in keysAndAttributes.Keys)
            {
                var (pk, sk) = KeyHelper.ExtractKeys(key, keyInfo.KeySchema, keyInfo.AttributeDefinitions);
                allKeys.Add((tableName, pk, sk));
            }
        }

        var results = await store.BatchGetItemsAsync(allKeys, nowEpoch, cancellationToken);

        foreach (var tableName in request.RequestItems.Keys)
            TriggerBackgroundCleanup(tableName);

        var responsesByTable = new Dictionary<string, List<Dictionary<string, AttributeValue>>>();
        foreach (var (tableName, itemJson) in results)
        {
            if (!responsesByTable.TryGetValue(tableName, out var list))
            {
                list = [];
                responsesByTable[tableName] = list;
            }

            var item = AttributeValueSerializer.Deserialize(itemJson);

            if (projectionPathsByTable.TryGetValue(tableName, out var paths))
                item = Expressions.ProjectionExpressionEvaluator.Apply(item, paths);

            list.Add(item);
        }

        return new BatchGetItemResponse
        {
            Responses = responsesByTable,
            UnprocessedKeys = [],
            HttpStatusCode = HttpStatusCode.OK
        };
    }

    /// <inheritdoc/>
    public Task<BatchWriteItemResponse> BatchWriteItemAsync(
        Dictionary<string, List<WriteRequest>> requestItems,
        CancellationToken cancellationToken = default) =>
        BatchWriteItemAsync(new BatchWriteItemRequest { RequestItems = requestItems }, cancellationToken);

    /// <inheritdoc/>
    public async Task<BatchWriteItemResponse> BatchWriteItemAsync(
        BatchWriteItemRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.RequestItems);

        if (request.RequestItems.Count == 0)
            throw new AmazonDynamoDBException("1 validation error detected: Value at 'requestItems' failed to satisfy constraint: Member must have length greater than or equal to 1");

        var totalOps = request.RequestItems.Values.Sum(static writes => writes.Count);
        if (totalOps > maxBatchWriteItems)
            throw new AmazonDynamoDBException("Too many items requested for the BatchWriteItem call");

        var seenKeys = new HashSet<(string, string, string)>(totalOps);
        var operations = new List<BatchWriteOperation>(totalOps);
        Dictionary<string, (List<IndexDefinition> Indexes, List<AttributeDefinition> AttrDefs)>? indexInfoByTable = null;

        foreach (var (tableName, writeRequests) in request.RequestItems)
        {
            var metadata = store.GetBatchWriteMetadata(tableName)
                ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {tableName} not found");

            var keyInfo = metadata.KeyInfo;
            var ttlAttr = metadata.TtlAttributeName;

            foreach (var writeRequest in writeRequests)
            {
                if (writeRequest.PutRequest is not null)
                {
                    KeyHelper.ValidateKeyTypes(writeRequest.PutRequest.Item, keyInfo.KeySchema, keyInfo.AttributeDefinitions);
                    var (pk, sk) = KeyHelper.ExtractKeys(writeRequest.PutRequest.Item, keyInfo.KeySchema, keyInfo.AttributeDefinitions);

                    if (!seenKeys.Add((tableName, pk, sk)))
                        throw new AmazonDynamoDBException("Provided list of item keys contains duplicates");

                    var itemJson = AttributeValueSerializer.Serialize(writeRequest.PutRequest.Item);
                    var skNum = ComputeSkNum(sk, keyInfo);
                    double? ttlEpoch = ttlAttr is not null && TtlEpochParser.TryParse(writeRequest.PutRequest.Item, ttlAttr, out var epoch) ? epoch : null;
                    operations.Add(new BatchWriteOperation(tableName, pk, sk, skNum, ttlEpoch, itemJson));
                }
                else if (writeRequest.DeleteRequest is not null)
                {
                    var (pk, sk) = KeyHelper.ExtractKeys(writeRequest.DeleteRequest.Key, keyInfo.KeySchema, keyInfo.AttributeDefinitions);

                    if (!seenKeys.Add((tableName, pk, sk)))
                        throw new AmazonDynamoDBException("Provided list of item keys contains duplicates");

                    operations.Add(new BatchWriteOperation(tableName, pk, sk, null, null, null));
                }
            }

            if (metadata.Indexes.Count > 0)
            {
                indexInfoByTable ??= [];
                indexInfoByTable[tableName] = (metadata.Indexes, keyInfo.AttributeDefinitions);
            }
        }

        await store.BatchWriteItemsAsync(operations, indexInfoByTable, cancellationToken);

        return new BatchWriteItemResponse
        {
            UnprocessedItems = [],
            HttpStatusCode = HttpStatusCode.OK
        };
    }
}
