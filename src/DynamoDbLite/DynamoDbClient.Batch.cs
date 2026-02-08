using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using System.Net;

namespace DynamoDbLite;

public sealed partial class DynamoDbClient
{
    public Task<BatchExecuteStatementResponse> BatchExecuteStatementAsync(BatchExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();

    public Task<BatchGetItemResponse> BatchGetItemAsync(
        Dictionary<string, KeysAndAttributes> requestItems,
        ReturnConsumedCapacity returnConsumedCapacity,
        CancellationToken cancellationToken = default) =>
        BatchGetItemAsync(new BatchGetItemRequest { RequestItems = requestItems, ReturnConsumedCapacity = returnConsumedCapacity }, cancellationToken);

    public Task<BatchGetItemResponse> BatchGetItemAsync(
        Dictionary<string, KeysAndAttributes> requestItems,
        CancellationToken cancellationToken = default) =>
        BatchGetItemAsync(new BatchGetItemRequest { RequestItems = requestItems }, cancellationToken);

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

        var allKeys = new List<(string TableName, string Pk, string Sk)>(totalKeys);
        var keyInfoByTable = new Dictionary<string, KeySchemaInfo>();

        foreach (var (tableName, keysAndAttributes) in request.RequestItems)
        {
            var keyInfo = await store.GetKeySchemaAsync(tableName, cancellationToken)
                ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {tableName} not found");

            keyInfoByTable[tableName] = keyInfo;

            foreach (var key in keysAndAttributes.Keys)
            {
                var (pk, sk) = KeyHelper.ExtractKeys(key, keyInfo.KeySchema, keyInfo.AttributeDefinitions);
                allKeys.Add((tableName, pk, sk));
            }
        }

        var results = await store.BatchGetItemsAsync(allKeys, cancellationToken);

        var responsesByTable = new Dictionary<string, List<Dictionary<string, AttributeValue>>>();
        foreach (var (tableName, itemJson) in results)
        {
            if (!responsesByTable.TryGetValue(tableName, out var list))
            {
                list = [];
                responsesByTable[tableName] = list;
            }

            var item = AttributeValueSerializer.Deserialize(itemJson);

            if (request.RequestItems.TryGetValue(tableName, out var ka)
                && !string.IsNullOrEmpty(ka.ProjectionExpression))
            {
                var paths = Expressions.ProjectionExpressionParser.Parse(
                    ka.ProjectionExpression, ka.ExpressionAttributeNames);
                item = Expressions.ProjectionExpressionEvaluator.Apply(item, paths);
            }

            list.Add(item);
        }

        return new BatchGetItemResponse
        {
            Responses = responsesByTable,
            UnprocessedKeys = [],
            HttpStatusCode = HttpStatusCode.OK
        };
    }

    public Task<BatchWriteItemResponse> BatchWriteItemAsync(
        Dictionary<string, List<WriteRequest>> requestItems,
        CancellationToken cancellationToken = default) =>
        BatchWriteItemAsync(new BatchWriteItemRequest { RequestItems = requestItems }, cancellationToken);

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
        if (totalOps > 25)
            throw new AmazonDynamoDBException("Too many items requested for the BatchWriteItem call");

        var seenKeys = new HashSet<(string, string, string)>();
        var operations = new List<BatchWriteOperation>(totalOps);

        foreach (var (tableName, writeRequests) in request.RequestItems)
        {
            var keyInfo = await store.GetKeySchemaAsync(tableName, cancellationToken)
                ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {tableName} not found");

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
                    operations.Add(new BatchWriteOperation(tableName, pk, sk, skNum, itemJson));
                }
                else if (writeRequest.DeleteRequest is not null)
                {
                    var (pk, sk) = KeyHelper.ExtractKeys(writeRequest.DeleteRequest.Key, keyInfo.KeySchema, keyInfo.AttributeDefinitions);

                    if (!seenKeys.Add((tableName, pk, sk)))
                        throw new AmazonDynamoDBException("Provided list of item keys contains duplicates");

                    operations.Add(new BatchWriteOperation(tableName, pk, sk, null, null));
                }
            }
        }

        // Load index info for tables that have indexes
        Dictionary<string, (List<IndexDefinition> Indexes, List<AttributeDefinition> AttrDefs)>? indexInfoByTable = null;
        foreach (var (tableName, _) in request.RequestItems)
        {
            if (indexInfoByTable?.ContainsKey(tableName) is true)
                continue;

            var keyInfo = await store.GetKeySchemaAsync(tableName, cancellationToken)
                ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {tableName} not found");
            var indexes = await store.GetIndexDefinitionsAsync(tableName, cancellationToken);

            if (indexes.Count > 0)
            {
                indexInfoByTable ??= [];
                indexInfoByTable[tableName] = (indexes, keyInfo.AttributeDefinitions);
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
