using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.SqliteStores;
using DynamoDbLite.SqliteStores.Models;
using System.Globalization;

namespace DynamoDbLite;

public sealed partial class DynamoDbClient
{
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

        var nowEpoch = SqliteStore.NowEpoch();

        var keyInfo = await store.GetKeySchemaAsync(request.TableName, cancellationToken)
            ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        KeyHelper.ValidateKeyTypes(request.Item, keyInfo.KeySchema, keyInfo.AttributeDefinitions);
        var (pk, sk) = KeyHelper.ExtractKeys(request.Item, keyInfo.KeySchema, keyInfo.AttributeDefinitions);
        var itemJson = AttributeValueSerializer.Serialize(request.Item);

        // Condition expression check
        if (!string.IsNullOrEmpty(request.ConditionExpression))
        {
            var existingJson = await store.GetItemAsync(request.TableName, pk, sk, nowEpoch, cancellationToken);
            var existingItem = existingJson is not null
                ? AttributeValueSerializer.Deserialize(existingJson)
                : null;

            var conditionAst = Expressions.ConditionExpressionParser.Parse(request.ConditionExpression);
            var conditionResult = Expressions.ConditionExpressionEvaluator.Evaluate(
                conditionAst, existingItem, request.ExpressionAttributeNames, request.ExpressionAttributeValues);

            if (!conditionResult)
                throw new ConditionalCheckFailedException("The conditional request failed");
        }

        var skNum = ComputeSkNum(sk, keyInfo);

        var ttlAttr = await store.GetTtlAttributeNameAsync(request.TableName, cancellationToken);
        double? ttlEpoch = ttlAttr is not null && TtlEpochParser.TryParse(request.Item, ttlAttr, out var epoch) ? epoch : null;

        var indexes = await store.GetIndexDefinitionsAsync(request.TableName, cancellationToken);
        var oldJson = indexes.Count > 0
            ? await store.PutItemWithIndexesAsync(
                request.TableName, pk, sk, itemJson, skNum, ttlEpoch, nowEpoch,
                indexes, keyInfo.AttributeDefinitions, request.Item, cancellationToken)
            : await store.PutItemAsync(request.TableName, pk, sk, itemJson, skNum, ttlEpoch, nowEpoch, cancellationToken);

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

        var nowEpoch = SqliteStore.NowEpoch();

        var keyInfo = await store.GetKeySchemaAsync(request.TableName, cancellationToken)
            ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        var (pk, sk) = KeyHelper.ExtractKeys(request.Key, keyInfo.KeySchema, keyInfo.AttributeDefinitions);
        var itemJson = await store.GetItemAsync(request.TableName, pk, sk, nowEpoch, cancellationToken);

        TriggerBackgroundCleanup(request.TableName);

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

        var nowEpoch = SqliteStore.NowEpoch();

        var keyInfo = await store.GetKeySchemaAsync(request.TableName, cancellationToken)
            ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        var (pk, sk) = KeyHelper.ExtractKeys(request.Key, keyInfo.KeySchema, keyInfo.AttributeDefinitions);

        // Condition expression check
        if (!string.IsNullOrEmpty(request.ConditionExpression))
        {
            var existingJson = await store.GetItemAsync(request.TableName, pk, sk, nowEpoch, cancellationToken);
            var existingItem = existingJson is not null
                ? AttributeValueSerializer.Deserialize(existingJson)
                : null;

            var conditionAst = Expressions.ConditionExpressionParser.Parse(request.ConditionExpression);
            var conditionResult = Expressions.ConditionExpressionEvaluator.Evaluate(
                conditionAst, existingItem, request.ExpressionAttributeNames, request.ExpressionAttributeValues);

            if (!conditionResult)
                throw new ConditionalCheckFailedException("The conditional request failed");
        }

        var indexes = await store.GetIndexDefinitionsAsync(request.TableName, cancellationToken);
        var oldJson = indexes.Count > 0
            ? await store.DeleteItemWithIndexesAsync(
                request.TableName, pk, sk, nowEpoch,
                indexes, keyInfo.AttributeDefinitions, cancellationToken)
            : await store.DeleteItemAsync(request.TableName, pk, sk, nowEpoch, cancellationToken);

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

        var nowEpoch = SqliteStore.NowEpoch();

        var keyInfo = await store.GetKeySchemaAsync(request.TableName, cancellationToken)
            ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        var (pk, sk) = KeyHelper.ExtractKeys(request.Key, keyInfo.KeySchema, keyInfo.AttributeDefinitions);
        var existingJson = await store.GetItemAsync(request.TableName, pk, sk, nowEpoch, cancellationToken);
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
                updatedOldAttrs = [];
                foreach (var k in modifiedKeys)
                    if (oldItem.TryGetValue(k, out var v))
                        updatedOldAttrs[k] = v;
            }

            if (request.ReturnValues == ReturnValue.UPDATED_NEW)
            {
                updatedNewAttrs = [];
                foreach (var k in modifiedKeys)
                    if (newItem.TryGetValue(k, out var v))
                        updatedNewAttrs[k] = v;
            }

            existingItem = newItem;
        }
        else if (request.AttributeUpdates is { Count: > 0 })
        {
            existingItem = ApplyAttributeUpdates(existingItem, request.AttributeUpdates);
        }

        var itemJson = AttributeValueSerializer.Serialize(existingItem);
        var skNum = ComputeSkNum(sk, keyInfo);

        var ttlAttr = await store.GetTtlAttributeNameAsync(request.TableName, cancellationToken);
        double? ttlEpoch = ttlAttr is not null && TtlEpochParser.TryParse(existingItem, ttlAttr, out var epoch) ? epoch : null;

        var indexes = await store.GetIndexDefinitionsAsync(request.TableName, cancellationToken);
        _ = indexes.Count > 0
            ? await store.PutItemWithIndexesAsync(
                request.TableName, pk, sk, itemJson, skNum, ttlEpoch, nowEpoch,
                indexes, keyInfo.AttributeDefinitions, existingItem, cancellationToken)
            : await store.PutItemAsync(request.TableName, pk, sk, itemJson, skNum, ttlEpoch, nowEpoch, cancellationToken);

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
        UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = key,
            AttributeUpdates = attributeUpdates,
        }, cancellationToken);

    public Task<UpdateItemResponse> UpdateItemAsync(
        string tableName,
        Dictionary<string, AttributeValue> key,
        Dictionary<string, AttributeValueUpdate> attributeUpdates,
        ReturnValue returnValues,
        CancellationToken cancellationToken = default) =>
        UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = key,
            AttributeUpdates = attributeUpdates,
            ReturnValues = returnValues,
        }, cancellationToken);

    private static Dictionary<string, AttributeValue> ApplyAttributeUpdates(
        Dictionary<string, AttributeValue> item,
        Dictionary<string, AttributeValueUpdate> attributeUpdates)
    {
        var result = new Dictionary<string, AttributeValue>(item);
        foreach (var (attrName, update) in attributeUpdates)
        {
            var action = update.Action?.Value ?? "PUT";
            switch (action)
            {
                case "PUT":
                    result[attrName] = update.Value;
                    break;
                case "DELETE" when update.Value is null:
                    _ = result.Remove(attrName);
                    break;
                case "DELETE" when update.Value.SS is { Count: > 0 }:
                    if (result.TryGetValue(attrName, out var ssVal) && ssVal.SS is not null)
                    {
                        foreach (var s in update.Value.SS)
                            _ = ssVal.SS.Remove(s);
                    }
                    break;
                case "DELETE" when update.Value.NS is { Count: > 0 }:
                    if (result.TryGetValue(attrName, out var nsVal) && nsVal.NS is not null)
                    {
                        foreach (var n in update.Value.NS)
                            _ = nsVal.NS.Remove(n);
                    }
                    break;
                case "ADD" when update.Value.N is not null:
                    if (result.TryGetValue(attrName, out var numVal) && numVal.N is not null)
                    {
                        var current = double.Parse(numVal.N, CultureInfo.InvariantCulture);
                        var addend = double.Parse(update.Value.N, CultureInfo.InvariantCulture);
                        result[attrName] = new AttributeValue { N = (current + addend).ToString(CultureInfo.InvariantCulture) };
                    }
                    else
                    {
                        result[attrName] = update.Value;
                    }
                    break;
                case "ADD" when update.Value.SS is { Count: > 0 }:
                    if (result.TryGetValue(attrName, out var addSsVal) && addSsVal.SS is not null)
                        addSsVal.SS.AddRange(update.Value.SS);
                    else
                        result[attrName] = update.Value;
                    break;
                case "ADD" when update.Value.NS is { Count: > 0 }:
                    if (result.TryGetValue(attrName, out var addNsVal) && addNsVal.NS is not null)
                        addNsVal.NS.AddRange(update.Value.NS);
                    else
                        result[attrName] = update.Value;
                    break;
                default:
                    result[attrName] = update.Value;
                    break;
            }
        }

        return result;
    }

    private static double? ComputeSkNum(string sk, KeySchemaInfo keyInfo)
    {
        var rangeKey = keyInfo.KeySchema.FirstOrDefault(static k => k.KeyType == KeyType.RANGE);
        if (rangeKey is null)
            return null;

        var attrDef = keyInfo.AttributeDefinitions.First(a => a.AttributeName == rangeKey.AttributeName);
        return attrDef.AttributeType == ScalarAttributeType.N
            ? double.Parse(sk, CultureInfo.InvariantCulture)
            : null;
    }
}
