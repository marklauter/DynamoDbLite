using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

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
}
