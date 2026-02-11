using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.SqlteStores;
using DynamoDbLite.SqlteStores.Models;
using System.Net;

namespace DynamoDbLite;

// ── Transactions & PartiQL ───────────────────────────────────────────
public sealed partial class DynamoDbClient
{
    public Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task<ExecuteTransactionResponse> ExecuteTransactionAsync(ExecuteTransactionRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public async Task<TransactWriteItemsResponse> TransactWriteItemsAsync(
        TransactWriteItemsRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.TransactItems);

        if (request.TransactItems.Count == 0)
            throw new AmazonDynamoDBException(
                "1 validation error detected: Value at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1");

        if (request.TransactItems.Count > MaxTransactItems)
            throw new AmazonDynamoDBException(
                $"1 validation error detected: Value at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to {MaxTransactItems}");

        // Idempotency: check token cache
        if (!string.IsNullOrEmpty(request.ClientRequestToken))
        {
            PurgeExpiredTokens();
            if (transactWriteTokenCache.TryGetValue(request.ClientRequestToken, out var cached)
                && cached.Expiry > DateTime.UtcNow)
                return cached.Response;
        }

        // ── Validation phase ─────────────────────────────────────────
        var actions = request.TransactItems;
        var seenKeys = new HashSet<(string, string, string)>();
        var keyInfoByTable = new Dictionary<string, KeySchemaInfo>();
        var resolvedActions = new List<ResolvedTransactWriteAction>(actions.Count);

        for (var i = 0; i < actions.Count; i++)
        {
            var item = actions[i];
            var actionCount = (item.Put is not null ? 1 : 0)
                + (item.Update is not null ? 1 : 0)
                + (item.Delete is not null ? 1 : 0)
                + (item.ConditionCheck is not null ? 1 : 0);

            if (actionCount != 1)
                throw new AmazonDynamoDBException(
                    "TransactItems can only contain one of ConditionCheck, Put, Update or Delete");

            var extracted = ExtractTransactWriteAction(item);

            if (extracted.ActionType is TransactActionType.Update
                && string.IsNullOrEmpty(extracted.UpdateExpression))
                throw new AmazonDynamoDBException(
                    "One or more parameter values are not valid. An UpdateExpression is required for the Update action.");

            if (!keyInfoByTable.TryGetValue(extracted.TableName, out var keyInfo))
            {
                keyInfo = await store.GetKeySchemaAsync(extracted.TableName, cancellationToken)
                    ?? throw new ResourceNotFoundException(
                        $"Requested resource not found: Table: {extracted.TableName} not found");
                keyInfoByTable[extracted.TableName] = keyInfo;
            }

            var keyDict = extracted.Item ?? extracted.Key!;
            KeyHelper.ValidateKeyTypes(keyDict, keyInfo.KeySchema, keyInfo.AttributeDefinitions);
            var (pk, sk) = KeyHelper.ExtractKeys(keyDict, keyInfo.KeySchema, keyInfo.AttributeDefinitions);

            if (!seenKeys.Add((extracted.TableName, pk, sk)))
                throw new AmazonDynamoDBException(
                    "Transaction request cannot include multiple operations on one item");

            resolvedActions.Add(new ResolvedTransactWriteAction(
                Index: i,
                TableName: extracted.TableName,
                Pk: pk,
                Sk: sk,
                KeyInfo: keyInfo,
                Key: extracted.Key,
                Item: extracted.Item,
                ConditionExpression: extracted.ConditionExpression,
                ExpressionAttributeNames: extracted.ExpressionAttributeNames,
                ExpressionAttributeValues: extracted.ExpressionAttributeValues,
                UpdateExpression: extracted.UpdateExpression,
                ReturnValuesOnConditionCheckFailure: extracted.ReturnValuesOnConditionCheckFailure,
                ActionType: extracted.ActionType));
        }

        // ── Pre-read phase ───────────────────────────────────────────
        var nowEpoch = SqliteStore.NowEpoch();
        var existingItems = new string?[actions.Count];
        for (var i = 0; i < resolvedActions.Count; i++)
        {
            var ra = resolvedActions[i];
            var needsExisting = ra.ActionType switch
            {
                TransactActionType.Put => !string.IsNullOrEmpty(ra.ConditionExpression),
                TransactActionType.Update => true,
                TransactActionType.Delete => !string.IsNullOrEmpty(ra.ConditionExpression),
                TransactActionType.ConditionCheck => true,
                _ => false
            };

            if (needsExisting)
                existingItems[i] = await store.GetItemAsync(ra.TableName, ra.Pk, ra.Sk, nowEpoch, cancellationToken);
        }

        // ── Condition evaluation phase ───────────────────────────────
        var cancellationReasons = new CancellationReason[actions.Count];
        var anyFailed = false;

        for (var i = 0; i < resolvedActions.Count; i++)
        {
            var ra = resolvedActions[i];
            if (string.IsNullOrEmpty(ra.ConditionExpression))
            {
                cancellationReasons[i] = new CancellationReason { Code = "None" };
                continue;
            }

            var existingJson = existingItems[i];
            var existingItem = existingJson is not null
                ? AttributeValueSerializer.Deserialize(existingJson)
                : null;

            var conditionAst = Expressions.ConditionExpressionParser.Parse(ra.ConditionExpression);
            var conditionResult = Expressions.ConditionExpressionEvaluator.Evaluate(
                conditionAst, existingItem, ra.ExpressionAttributeNames, ra.ExpressionAttributeValues);

            if (conditionResult)
            {
                cancellationReasons[i] = new CancellationReason { Code = "None" };
            }
            else
            {
                anyFailed = true;
                cancellationReasons[i] = new CancellationReason
                {
                    Code = "ConditionalCheckFailed",
                    Message = "The conditional request failed"
                };

                if (ra.ReturnValuesOnConditionCheckFailure == ReturnValuesOnConditionCheckFailure.ALL_OLD
                    && existingItem is not null)
                    cancellationReasons[i].Item = existingItem;
            }
        }

        if (anyFailed)
            throw new TransactionCanceledException("Transaction cancelled, precondition failed.")
            {
                CancellationReasons = [.. cancellationReasons]
            };

        // ── Update expression application ────────────────────────────
        var computedItems = new Dictionary<string, AttributeValue>?[actions.Count];

        for (var i = 0; i < resolvedActions.Count; i++)
        {
            var ra = resolvedActions[i];
            if (ra.ActionType is TransactActionType.Update)
            {
                var existingJson = existingItems[i];
                // For new items, seed with key attributes from the Key dictionary
                var existingItem = existingJson is not null
                    ? AttributeValueSerializer.Deserialize(existingJson)
                    : new Dictionary<string, AttributeValue>(ra.Key!);

                var updateAst = Expressions.UpdateExpressionParser.Parse(ra.UpdateExpression!);
                var (newItem, modifiedKeys) = Expressions.UpdateExpressionEvaluator.Apply(
                    updateAst, existingItem, ra.ExpressionAttributeNames, ra.ExpressionAttributeValues);

                foreach (var key in ra.KeyInfo.KeySchema)
                {
                    if (modifiedKeys.Contains(key.AttributeName))
                        throw new AmazonDynamoDBException(
                            $"One or more parameter values were invalid: Cannot update attribute {key.AttributeName}. This attribute is part of the key");
                }

                computedItems[i] = newItem;
            }
            else if (ra.ActionType is TransactActionType.Put)
            {
                computedItems[i] = ra.Item;
            }
        }

        // ── Write phase (single SQLite transaction) ──────────────────
        var operations = new List<TransactWriteOperation>();
        var ttlConfigByTable = new Dictionary<string, string?>();

        for (var i = 0; i < resolvedActions.Count; i++)
        {
            var ra = resolvedActions[i];

            if (ra.ActionType is TransactActionType.ConditionCheck)
                continue;

            if (ra.ActionType is TransactActionType.Delete)
            {
                operations.Add(new TransactWriteOperation(ra.TableName, ra.Pk, ra.Sk, null, null, null, IsDelete: true));
            }
            else
            {
                if (!ttlConfigByTable.TryGetValue(ra.TableName, out var ttlAttr))
                {
                    ttlAttr = await store.GetTtlAttributeNameAsync(ra.TableName, cancellationToken);
                    ttlConfigByTable[ra.TableName] = ttlAttr;
                }

                var itemToWrite = computedItems[i]!;
                var ttlEpoch = ttlAttr is not null ? TtlHelper.ExtractTtlEpoch(itemToWrite, ttlAttr) : null;
                var itemJson = AttributeValueSerializer.Serialize(itemToWrite);
                var skNum = ComputeSkNum(ra.Sk, ra.KeyInfo);
                operations.Add(new TransactWriteOperation(ra.TableName, ra.Pk, ra.Sk, skNum, ttlEpoch, itemJson, IsDelete: false));
            }
        }

        // Load index info for affected tables
        Dictionary<string, (List<IndexDefinition> Indexes, List<AttributeDefinition> AttrDefs)>? indexInfoByTable = null;
        var affectedTables = operations.Select(static o => o.TableName).Distinct();

        foreach (var tableName in affectedTables)
        {
            var indexes = await store.GetIndexDefinitionsAsync(tableName, cancellationToken);
            if (indexes.Count > 0)
            {
                indexInfoByTable ??= [];
                indexInfoByTable[tableName] = (indexes, keyInfoByTable[tableName].AttributeDefinitions);
            }
        }

        if (operations.Count > 0)
            await store.TransactWriteItemsAsync(operations, indexInfoByTable, nowEpoch, cancellationToken);

        var response = new TransactWriteItemsResponse { HttpStatusCode = HttpStatusCode.OK };

        // Idempotency: cache response
        if (!string.IsNullOrEmpty(request.ClientRequestToken))
            _ = transactWriteTokenCache.TryAdd(
                request.ClientRequestToken,
                (DateTime.UtcNow.AddMinutes(10), response));

        return response;
    }

    public async Task<TransactGetItemsResponse> TransactGetItemsAsync(
        TransactGetItemsRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.TransactItems);

        if (request.TransactItems.Count == 0)
            throw new AmazonDynamoDBException(
                "1 validation error detected: Value at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1");

        if (request.TransactItems.Count > MaxTransactItems)
            throw new AmazonDynamoDBException(
                $"1 validation error detected: Value at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to {MaxTransactItems}");

        var nowEpoch = SqliteStore.NowEpoch();
        var responses = new List<ItemResponse>(request.TransactItems.Count);
        var cleanedTables = new HashSet<string>();

        foreach (var transactItem in request.TransactItems)
        {
            ArgumentNullException.ThrowIfNull(transactItem.Get);
            ArgumentException.ThrowIfNullOrWhiteSpace(transactItem.Get.TableName);
            ArgumentNullException.ThrowIfNull(transactItem.Get.Key);

            var get = transactItem.Get;
            var keyInfo = await store.GetKeySchemaAsync(get.TableName, cancellationToken)
                ?? throw new ResourceNotFoundException(
                    $"Requested resource not found: Table: {get.TableName} not found");

            if (cleanedTables.Add(get.TableName))
                TriggerBackgroundCleanup(get.TableName);

            var (pk, sk) = KeyHelper.ExtractKeys(get.Key, keyInfo.KeySchema, keyInfo.AttributeDefinitions);
            var itemJson = await store.GetItemAsync(get.TableName, pk, sk, nowEpoch, cancellationToken);

            if (itemJson is not null)
            {
                var item = AttributeValueSerializer.Deserialize(itemJson);

                if (!string.IsNullOrEmpty(get.ProjectionExpression))
                {
                    var paths = Expressions.ProjectionExpressionParser.Parse(
                        get.ProjectionExpression, get.ExpressionAttributeNames);
                    item = Expressions.ProjectionExpressionEvaluator.Apply(item, paths);
                }

                responses.Add(new ItemResponse { Item = item });
            }
            else
            {
                responses.Add(new ItemResponse());
            }
        }

        return new TransactGetItemsResponse
        {
            Responses = responses,
            HttpStatusCode = HttpStatusCode.OK
        };
    }

    private void PurgeExpiredTokens()
    {
        var now = DateTime.UtcNow;
        foreach (var key in transactWriteTokenCache.Keys)
            if (transactWriteTokenCache.TryGetValue(key, out var entry) && entry.Expiry <= now)
                _ = transactWriteTokenCache.TryRemove(key, out _);
    }

    private static ResolvedTransactWriteAction ExtractTransactWriteAction(TransactWriteItem item)
    {
        if (item.Put is not null)
        {
            var p = item.Put;
            return new(default, p.TableName, default!, default!, default!,
                Key: null, Item: p.Item,
                p.ConditionExpression, p.ExpressionAttributeNames, p.ExpressionAttributeValues,
                null, ParseReturnOnFail(p.ReturnValuesOnConditionCheckFailure),
                TransactActionType.Put);
        }

        if (item.Update is not null)
        {
            var u = item.Update;
            return new(default, u.TableName, default!, default!, default!,
                Key: u.Key, Item: null,
                u.ConditionExpression, u.ExpressionAttributeNames, u.ExpressionAttributeValues,
                u.UpdateExpression, ParseReturnOnFail(u.ReturnValuesOnConditionCheckFailure),
                TransactActionType.Update);
        }

        if (item.Delete is not null)
        {
            var d = item.Delete;
            return new(default, d.TableName, default!, default!, default!,
                Key: d.Key, Item: null,
                d.ConditionExpression, d.ExpressionAttributeNames, d.ExpressionAttributeValues,
                null, ParseReturnOnFail(d.ReturnValuesOnConditionCheckFailure),
                TransactActionType.Delete);
        }

        var c = item.ConditionCheck!;
        return new(default, c.TableName, default!, default!, default!,
            Key: c.Key, Item: null,
            c.ConditionExpression, c.ExpressionAttributeNames, c.ExpressionAttributeValues,
            null, ParseReturnOnFail(c.ReturnValuesOnConditionCheckFailure),
            TransactActionType.ConditionCheck);
    }

    private static ReturnValuesOnConditionCheckFailure? ParseReturnOnFail(string? value) =>
        string.IsNullOrEmpty(value) ? null
        : value == "ALL_OLD" ? ReturnValuesOnConditionCheckFailure.ALL_OLD
        : ReturnValuesOnConditionCheckFailure.NONE;

    private enum TransactActionType { Put, Update, Delete, ConditionCheck }

    private sealed record ResolvedTransactWriteAction(
        int Index,
        string TableName,
        string Pk,
        string Sk,
        KeySchemaInfo KeyInfo,
        Dictionary<string, AttributeValue>? Key,
        Dictionary<string, AttributeValue>? Item,
        string? ConditionExpression,
        Dictionary<string, string>? ExpressionAttributeNames,
        Dictionary<string, AttributeValue>? ExpressionAttributeValues,
        string? UpdateExpression,
        ReturnValuesOnConditionCheckFailure? ReturnValuesOnConditionCheckFailure,
        TransactActionType ActionType);
}
