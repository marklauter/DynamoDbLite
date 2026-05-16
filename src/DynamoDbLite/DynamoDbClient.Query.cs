using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Expressions;
using DynamoDbLite.SqliteStores;
using DynamoDbLite.SqliteStores.Models;

namespace DynamoDbLite;

public sealed partial class DynamoDbClient
{
    /// <inheritdoc/>
    public async Task<QueryResponse> QueryAsync(QueryRequest request, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableName);

        // Support legacy KeyConditions by converting to KeyConditionExpression
        if (string.IsNullOrWhiteSpace(request.KeyConditionExpression) && request.KeyConditions is { Count: > 0 })
            ConvertKeyConditionsToExpression(request);

        // Support legacy QueryFilter by converting to FilterExpression
        if (string.IsNullOrEmpty(request.FilterExpression) && request.QueryFilter is { Count: > 0 })
            ConvertQueryFilterToExpression(request);

        ArgumentException.ThrowIfNullOrWhiteSpace(request.KeyConditionExpression);

        var nowEpoch = SqliteStore.NowEpoch();

        // Validate table exists
        var tableKeyInfo = await store.GetKeySchemaAsync(request.TableName, cancellationToken)
            ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        if (!string.IsNullOrEmpty(request.IndexName))
            return await QueryIndexAsync(request, tableKeyInfo, nowEpoch, cancellationToken);

        var keyCondition = Expressions.KeyConditionExpressionParser.Parse(request.KeyConditionExpression);
        var sql = KeyConditionSqlBuilder.Build(
            keyCondition, tableKeyInfo, request.ExpressionAttributeValues);

        var ascending = request.ScanIndexForward is not false;

        string? exclusiveStartSk = null;
        if (request.ExclusiveStartKey is { Count: > 0 })
        {
            var (_, esSk) = KeyHelper.ExtractKeys(request.ExclusiveStartKey, tableKeyInfo.KeySchema, tableKeyInfo.AttributeDefinitions);
            exclusiveStartSk = esSk;
        }

        var rows = await store.QueryItemsAsync(
            request.TableName, sql.PkValue, sql.SkWhereSql, sql.SkParams,
            sql.OrderByColumn, ascending, request.Limit, exclusiveStartSk, nowEpoch, cancellationToken);

        TriggerBackgroundCleanup(request.TableName);

        var scannedCount = rows.Count;
        var items = new List<Dictionary<string, AttributeValue>>(rows.Count);

        Expressions.ConditionNode? filterAst = null;
        if (!string.IsNullOrEmpty(request.FilterExpression))
            filterAst = Expressions.ConditionExpressionParser.Parse(request.FilterExpression);

        IReadOnlyList<Expressions.AttributePath>? projectionPaths = null;
        if (!string.IsNullOrEmpty(request.ProjectionExpression))
            projectionPaths = Expressions.ProjectionExpressionParser.Parse(
                request.ProjectionExpression, request.ExpressionAttributeNames);

        foreach (var row in rows)
        {
            var item = AttributeValueSerializer.Deserialize(row.ItemJson);

            if (filterAst is not null)
            {
                var passes = Expressions.ConditionExpressionEvaluator.Evaluate(
                    filterAst, item, request.ExpressionAttributeNames, request.ExpressionAttributeValues);
                if (!passes)
                    continue;
            }

            if (request.Select == Select.COUNT)
            {
                items.Add(item);
                continue;
            }

            if (projectionPaths is not null)
                item = Expressions.ProjectionExpressionEvaluator.Apply(item, projectionPaths);

            items.Add(item);
        }

        var response = new QueryResponse
        {
            HttpStatusCode = System.Net.HttpStatusCode.OK,
            ScannedCount = scannedCount,
            Count = items.Count,
        };

        if (request.Select != Select.COUNT)
            response.Items = items;

        if (request.Limit is not null && rows.Count == request.Limit.Value && rows.Count > 0)
        {
            var lastRow = rows[^1];
            response.LastEvaluatedKey = KeyHelper.BuildLastEvaluatedKey(lastRow.Pk, lastRow.Sk, tableKeyInfo);
        }

        return response;
    }

    private async Task<QueryResponse> QueryIndexAsync(
        QueryRequest request,
        KeySchemaInfo tableKeyInfo,
        double nowEpoch,
        CancellationToken cancellationToken)
    {
        var indexKeyInfo = await store.GetIndexKeySchemaAsync(request.TableName, request.IndexName, cancellationToken)
            ?? throw new AmazonDynamoDBException($"The table does not have the specified index: {request.IndexName}");

        // Determine if this is a GSI (for ConsistentRead validation)
        var allIndexes = await store.GetIndexDefinitionsAsync(request.TableName, cancellationToken);
        var indexDef = allIndexes.First(i => i.IndexName == request.IndexName);

        if (indexDef.IsGlobal && request.ConsistentRead is true)
            throw new AmazonDynamoDBException(
                "Consistent reads are not supported on global secondary indexes");

        var keyCondition = Expressions.KeyConditionExpressionParser.Parse(request.KeyConditionExpression);
        var sql = KeyConditionSqlBuilder.Build(
            keyCondition, indexKeyInfo, request.ExpressionAttributeValues);

        var ascending = request.ScanIndexForward is not false;

        string? exclusiveStartSk = null;
        string? exclusiveStartTablePk = null;
        string? exclusiveStartTableSk = null;
        if (request.ExclusiveStartKey is { Count: > 0 })
        {
            var indexKeys = KeyHelper.TryExtractIndexKeys(request.ExclusiveStartKey, indexKeyInfo.KeySchema, indexKeyInfo.AttributeDefinitions);
            if (indexKeys is not null)
                exclusiveStartSk = indexKeys.Value.Sk;
            var tableKeys = KeyHelper.TryExtractIndexKeys(request.ExclusiveStartKey, tableKeyInfo.KeySchema, tableKeyInfo.AttributeDefinitions);
            if (tableKeys is not null)
            {
                exclusiveStartTablePk = tableKeys.Value.Pk;
                exclusiveStartTableSk = tableKeys.Value.Sk;
            }
        }

        var rows = await store.QueryIndexItemsAsync(
            request.TableName, request.IndexName, sql.PkValue, sql.SkWhereSql, sql.SkParams,
            sql.OrderByColumn, ascending, request.Limit, exclusiveStartSk,
            exclusiveStartTablePk, exclusiveStartTableSk, nowEpoch, cancellationToken);

        var scannedCount = rows.Count;
        var items = new List<Dictionary<string, AttributeValue>>(rows.Count);

        Expressions.ConditionNode? filterAst = null;
        if (!string.IsNullOrEmpty(request.FilterExpression))
            filterAst = Expressions.ConditionExpressionParser.Parse(request.FilterExpression);

        IReadOnlyList<Expressions.AttributePath>? projectionPaths = null;
        if (!string.IsNullOrEmpty(request.ProjectionExpression))
            projectionPaths = Expressions.ProjectionExpressionParser.Parse(
                request.ProjectionExpression, request.ExpressionAttributeNames);

        foreach (var row in rows)
        {
            var item = AttributeValueSerializer.Deserialize(row.ItemJson);

            // Apply index projection filtering first
            item = ApplyIndexProjection(item, indexDef, tableKeyInfo, indexKeyInfo);

            if (filterAst is not null)
            {
                var passes = Expressions.ConditionExpressionEvaluator.Evaluate(
                    filterAst, item, request.ExpressionAttributeNames, request.ExpressionAttributeValues);
                if (!passes)
                    continue;
            }

            if (request.Select == Select.COUNT)
            {
                items.Add(item);
                continue;
            }

            if (request.Select == Select.ALL_PROJECTED_ATTRIBUTES)
            {
                // Already applied index projection above
                items.Add(item);
                continue;
            }

            if (projectionPaths is not null)
                item = Expressions.ProjectionExpressionEvaluator.Apply(item, projectionPaths);

            items.Add(item);
        }

        var response = new QueryResponse
        {
            HttpStatusCode = System.Net.HttpStatusCode.OK,
            ScannedCount = scannedCount,
            Count = items.Count,
        };

        if (request.Select != Select.COUNT)
            response.Items = items;

        if (request.Limit is not null && rows.Count == request.Limit.Value && rows.Count > 0)
        {
            var lastRow = rows[^1];
            response.LastEvaluatedKey = KeyHelper.BuildIndexLastEvaluatedKey(
                lastRow, indexKeyInfo, tableKeyInfo);
        }

        return response;
    }

    private static void ConvertKeyConditionsToExpression(QueryRequest request)
    {
        var (expression, attrNames, attrValues) = LegacyConditionConverter.Convert(request.KeyConditions);
        request.KeyConditionExpression = expression;

        request.ExpressionAttributeNames ??= [];
        foreach (var (k, v) in attrNames)
            request.ExpressionAttributeNames[k] = v;

        request.ExpressionAttributeValues ??= [];
        foreach (var (k, v) in attrValues)
            request.ExpressionAttributeValues[k] = v;
    }

    private static void ConvertQueryFilterToExpression(QueryRequest request)
    {
        var (expression, attrNames, attrValues) = LegacyConditionConverter.Convert(request.QueryFilter, "qf");

        request.FilterExpression = request.FilterExpression is not null
            ? $"({request.FilterExpression}) AND ({expression})"
            : expression;

        request.ExpressionAttributeNames ??= [];
        foreach (var (k, v) in attrNames)
            request.ExpressionAttributeNames[k] = v;

        request.ExpressionAttributeValues ??= [];
        foreach (var (k, v) in attrValues)
            request.ExpressionAttributeValues[k] = v;
    }

    private static Dictionary<string, AttributeValue> ApplyIndexProjection(
        Dictionary<string, AttributeValue> item,
        IndexDefinition indexDef,
        KeySchemaInfo tableKeyInfo,
        KeySchemaInfo indexKeyInfo)
    {
        if (indexDef.ProjectionType == "ALL")
            return item;

        // Collect key attribute names (table keys + index keys always included)
        var keyAttrs = new HashSet<string>();
        foreach (var k in tableKeyInfo.KeySchema)
            _ = keyAttrs.Add(k.AttributeName);
        foreach (var k in indexKeyInfo.KeySchema)
            _ = keyAttrs.Add(k.AttributeName);

        if (indexDef.ProjectionType == "KEYS_ONLY")
        {
            var result = new Dictionary<string, AttributeValue>();
            foreach (var (name, value) in item)
                if (keyAttrs.Contains(name))
                    result[name] = value;
            return result;
        }

        // INCLUDE: keys + specified non-key attributes
        if (indexDef.ProjectionType == "INCLUDE")
        {
            var includedAttrs = new HashSet<string>(keyAttrs);
            if (indexDef.NonKeyAttributes is not null)
                foreach (var attr in indexDef.NonKeyAttributes)
                    _ = includedAttrs.Add(attr);

            var result = new Dictionary<string, AttributeValue>();
            foreach (var (name, value) in item)
                if (includedAttrs.Contains(name))
                    result[name] = value;
            return result;
        }

        return item;
    }
}
