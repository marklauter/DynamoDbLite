using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Expressions;
using DynamoDbLite.SqliteStores;
using DynamoDbLite.SqliteStores.Models;

namespace DynamoDbLite;

public sealed partial class DynamoDbClient
{
    /// <inheritdoc/>
    public async Task<ScanResponse> ScanAsync(ScanRequest request, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableName);

        ValidateScanSegmentation(request);

        // Support legacy ScanFilter by converting to FilterExpression
        if (string.IsNullOrEmpty(request.FilterExpression) && request.ScanFilter is { Count: > 0 })
            ConvertScanFilterToExpression(request);

        var nowEpoch = SqliteStore.NowEpoch();

        var tableKeyInfo = await store.GetKeySchemaAsync(request.TableName, cancellationToken)
            ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        if (!string.IsNullOrEmpty(request.IndexName))
            return await ScanIndexAsync(request, tableKeyInfo, nowEpoch, cancellationToken);

        string? exclusiveStartPk = null;
        string? exclusiveStartSk = null;
        if (request.ExclusiveStartKey is { Count: > 0 })
        {
            var (esPk, esSk) = KeyHelper.ExtractKeys(request.ExclusiveStartKey, tableKeyInfo.KeySchema, tableKeyInfo.AttributeDefinitions);
            exclusiveStartPk = esPk;
            exclusiveStartSk = esSk;
        }

        var rows = await store.ScanItemsAsync(
            request.TableName, request.Limit, exclusiveStartPk, exclusiveStartSk, nowEpoch, cancellationToken);

        TriggerBackgroundCleanup(request.TableName);

        if (request.TotalSegments is int total && total > 1)
            rows = [.. rows.Where(r => SegmentOf(r.Pk, total) == request.Segment!.Value)];

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

        var response = new ScanResponse
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
            response.LastEvaluatedKey = BuildLastEvaluatedKey(lastRow.Pk, lastRow.Sk, tableKeyInfo);
        }

        return response;
    }

    private async Task<ScanResponse> ScanIndexAsync(
        ScanRequest request,
        KeySchemaInfo tableKeyInfo,
        double nowEpoch,
        CancellationToken cancellationToken)
    {
        var indexKeyInfo = await store.GetIndexKeySchemaAsync(request.TableName, request.IndexName, cancellationToken)
            ?? throw new AmazonDynamoDBException($"The table does not have the specified index: {request.IndexName}");

        var allIndexes = await store.GetIndexDefinitionsAsync(request.TableName, cancellationToken);
        var indexDef = allIndexes.First(i => i.IndexName == request.IndexName);

        if (indexDef.IsGlobal && request.ConsistentRead is true)
            throw new AmazonDynamoDBException(
                "Consistent reads are not supported on global secondary indexes");

        string? exclusiveStartPk = null;
        string? exclusiveStartSk = null;
        string? exclusiveStartTablePk = null;
        string? exclusiveStartTableSk = null;
        if (request.ExclusiveStartKey is { Count: > 0 })
        {
            var indexKeys = KeyHelper.TryExtractIndexKeys(request.ExclusiveStartKey, indexKeyInfo.KeySchema, indexKeyInfo.AttributeDefinitions);
            if (indexKeys is not null)
            {
                exclusiveStartPk = indexKeys.Value.Pk;
                exclusiveStartSk = indexKeys.Value.Sk;
            }

            var tableKeys = KeyHelper.TryExtractIndexKeys(request.ExclusiveStartKey, tableKeyInfo.KeySchema, tableKeyInfo.AttributeDefinitions);
            if (tableKeys is not null)
            {
                exclusiveStartTablePk = tableKeys.Value.Pk;
                exclusiveStartTableSk = tableKeys.Value.Sk;
            }
        }

        var rows = await store.ScanIndexItemsAsync(
            request.TableName, request.IndexName, request.Limit,
            exclusiveStartPk, exclusiveStartSk,
            exclusiveStartTablePk, exclusiveStartTableSk, nowEpoch, cancellationToken);

        if (request.TotalSegments is int total && total > 1)
            rows = [.. rows.Where(r => SegmentOf(r.Pk, total) == request.Segment!.Value)];

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
                items.Add(item);
                continue;
            }

            if (projectionPaths is not null)
                item = Expressions.ProjectionExpressionEvaluator.Apply(item, projectionPaths);

            items.Add(item);
        }

        var response = new ScanResponse
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
            response.LastEvaluatedKey = BuildIndexLastEvaluatedKey(
                lastRow, indexKeyInfo, tableKeyInfo);
        }

        return response;
    }

    /// <inheritdoc/>
    public Task<ScanResponse> ScanAsync(
        string tableName,
        List<string> attributesToGet,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(attributesToGet);
        var (projection, attrNames) = BuildProjectionFromAttributesToGet(attributesToGet);
        return ScanAsync(new ScanRequest
        {
            TableName = tableName,
            ProjectionExpression = projection,
            ExpressionAttributeNames = attrNames,
        }, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<ScanResponse> ScanAsync(
        string tableName,
        Dictionary<string, Condition> scanFilter,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scanFilter);
        var (filterExpression, attrNames, attrValues) = ConvertConditionsToExpression(scanFilter);
        return ScanAsync(new ScanRequest
        {
            TableName = tableName,
            FilterExpression = filterExpression,
            ExpressionAttributeNames = attrNames,
            ExpressionAttributeValues = attrValues,
        }, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<ScanResponse> ScanAsync(
        string tableName,
        List<string> attributesToGet,
        Dictionary<string, Condition> scanFilter,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(attributesToGet);
        ArgumentNullException.ThrowIfNull(scanFilter);
        var (filterExpression, attrNames, attrValues) = ConvertConditionsToExpression(scanFilter);
        var (projection, projectionNames) = BuildProjectionFromAttributesToGet(attributesToGet);
        foreach (var (k, v) in projectionNames)
            attrNames[k] = v;
        return ScanAsync(new ScanRequest
        {
            TableName = tableName,
            ProjectionExpression = projection,
            FilterExpression = filterExpression,
            ExpressionAttributeNames = attrNames,
            ExpressionAttributeValues = attrValues,
        }, cancellationToken);
    }

    private static void ConvertScanFilterToExpression(ScanRequest request)
    {
        var (expression, attrNames, attrValues) = ConvertConditionsToExpression(request.ScanFilter, "sf");

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

    private static void ValidateScanSegmentation(ScanRequest request)
    {
        var hasSegment = request.IsSegmentSet;
        var hasTotal = request.IsTotalSegmentsSet;

        if (hasSegment != hasTotal)
            throw new AmazonDynamoDBException(hasTotal
                ? "1 validation error detected: Value at 'segment' failed to satisfy constraint: Member must not be null"
                : "1 validation error detected: Value at 'totalSegments' failed to satisfy constraint: Member must not be null");

        if (!hasTotal)
            return;

        if (request.TotalSegments is < 1 or > 1_000_000)
            throw new AmazonDynamoDBException(
                "1 validation error detected: Value at 'totalSegments' failed to satisfy constraint: Member must be between 1 and 1000000");

        if (request.Segment < 0 || request.Segment >= request.TotalSegments)
            throw new AmazonDynamoDBException(
                "1 validation error detected: Value at 'segment' failed to satisfy constraint: Member must be between 0 and totalSegments - 1");
    }

    // Stable partition-by-hash used for parallel Scan. Real DynamoDB partitions items by an
    // internal hash of the partition key; for the in-process emulator any deterministic,
    // well-distributed hash suffices to make the per-segment results disjoint and complete.
    // FNV-1a chosen for being stable across processes (unlike string.GetHashCode) and zero-dep.
    private static int SegmentOf(string partitionKey, int totalSegments)
    {
        const uint FnvOffsetBasis = 2166136261u;
        const uint FnvPrime = 16777619u;
        var hash = FnvOffsetBasis;
        foreach (var c in partitionKey)
        {
            hash ^= (byte)(c & 0xff);
            hash *= FnvPrime;
            hash ^= (byte)((c >> 8) & 0xff);
            hash *= FnvPrime;
        }

        return (int)(hash % (uint)totalSegments);
    }
}
