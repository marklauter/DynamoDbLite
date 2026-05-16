using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Expressions;
using DynamoDbLite.SqliteStores;
using DynamoDbLite.SqliteStores.Models;
using System.Globalization;

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
            response.LastEvaluatedKey = BuildLastEvaluatedKey(lastRow.Pk, lastRow.Sk, tableKeyInfo);
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
            response.LastEvaluatedKey = BuildIndexLastEvaluatedKey(
                lastRow, indexKeyInfo, tableKeyInfo);
        }

        return response;
    }

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

    private static (string Projection, Dictionary<string, string> Names) BuildProjectionFromAttributesToGet(
        List<string> attributesToGet)
    {
        var names = new Dictionary<string, string>(attributesToGet.Count);
        var aliases = new string[attributesToGet.Count];
        for (var i = 0; i < attributesToGet.Count; i++)
        {
            var alias = $"#ag{i}";
            names[alias] = attributesToGet[i];
            aliases[i] = alias;
        }

        return (string.Join(", ", aliases), names);
    }


    private static Dictionary<string, AttributeValue> BuildLastEvaluatedKey(string pk, string sk, KeySchemaInfo keyInfo)
    {
        var hashKey = keyInfo.KeySchema.First(static k => k.KeyType == KeyType.HASH);
        var rangeKey = keyInfo.KeySchema.FirstOrDefault(static k => k.KeyType == KeyType.RANGE);

        var result = new Dictionary<string, AttributeValue>
        {
            [hashKey.AttributeName] = BuildKeyAttributeValue(pk, keyInfo.AttributeDefinitions.First(a => a.AttributeName == hashKey.AttributeName).AttributeType)
        };

        if (rangeKey is not null)
            result[rangeKey.AttributeName] = BuildKeyAttributeValue(sk, keyInfo.AttributeDefinitions.First(a => a.AttributeName == rangeKey.AttributeName).AttributeType);

        return result;
    }

    private static AttributeValue BuildKeyAttributeValue(string value, ScalarAttributeType type) =>
        type.Value switch
        {
            "S" => new AttributeValue { S = value },
            "N" => new AttributeValue { N = value },
            "B" => new AttributeValue { B = new MemoryStream(Convert.FromBase64String(value)) },
            _ => throw new ArgumentException($"Unsupported key attribute type: {type.Value}")
        };

    private static (string FilterExpression, Dictionary<string, string> AttrNames, Dictionary<string, AttributeValue> AttrValues)
        ConvertConditionsToExpression(Dictionary<string, Condition> conditions, string prefix = "legacy")
    {
        var expressions = new List<string>(conditions.Count);
        var attrNames = new Dictionary<string, string>(conditions.Count);
        var attrValues = new Dictionary<string, AttributeValue>(conditions.Count);
        var i = 0;

        foreach (var (attributeName, condition) in conditions)
        {
            var nameKey = BuildLegacyNameKey(prefix, i);
            attrNames[nameKey] = attributeName;

            var expr = condition.ComparisonOperator.Value switch
            {
                "EQ" => BuildSingleValueCondition(nameKey, "=", prefix, i, condition, attrValues),
                "NE" => BuildSingleValueCondition(nameKey, "<>", prefix, i, condition, attrValues),
                "LT" => BuildSingleValueCondition(nameKey, "<", prefix, i, condition, attrValues),
                "LE" => BuildSingleValueCondition(nameKey, "<=", prefix, i, condition, attrValues),
                "GT" => BuildSingleValueCondition(nameKey, ">", prefix, i, condition, attrValues),
                "GE" => BuildSingleValueCondition(nameKey, ">=", prefix, i, condition, attrValues),
                "BEGINS_WITH" => BuildBeginsWithCondition(nameKey, prefix, i, condition, attrValues),
                "CONTAINS" => BuildContainsCondition(nameKey, prefix, i, condition, attrValues),
                "BETWEEN" => BuildBetweenCondition(nameKey, prefix, i, condition, attrValues),
                "NOT_NULL" => $"attribute_exists({nameKey})",
                "NULL" => $"attribute_not_exists({nameKey})",
                _ => throw new ArgumentException($"Unsupported comparison operator: {condition.ComparisonOperator.Value}")
            };

            expressions.Add(expr);
            i++;
        }

        return (string.Join(" AND ", expressions), attrNames, attrValues);
    }

    private static string BuildSingleValueCondition(
        string nameKey, string op, string prefix, int index,
        Condition condition, Dictionary<string, AttributeValue> attrValues)
    {
        var valueKey = BuildLegacyValueKey(prefix, index);
        attrValues[valueKey] = condition.AttributeValueList[0];
        return $"{nameKey} {op} {valueKey}";
    }

    private static string BuildBeginsWithCondition(
        string nameKey, string prefix, int index,
        Condition condition, Dictionary<string, AttributeValue> attrValues)
    {
        var valueKey = BuildLegacyValueKey(prefix, index);
        attrValues[valueKey] = condition.AttributeValueList[0];
        return $"begins_with({nameKey}, {valueKey})";
    }

    private static string BuildContainsCondition(
        string nameKey, string prefix, int index,
        Condition condition, Dictionary<string, AttributeValue> attrValues)
    {
        var valueKey = BuildLegacyValueKey(prefix, index);
        attrValues[valueKey] = condition.AttributeValueList[0];
        return $"contains({nameKey}, {valueKey})";
    }

    private static string BuildBetweenCondition(
        string nameKey, string prefix, int index,
        Condition condition, Dictionary<string, AttributeValue> attrValues)
    {
        var lowKey = BuildLegacyValueKey(prefix, index, 'a');
        var highKey = BuildLegacyValueKey(prefix, index, 'b');
        attrValues[lowKey] = condition.AttributeValueList[0];
        attrValues[highKey] = condition.AttributeValueList[1];
        return $"{nameKey} BETWEEN {lowKey} AND {highKey}";
    }

    // Builds "#{prefix}N{index}" without intermediate allocations.
    private static string BuildLegacyNameKey(string prefix, int index) =>
        string.Create(2 + prefix.Length + CountDigits(index), (prefix, index), static (span, state) =>
        {
            span[0] = '#';
            state.prefix.AsSpan().CopyTo(span[1..]);
            span[1 + state.prefix.Length] = 'N';
            _ = state.index.TryFormat(span[(2 + state.prefix.Length)..], out _, provider: CultureInfo.InvariantCulture);
        });

    // Builds ":{prefix}V{index}" or ":{prefix}V{index}{suffix}" without intermediate allocations.
    private static string BuildLegacyValueKey(string prefix, int index, char suffix = '\0')
    {
        var hasSuffix = suffix != '\0';
        var len = 2 + prefix.Length + CountDigits(index) + (hasSuffix ? 1 : 0);
        return string.Create(len, (prefix, index, suffix, hasSuffix), static (span, state) =>
        {
            span[0] = ':';
            state.prefix.AsSpan().CopyTo(span[1..]);
            span[1 + state.prefix.Length] = 'V';
            var digitStart = 2 + state.prefix.Length;
            _ = state.index.TryFormat(span[digitStart..], out var digitsWritten, provider: CultureInfo.InvariantCulture);
            if (state.hasSuffix)
                span[digitStart + digitsWritten] = state.suffix;
        });
    }

    private static int CountDigits(int n) =>
        n < 10 ? 1
        : n < 100 ? 2
        : n < 1000 ? 3
        : n < 10000 ? 4
        : n < 100000 ? 5
        : n < 1000000 ? 6
        : n < 10000000 ? 7
        : n < 100000000 ? 8
        : n < 1000000000 ? 9
        : 10;

    private static void ConvertKeyConditionsToExpression(QueryRequest request)
    {
        var (expression, attrNames, attrValues) = ConvertConditionsToExpression(request.KeyConditions);
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
        var (expression, attrNames, attrValues) = ConvertConditionsToExpression(request.QueryFilter, "qf");

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

    private static Dictionary<string, AttributeValue> BuildIndexLastEvaluatedKey(
        IndexItemRow lastRow,
        KeySchemaInfo indexKeyInfo,
        KeySchemaInfo tableKeyInfo)
    {
        var result = new Dictionary<string, AttributeValue>();

        // Add index keys
        var indexHashKey = indexKeyInfo.KeySchema.First(static k => k.KeyType == KeyType.HASH);
        result[indexHashKey.AttributeName] = BuildKeyAttributeValue(
            lastRow.Pk,
            indexKeyInfo.AttributeDefinitions.First(a => a.AttributeName == indexHashKey.AttributeName).AttributeType);

        var indexRangeKey = indexKeyInfo.KeySchema.FirstOrDefault(static k => k.KeyType == KeyType.RANGE);
        if (indexRangeKey is not null)
            result[indexRangeKey.AttributeName] = BuildKeyAttributeValue(
                lastRow.Sk,
                indexKeyInfo.AttributeDefinitions.First(a => a.AttributeName == indexRangeKey.AttributeName).AttributeType);

        // Add table keys
        var tableHashKey = tableKeyInfo.KeySchema.First(static k => k.KeyType == KeyType.HASH);
        if (!result.ContainsKey(tableHashKey.AttributeName))
            result[tableHashKey.AttributeName] = BuildKeyAttributeValue(
                lastRow.TablePk,
                tableKeyInfo.AttributeDefinitions.First(a => a.AttributeName == tableHashKey.AttributeName).AttributeType);

        var tableRangeKey = tableKeyInfo.KeySchema.FirstOrDefault(static k => k.KeyType == KeyType.RANGE);
        if (tableRangeKey is not null && !result.ContainsKey(tableRangeKey.AttributeName))
            result[tableRangeKey.AttributeName] = BuildKeyAttributeValue(
                lastRow.TableSk,
                tableKeyInfo.AttributeDefinitions.First(a => a.AttributeName == tableRangeKey.AttributeName).AttributeType);

        return result;
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
