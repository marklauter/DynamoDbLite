using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite;

public sealed partial class DynamoDbClient
{
    public async Task<QueryResponse> QueryAsync(QueryRequest request, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableName);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.KeyConditionExpression);

        var keyInfo = await store.GetKeySchemaAsync(request.TableName, cancellationToken)
            ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        var keyCondition = Expressions.KeyConditionExpressionParser.Parse(request.KeyConditionExpression);
        var sql = KeyConditionSqlBuilder.Build(
            keyCondition, keyInfo, request.ExpressionAttributeNames, request.ExpressionAttributeValues);

        var ascending = request.ScanIndexForward is not false;

        string? exclusiveStartSk = null;
        if (request.ExclusiveStartKey is { Count: > 0 })
        {
            var (_, esSk) = KeyHelper.ExtractKeys(request.ExclusiveStartKey, keyInfo.KeySchema, keyInfo.AttributeDefinitions);
            exclusiveStartSk = esSk;
        }

        var rows = await store.QueryItemsAsync(
            request.TableName, sql.PkValue, sql.SkWhereSql, sql.SkParams,
            sql.OrderByColumn, ascending, request.Limit, exclusiveStartSk, cancellationToken);

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
            response.LastEvaluatedKey = BuildLastEvaluatedKey(lastRow.Pk, lastRow.Sk, keyInfo);
        }

        return response;
    }

    public async Task<ScanResponse> ScanAsync(ScanRequest request, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableName);

        var keyInfo = await store.GetKeySchemaAsync(request.TableName, cancellationToken)
            ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        string? exclusiveStartPk = null;
        string? exclusiveStartSk = null;
        if (request.ExclusiveStartKey is { Count: > 0 })
        {
            var (esPk, esSk) = KeyHelper.ExtractKeys(request.ExclusiveStartKey, keyInfo.KeySchema, keyInfo.AttributeDefinitions);
            exclusiveStartPk = esPk;
            exclusiveStartSk = esSk;
        }

        var rows = await store.ScanItemsAsync(
            request.TableName, request.Limit, exclusiveStartPk, exclusiveStartSk, cancellationToken);

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
            response.LastEvaluatedKey = BuildLastEvaluatedKey(lastRow.Pk, lastRow.Sk, keyInfo);
        }

        return response;
    }

    // ── Legacy ScanAsync overloads ──────────────────────────────────

    public Task<ScanResponse> ScanAsync(
        string tableName,
        List<string> attributesToGet,
        CancellationToken cancellationToken = default) =>
        ScanAsync(new ScanRequest
        {
            TableName = tableName,
            ProjectionExpression = string.Join(", ", attributesToGet),
        }, cancellationToken);

    public Task<ScanResponse> ScanAsync(
        string tableName,
        Dictionary<string, Condition> scanFilter,
        CancellationToken cancellationToken = default)
    {
        var (filterExpression, attrNames, attrValues) = ConvertConditionsToExpression(scanFilter);
        return ScanAsync(new ScanRequest
        {
            TableName = tableName,
            FilterExpression = filterExpression,
            ExpressionAttributeNames = attrNames,
            ExpressionAttributeValues = attrValues,
        }, cancellationToken);
    }

    public Task<ScanResponse> ScanAsync(
        string tableName,
        List<string> attributesToGet,
        Dictionary<string, Condition> scanFilter,
        CancellationToken cancellationToken = default)
    {
        var (filterExpression, attrNames, attrValues) = ConvertConditionsToExpression(scanFilter);
        return ScanAsync(new ScanRequest
        {
            TableName = tableName,
            ProjectionExpression = string.Join(", ", attributesToGet),
            FilterExpression = filterExpression,
            ExpressionAttributeNames = attrNames,
            ExpressionAttributeValues = attrValues,
        }, cancellationToken);
    }

    // ── Helpers ─────────────────────────────────────────────────────

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
        ConvertConditionsToExpression(Dictionary<string, Condition> conditions)
    {
        var expressions = new List<string>();
        var attrNames = new Dictionary<string, string>();
        var attrValues = new Dictionary<string, AttributeValue>();
        var i = 0;

        foreach (var (attributeName, condition) in conditions)
        {
            var nameKey = $"#legacyN{i}";
            attrNames[nameKey] = attributeName;

            var expr = condition.ComparisonOperator.Value switch
            {
                "EQ" => BuildSingleValueCondition(nameKey, "=", i, condition, attrValues),
                "NE" => BuildSingleValueCondition(nameKey, "<>", i, condition, attrValues),
                "LT" => BuildSingleValueCondition(nameKey, "<", i, condition, attrValues),
                "LE" => BuildSingleValueCondition(nameKey, "<=", i, condition, attrValues),
                "GT" => BuildSingleValueCondition(nameKey, ">", i, condition, attrValues),
                "GE" => BuildSingleValueCondition(nameKey, ">=", i, condition, attrValues),
                "BEGINS_WITH" => BuildBeginsWithCondition(nameKey, i, condition, attrValues),
                "CONTAINS" => BuildContainsCondition(nameKey, i, condition, attrValues),
                "BETWEEN" => BuildBetweenCondition(nameKey, i, condition, attrValues),
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
        string nameKey, string op, int index,
        Condition condition, Dictionary<string, AttributeValue> attrValues)
    {
        var valueKey = $":legacyV{index}";
        attrValues[valueKey] = condition.AttributeValueList[0];
        return $"{nameKey} {op} {valueKey}";
    }

    private static string BuildBeginsWithCondition(
        string nameKey, int index,
        Condition condition, Dictionary<string, AttributeValue> attrValues)
    {
        var valueKey = $":legacyV{index}";
        attrValues[valueKey] = condition.AttributeValueList[0];
        return $"begins_with({nameKey}, {valueKey})";
    }

    private static string BuildContainsCondition(
        string nameKey, int index,
        Condition condition, Dictionary<string, AttributeValue> attrValues)
    {
        var valueKey = $":legacyV{index}";
        attrValues[valueKey] = condition.AttributeValueList[0];
        return $"contains({nameKey}, {valueKey})";
    }

    private static string BuildBetweenCondition(
        string nameKey, int index,
        Condition condition, Dictionary<string, AttributeValue> attrValues)
    {
        var lowKey = $":legacyV{index}a";
        var highKey = $":legacyV{index}b";
        attrValues[lowKey] = condition.AttributeValueList[0];
        attrValues[highKey] = condition.AttributeValueList[1];
        return $"{nameKey} BETWEEN {lowKey} AND {highKey}";
    }
}
