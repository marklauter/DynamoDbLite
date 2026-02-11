using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.SqliteStores.Models;

namespace DynamoDbLite;

public sealed partial class DynamoDbClient
{
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

        var gsiDefs = ValidateAndConvertGsiDefinitions(request.GlobalSecondaryIndexes, request.KeySchema, request.AttributeDefinitions);
        var lsiDefs = ValidateAndConvertLsiDefinitions(request.LocalSecondaryIndexes, request.KeySchema, request.AttributeDefinitions);

        ValidateAttributeDefinitionsCoverage(request.KeySchema, request.GlobalSecondaryIndexes, request.LocalSecondaryIndexes, request.AttributeDefinitions);

        if (await store.TableExistsAsync(request.TableName, cancellationToken))
            throw new ResourceInUseException($"Table already exists: {request.TableName}");

        await store.CreateTableAsync(
            request.TableName,
            request.KeySchema,
            request.AttributeDefinitions,
            request.ProvisionedThroughput,
            gsiDefs,
            lsiDefs,
            cancellationToken);

        if (request.Tags is { Count: > 0 })
        {
            ValidateTags(request.Tags);
            if (request.Tags.Count > MaxTagsPerResource)
                throw new AmazonDynamoDBException(
                    $"One or more parameter values were invalid: Too many tags: {request.Tags.Count}, maximum is {MaxTagsPerResource}");
            await store.SetTagsAsync(request.TableName,
                [.. request.Tags.Select(static t => (t.Key, t.Value))], cancellationToken);
        }

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

    private static List<IndexDefinition>? ValidateAndConvertGsiDefinitions(
        List<GlobalSecondaryIndex>? gsis,
        List<KeySchemaElement> tableKeySchema,
        List<AttributeDefinition> attributeDefinitions)
    {
        if (gsis is not { Count: > 0 })
            return null;

        if (gsis.Count > 5)
            throw new AmazonDynamoDBException(
                "One or more parameter values were invalid: GlobalSecondaryIndex count exceeds the per-table limit of 5");

        var indexNames = new HashSet<string>();
        var result = new List<IndexDefinition>(gsis.Count);

        foreach (var gsi in gsis)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(gsi.IndexName);

            if (!indexNames.Add(gsi.IndexName))
                throw new AmazonDynamoDBException(
                    $"One or more parameter values were invalid: Duplicate index name: {gsi.IndexName}");

            ValidateIndexKeySchema(gsi.KeySchema, attributeDefinitions, gsi.IndexName);

            var projectionType = gsi.Projection?.ProjectionType?.Value ?? "ALL";
            var nonKeyAttrs = gsi.Projection?.NonKeyAttributes;

            result.Add(new IndexDefinition(
                gsi.IndexName,
                IsGlobal: true,
                gsi.KeySchema,
                projectionType,
                nonKeyAttrs));
        }

        return result;
    }

    private static List<IndexDefinition>? ValidateAndConvertLsiDefinitions(
        List<LocalSecondaryIndex>? lsis,
        List<KeySchemaElement> tableKeySchema,
        List<AttributeDefinition> attributeDefinitions)
    {
        if (lsis is not { Count: > 0 })
            return null;

        if (lsis.Count > 5)
            throw new AmazonDynamoDBException(
                "One or more parameter values were invalid: LocalSecondaryIndex count exceeds the per-table limit of 5");

        var tableHashKey = tableKeySchema.First(static k => k.KeyType == KeyType.HASH).AttributeName;
        var indexNames = new HashSet<string>();
        var result = new List<IndexDefinition>(lsis.Count);

        foreach (var lsi in lsis)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(lsi.IndexName);

            if (!indexNames.Add(lsi.IndexName))
                throw new AmazonDynamoDBException(
                    $"One or more parameter values were invalid: Duplicate index name: {lsi.IndexName}");

            ValidateIndexKeySchema(lsi.KeySchema, attributeDefinitions, lsi.IndexName);

            var lsiHashKey = lsi.KeySchema.First(static k => k.KeyType == KeyType.HASH).AttributeName;
            if (lsiHashKey != tableHashKey)
                throw new AmazonDynamoDBException(
                    $"One or more parameter values were invalid: Table KeySchema: The AttributeValue for a key attribute for the table must match the AttributeValue for the LSI key schema. Key: {tableHashKey}");

            var projectionType = lsi.Projection?.ProjectionType?.Value ?? "ALL";
            var nonKeyAttrs = lsi.Projection?.NonKeyAttributes;

            result.Add(new IndexDefinition(
                lsi.IndexName,
                IsGlobal: false,
                lsi.KeySchema,
                projectionType,
                nonKeyAttrs));
        }

        return result;
    }

    private static void ValidateIndexKeySchema(
        List<KeySchemaElement> keySchema,
        List<AttributeDefinition> attributeDefinitions,
        string indexName)
    {
        if (keySchema is not { Count: > 0 and <= 2 })
            throw new AmazonDynamoDBException(
                $"One or more parameter values were invalid: Index {indexName} requires 1 or 2 key schema elements");

        var hashKeys = keySchema.Where(static k => k.KeyType == KeyType.HASH).ToList();
        if (hashKeys.Count is not 1)
            throw new AmazonDynamoDBException(
                $"One or more parameter values were invalid: Index {indexName} requires exactly one HASH key");

        var definedNames = attributeDefinitions.Select(static a => a.AttributeName).ToHashSet();
        foreach (var key in keySchema)
        {
            if (!definedNames.Contains(key.AttributeName))
                throw new AmazonDynamoDBException(
                    $"One or more parameter values were invalid: Index key attribute {key.AttributeName} is not defined in AttributeDefinitions");
        }
    }

    private static void ValidateAttributeDefinitionsCoverage(
        List<KeySchemaElement> tableKeySchema,
        List<GlobalSecondaryIndex>? gsis,
        List<LocalSecondaryIndex>? lsis,
        List<AttributeDefinition> attributeDefinitions)
    {
        var referencedAttrs = new HashSet<string>(tableKeySchema.Select(static k => k.AttributeName));

        if (gsis is not null)
            foreach (var gsi in gsis)
                foreach (var key in gsi.KeySchema)
                    _ = referencedAttrs.Add(key.AttributeName);

        if (lsis is not null)
            foreach (var lsi in lsis)
                foreach (var key in lsi.KeySchema)
                    _ = referencedAttrs.Add(key.AttributeName);

        var definedAttrs = attributeDefinitions.Select(static a => a.AttributeName).ToHashSet();

        if (!definedAttrs.SetEquals(referencedAttrs))
        {
            var unused = definedAttrs.Except(referencedAttrs);
            if (unused.Any())
                throw new AmazonDynamoDBException(
                    $"One or more parameter values were invalid: Number of attributes in KeySchema does not match the number defined in AttributeDefinitions");
        }
    }
}
