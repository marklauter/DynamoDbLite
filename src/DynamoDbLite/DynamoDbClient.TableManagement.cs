using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

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

        if (await store.TableExistsAsync(request.TableName, cancellationToken))
            throw new ResourceInUseException($"Table already exists: {request.TableName}");

        await store.CreateTableAsync(
            request.TableName,
            request.KeySchema,
            request.AttributeDefinitions,
            request.ProvisionedThroughput,
            cancellationToken);

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
}
