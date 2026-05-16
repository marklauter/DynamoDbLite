using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite.Parity.Tests.Fixtures;

internal static class TestTables
{
    public static string UniqueName(string prefix) =>
        $"{prefix}_{Guid.NewGuid():N}"[..(prefix.Length + 9)];

    public static CreateTableRequest HashKeyString(string tableName) => new()
    {
        TableName = tableName,
        KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
        AttributeDefinitions = [new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }],
        BillingMode = BillingMode.PAY_PER_REQUEST,
    };

    public static CreateTableRequest HashKeyStringSortKeyString(string tableName) => new()
    {
        TableName = tableName,
        KeySchema =
        [
            new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
            new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE },
        ],
        AttributeDefinitions =
        [
            new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
            new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S },
        ],
        BillingMode = BillingMode.PAY_PER_REQUEST,
    };

    public static CreateTableRequest HashKeyStringSortKeyNumber(string tableName) => new()
    {
        TableName = tableName,
        KeySchema =
        [
            new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
            new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE },
        ],
        AttributeDefinitions =
        [
            new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
            new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.N },
        ],
        BillingMode = BillingMode.PAY_PER_REQUEST,
    };

    public static CreateTableRequest HashKeyStringSortKeyStringWithGsi(string tableName, string indexName) => new()
    {
        TableName = tableName,
        KeySchema =
        [
            new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
            new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE },
        ],
        AttributeDefinitions =
        [
            new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
            new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S },
            new AttributeDefinition { AttributeName = "GsiPK", AttributeType = ScalarAttributeType.S },
            new AttributeDefinition { AttributeName = "GsiSK", AttributeType = ScalarAttributeType.S },
        ],
        GlobalSecondaryIndexes =
        [
            new GlobalSecondaryIndex
            {
                IndexName = indexName,
                KeySchema =
                [
                    new KeySchemaElement { AttributeName = "GsiPK", KeyType = KeyType.HASH },
                    new KeySchemaElement { AttributeName = "GsiSK", KeyType = KeyType.RANGE },
                ],
                Projection = new Projection
                {
                    ProjectionType = ProjectionType.INCLUDE,
                    NonKeyAttributes = ["projected"],
                },
            },
        ],
        BillingMode = BillingMode.PAY_PER_REQUEST,
    };

    public static CreateTableRequest HashKeyStringSortKeyStringWithGsiProjection(string tableName, string indexName, ProjectionType projectionType) => new()
    {
        TableName = tableName,
        KeySchema =
        [
            new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
            new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE },
        ],
        AttributeDefinitions =
        [
            new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
            new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S },
            new AttributeDefinition { AttributeName = "GsiPK", AttributeType = ScalarAttributeType.S },
            new AttributeDefinition { AttributeName = "GsiSK", AttributeType = ScalarAttributeType.S },
        ],
        GlobalSecondaryIndexes =
        [
            new GlobalSecondaryIndex
            {
                IndexName = indexName,
                KeySchema =
                [
                    new KeySchemaElement { AttributeName = "GsiPK", KeyType = KeyType.HASH },
                    new KeySchemaElement { AttributeName = "GsiSK", KeyType = KeyType.RANGE },
                ],
                Projection = projectionType == ProjectionType.INCLUDE
                    ? new Projection { ProjectionType = ProjectionType.INCLUDE, NonKeyAttributes = ["projected"] }
                    : new Projection { ProjectionType = projectionType },
            },
        ],
        BillingMode = BillingMode.PAY_PER_REQUEST,
    };

    public static CreateTableRequest HashKeyStringSortKeyStringWithLsi(string tableName, string indexName) => new()
    {
        TableName = tableName,
        KeySchema =
        [
            new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
            new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE },
        ],
        AttributeDefinitions =
        [
            new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
            new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S },
            new AttributeDefinition { AttributeName = "LsiSK", AttributeType = ScalarAttributeType.S },
        ],
        LocalSecondaryIndexes =
        [
            new LocalSecondaryIndex
            {
                IndexName = indexName,
                KeySchema =
                [
                    new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                    new KeySchemaElement { AttributeName = "LsiSK", KeyType = KeyType.RANGE },
                ],
                Projection = new Projection
                {
                    ProjectionType = ProjectionType.INCLUDE,
                    NonKeyAttributes = ["projected"],
                },
            },
        ],
        BillingMode = BillingMode.PAY_PER_REQUEST,
    };

    public static async Task CreateAndWaitAsync(IAmazonDynamoDB client, CreateTableRequest request, CancellationToken ct)
    {
        _ = await client.CreateTableAsync(request, ct);
        await WaitForActiveAsync(client, request.TableName, ct);
    }

    private static async Task WaitForActiveAsync(IAmazonDynamoDB client, string tableName, CancellationToken ct)
    {
        for (var i = 0; i < 50; i++)
        {
            var response = await client.DescribeTableAsync(tableName, ct);
            if (response.Table.TableStatus == TableStatus.ACTIVE)
                return;
            await Task.Delay(100, ct);
        }

        throw new TimeoutException($"Table {tableName} did not become ACTIVE within 5 seconds.");
    }
}
