using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite.Tests;

public sealed class TableManagementTests
    : IAsyncLifetime
{
    private readonly DynamoDbClient client = new(new DynamoDbLiteOptions(
        $"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        client.Dispose();
        return ValueTask.CompletedTask;
    }

    // ── CreateTableAsync ───────────────────────────────────────────────

    [Fact]
    public async Task CreateTableAsync_WithHashKey_ReturnsActiveTable()
    {
        var response = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "Users",
            KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
            AttributeDefinitions = [new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }],
            ProvisionedThroughput = new ProvisionedThroughput { ReadCapacityUnits = 5, WriteCapacityUnits = 5 }
        }, TestContext.Current.CancellationToken);

        Assert.Equal("Users", response.TableDescription.TableName);
        Assert.Equal(TableStatus.ACTIVE, response.TableDescription.TableStatus);
        Assert.Equal(System.Net.HttpStatusCode.OK, response.HttpStatusCode);
    }

    [Fact]
    public async Task CreateTableAsync_WithHashAndRangeKey_ReturnsCorrectKeySchema()
    {
        var response = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "Orders",
            KeySchema =
            [
                new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S }
            ],
            ProvisionedThroughput = new ProvisionedThroughput { ReadCapacityUnits = 5, WriteCapacityUnits = 5 }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.TableDescription.KeySchema.Count);
        Assert.Contains(response.TableDescription.KeySchema, k => k.AttributeName == "PK" && k.KeyType == KeyType.HASH);
        Assert.Contains(response.TableDescription.KeySchema, k => k.AttributeName == "SK" && k.KeyType == KeyType.RANGE);
    }

    [Fact]
    public async Task CreateTableAsync_WithSimpleOverload_Succeeds()
    {
        var response = await client.CreateTableAsync(
            "Products",
            [new KeySchemaElement { AttributeName = "Id", KeyType = KeyType.HASH }],
            [new AttributeDefinition { AttributeName = "Id", AttributeType = ScalarAttributeType.S }],
            new ProvisionedThroughput { ReadCapacityUnits = 1, WriteCapacityUnits = 1 },
            TestContext.Current.CancellationToken);

        Assert.Equal("Products", response.TableDescription.TableName);
        Assert.Equal(TableStatus.ACTIVE, response.TableDescription.TableStatus);
    }

    [Fact]
    public async Task CreateTableAsync_DuplicateTable_ThrowsResourceInUseException()
    {
        _ = await CreateSimpleTableAsync("Dupes");

        _ = await Assert.ThrowsAsync<ResourceInUseException>(() =>
            client.CreateTableAsync(new CreateTableRequest
            {
                TableName = "Dupes",
                KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
                AttributeDefinitions = [new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }]
            }, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task CreateTableAsync_NoHashKey_ThrowsAmazonDynamoDBException()
        => _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.CreateTableAsync(new CreateTableRequest
            {
                TableName = "Bad",
                KeySchema = [new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }],
                AttributeDefinitions = [new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S }]
            }, TestContext.Current.CancellationToken));

    [Fact]
    public async Task CreateTableAsync_KeyNotInAttributeDefinitions_ThrowsAmazonDynamoDBException()
        => _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.CreateTableAsync(new CreateTableRequest
            {
                TableName = "Bad",
                KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
                AttributeDefinitions = [new AttributeDefinition { AttributeName = "Other", AttributeType = ScalarAttributeType.S }]
            }, TestContext.Current.CancellationToken));

    [Fact]
    public async Task CreateTableAsync_SetsProvisionedThroughput()
    {
        var response = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "Throughput",
            KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
            AttributeDefinitions = [new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }],
            ProvisionedThroughput = new ProvisionedThroughput { ReadCapacityUnits = 10, WriteCapacityUnits = 20 }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(10, response.TableDescription.ProvisionedThroughput.ReadCapacityUnits);
        Assert.Equal(20, response.TableDescription.ProvisionedThroughput.WriteCapacityUnits);
    }

    [Fact]
    public async Task CreateTableAsync_SetsTableArn()
    {
        var response = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "ArnTest",
            KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
            AttributeDefinitions = [new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }]
        }, TestContext.Current.CancellationToken);

        Assert.Equal("arn:aws:dynamodb:local:000000000000:table/ArnTest", response.TableDescription.TableArn);
    }

    // ── DeleteTableAsync ───────────────────────────────────────────────

    [Fact]
    public async Task DeleteTableAsync_ExistingTable_ReturnsDeletingStatus()
    {
        _ = await CreateSimpleTableAsync("ToDelete");

        var response = await client.DeleteTableAsync("ToDelete", TestContext.Current.CancellationToken);

        Assert.Equal("ToDelete", response.TableDescription.TableName);
        Assert.Equal(TableStatus.DELETING, response.TableDescription.TableStatus);
    }

    [Fact]
    public async Task DeleteTableAsync_ExistingTable_RemovesFromStore()
    {
        _ = await CreateSimpleTableAsync("ToRemove");

        _ = await client.DeleteTableAsync("ToRemove", TestContext.Current.CancellationToken);

        _ = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            client.DescribeTableAsync("ToRemove", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DeleteTableAsync_NonExistentTable_ThrowsResourceNotFoundException()
        => _ = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            client.DeleteTableAsync("DoesNotExist", TestContext.Current.CancellationToken));

    // ── DescribeTableAsync ─────────────────────────────────────────────

    [Fact]
    public async Task DescribeTableAsync_ExistingTable_ReturnsDescription()
    {
        _ = await CreateSimpleTableAsync("Described");

        var response = await client.DescribeTableAsync("Described", TestContext.Current.CancellationToken);

        Assert.Equal("Described", response.Table.TableName);
        Assert.Equal(TableStatus.ACTIVE, response.Table.TableStatus);
        _ = Assert.Single(response.Table.KeySchema);
        _ = Assert.Single(response.Table.AttributeDefinitions);
        Assert.True(response.Table.CreationDateTime > DateTime.MinValue);
    }

    [Fact]
    public async Task DescribeTableAsync_NonExistentTable_ThrowsResourceNotFoundException()
        => _ = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            client.DescribeTableAsync("Ghost", TestContext.Current.CancellationToken));

    // ── ListTablesAsync ────────────────────────────────────────────────

    [Fact]
    public async Task ListTablesAsync_NoTables_ReturnsEmptyList()
    {
        var response = await client.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Empty(response.TableNames);
    }

    [Fact]
    public async Task ListTablesAsync_MultipleTables_ReturnsAlphabeticalOrder()
    {
        _ = await CreateSimpleTableAsync("Charlie");
        _ = await CreateSimpleTableAsync("Alpha");
        _ = await CreateSimpleTableAsync("Bravo");

        var response = await client.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Equal(["Alpha", "Bravo", "Charlie"], response.TableNames);
    }

    [Fact]
    public async Task ListTablesAsync_WithLimit_RespectsLimit()
    {
        _ = await CreateSimpleTableAsync("A");
        _ = await CreateSimpleTableAsync("B");
        _ = await CreateSimpleTableAsync("C");

        var response = await client.ListTablesAsync(2, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.TableNames.Count);
        Assert.Equal(["A", "B"], response.TableNames);
        Assert.NotNull(response.LastEvaluatedTableName);
    }

    [Fact]
    public async Task ListTablesAsync_Pagination_ReturnsNextPage()
    {
        _ = await CreateSimpleTableAsync("A");
        _ = await CreateSimpleTableAsync("B");
        _ = await CreateSimpleTableAsync("C");

        var firstPage = await client.ListTablesAsync(2, TestContext.Current.CancellationToken);
        var secondPage = await client.ListTablesAsync(firstPage.LastEvaluatedTableName!, 2, TestContext.Current.CancellationToken);

        Assert.Equal(["C"], secondPage.TableNames);
        Assert.Null(secondPage.LastEvaluatedTableName);
    }

    [Fact]
    public async Task ListTablesAsync_AllFitInOnePage_NoLastEvaluatedTableName()
    {
        _ = await CreateSimpleTableAsync("Only");

        var response = await client.ListTablesAsync(TestContext.Current.CancellationToken);

        _ = Assert.Single(response.TableNames);
        Assert.Null(response.LastEvaluatedTableName);
    }

    // ── Disposal ───────────────────────────────────────────────────────

    [Fact]
    public async Task Operations_AfterDispose_ThrowObjectDisposedException()
    {
        client.Dispose();

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.CreateTableAsync(new CreateTableRequest { TableName = "X" }, TestContext.Current.CancellationToken));

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.DeleteTableAsync("X", TestContext.Current.CancellationToken));

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.DescribeTableAsync("X", TestContext.Current.CancellationToken));

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.ListTablesAsync(TestContext.Current.CancellationToken));
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private Task<CreateTableResponse> CreateSimpleTableAsync(string tableName) =>
        client.CreateTableAsync(new CreateTableRequest
        {
            TableName = tableName,
            KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
            AttributeDefinitions = [new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }]
        }, TestContext.Current.CancellationToken);
}
