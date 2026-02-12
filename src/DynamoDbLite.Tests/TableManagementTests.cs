using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

public sealed class TableManagementTests
    : DynamoDbClientFixture
{
    // ── CreateTableAsync ───────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task CreateTableAsync_WithHashKey_ReturnsActiveTable(StoreType st)
    {
        var client = Client(st);

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

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task CreateTableAsync_WithHashAndRangeKey_ReturnsCorrectKeySchema(StoreType st)
    {
        var client = Client(st);

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

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task CreateTableAsync_WithSimpleOverload_Succeeds(StoreType st)
    {
        var client = Client(st);

        var response = await client.CreateTableAsync(
            "Products",
            [new KeySchemaElement { AttributeName = "Id", KeyType = KeyType.HASH }],
            [new AttributeDefinition { AttributeName = "Id", AttributeType = ScalarAttributeType.S }],
            new ProvisionedThroughput { ReadCapacityUnits = 1, WriteCapacityUnits = 1 },
            TestContext.Current.CancellationToken);

        Assert.Equal("Products", response.TableDescription.TableName);
        Assert.Equal(TableStatus.ACTIVE, response.TableDescription.TableStatus);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task CreateTableAsync_DuplicateTable_ThrowsResourceInUseException(StoreType st)
    {
        var client = Client(st);
        _ = await CreateSimpleTableAsync(client, "Dupes");

        _ = await Assert.ThrowsAsync<ResourceInUseException>(() =>
            client.CreateTableAsync(new CreateTableRequest
            {
                TableName = "Dupes",
                KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
                AttributeDefinitions = [new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }]
            }, TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task CreateTableAsync_NoHashKey_ThrowsAmazonDynamoDBException(StoreType st)
        => _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            Client(st).CreateTableAsync(new CreateTableRequest
            {
                TableName = "Bad",
                KeySchema = [new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }],
                AttributeDefinitions = [new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S }]
            }, TestContext.Current.CancellationToken));

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task CreateTableAsync_KeyNotInAttributeDefinitions_ThrowsAmazonDynamoDBException(StoreType st)
        => _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            Client(st).CreateTableAsync(new CreateTableRequest
            {
                TableName = "Bad",
                KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
                AttributeDefinitions = [new AttributeDefinition { AttributeName = "Other", AttributeType = ScalarAttributeType.S }]
            }, TestContext.Current.CancellationToken));

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task CreateTableAsync_SetsProvisionedThroughput(StoreType st)
    {
        var client = Client(st);

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

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task CreateTableAsync_SetsTableArn(StoreType st)
    {
        var client = Client(st);

        var response = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "ArnTest",
            KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
            AttributeDefinitions = [new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }]
        }, TestContext.Current.CancellationToken);

        Assert.Equal("arn:aws:dynamodb:local:000000000000:table/ArnTest", response.TableDescription.TableArn);
    }

    // ── DeleteTableAsync ───────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DeleteTableAsync_ExistingTable_ReturnsDeletingStatus(StoreType st)
    {
        var client = Client(st);
        _ = await CreateSimpleTableAsync(client, "ToDelete");

        var response = await client.DeleteTableAsync("ToDelete", TestContext.Current.CancellationToken);

        Assert.Equal("ToDelete", response.TableDescription.TableName);
        Assert.Equal(TableStatus.DELETING, response.TableDescription.TableStatus);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DeleteTableAsync_ExistingTable_RemovesFromStore(StoreType st)
    {
        var client = Client(st);
        _ = await CreateSimpleTableAsync(client, "ToRemove");

        _ = await client.DeleteTableAsync("ToRemove", TestContext.Current.CancellationToken);

        _ = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            client.DescribeTableAsync("ToRemove", TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DeleteTableAsync_NonExistentTable_ThrowsResourceNotFoundException(StoreType st)
        => _ = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            Client(st).DeleteTableAsync("DoesNotExist", TestContext.Current.CancellationToken));

    // ── DescribeTableAsync ─────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DescribeTableAsync_ExistingTable_ReturnsDescription(StoreType st)
    {
        var client = Client(st);
        _ = await CreateSimpleTableAsync(client, "Described");

        var response = await client.DescribeTableAsync("Described", TestContext.Current.CancellationToken);

        Assert.Equal("Described", response.Table.TableName);
        Assert.Equal(TableStatus.ACTIVE, response.Table.TableStatus);
        _ = Assert.Single(response.Table.KeySchema);
        _ = Assert.Single(response.Table.AttributeDefinitions);
        Assert.True(response.Table.CreationDateTime > DateTime.MinValue);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DescribeTableAsync_NonExistentTable_ThrowsResourceNotFoundException(StoreType st)
        => _ = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            Client(st).DescribeTableAsync("Ghost", TestContext.Current.CancellationToken));

    // ── ListTablesAsync ────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ListTablesAsync_NoTables_ReturnsEmptyList(StoreType st)
    {
        var client = Client(st);

        var response = await client.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Empty(response.TableNames);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ListTablesAsync_MultipleTables_ReturnsAlphabeticalOrder(StoreType st)
    {
        var client = Client(st);
        _ = await CreateSimpleTableAsync(client, "Charlie");
        _ = await CreateSimpleTableAsync(client, "Alpha");
        _ = await CreateSimpleTableAsync(client, "Bravo");

        var response = await client.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Equal(["Alpha", "Bravo", "Charlie"], response.TableNames);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ListTablesAsync_WithLimit_RespectsLimit(StoreType st)
    {
        var client = Client(st);
        _ = await CreateSimpleTableAsync(client, "A");
        _ = await CreateSimpleTableAsync(client, "B");
        _ = await CreateSimpleTableAsync(client, "C");

        var response = await client.ListTablesAsync(2, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.TableNames.Count);
        Assert.Equal(["A", "B"], response.TableNames);
        Assert.NotNull(response.LastEvaluatedTableName);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ListTablesAsync_Pagination_ReturnsNextPage(StoreType st)
    {
        var client = Client(st);
        _ = await CreateSimpleTableAsync(client, "A");
        _ = await CreateSimpleTableAsync(client, "B");
        _ = await CreateSimpleTableAsync(client, "C");

        var firstPage = await client.ListTablesAsync(2, TestContext.Current.CancellationToken);
        var secondPage = await client.ListTablesAsync(firstPage.LastEvaluatedTableName!, 2, TestContext.Current.CancellationToken);

        Assert.Equal(["C"], secondPage.TableNames);
        Assert.Null(secondPage.LastEvaluatedTableName);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ListTablesAsync_AllFitInOnePage_NoLastEvaluatedTableName(StoreType st)
    {
        var client = Client(st);
        _ = await CreateSimpleTableAsync(client, "Only");

        var response = await client.ListTablesAsync(TestContext.Current.CancellationToken);

        _ = Assert.Single(response.TableNames);
        Assert.Null(response.LastEvaluatedTableName);
    }

    // ── Disposal ───────────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task Operations_AfterDispose_ThrowObjectDisposedException(StoreType st)
    {
#pragma warning disable IDISP016, IDISP017
        var client = Client(st);
        client.Dispose();

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.CreateTableAsync(new CreateTableRequest { TableName = "X" }, TestContext.Current.CancellationToken));

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.DeleteTableAsync("X", TestContext.Current.CancellationToken));

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.DescribeTableAsync("X", TestContext.Current.CancellationToken));

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.ListTablesAsync(TestContext.Current.CancellationToken));
#pragma warning restore IDISP016, IDISP017
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private static Task<CreateTableResponse> CreateSimpleTableAsync(DynamoDbClient client, string tableName) =>
        client.CreateTableAsync(new CreateTableRequest
        {
            TableName = tableName,
            KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
            AttributeDefinitions = [new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }]
        }, TestContext.Current.CancellationToken);
}
