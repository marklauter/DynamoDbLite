using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

public sealed class SecondaryIndexTests
    : DynamoDbClientFixture
{
    private static async Task CreateTableWithGsiAsync(DynamoDbClient client, string tableName = "TestTable")
        => _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = tableName,
            KeySchema =
            [
                new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "GSI_PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "GSI_SK", AttributeType = ScalarAttributeType.S }
            ],
            GlobalSecondaryIndexes =
            [
                new GlobalSecondaryIndex
                {
                    IndexName = "GSI1",
                    KeySchema =
                    [
                        new KeySchemaElement { AttributeName = "GSI_PK", KeyType = KeyType.HASH },
                        new KeySchemaElement { AttributeName = "GSI_SK", KeyType = KeyType.RANGE }
                    ],
                    Projection = new Projection { ProjectionType = ProjectionType.ALL }
                }
            ]
        }, TestContext.Current.CancellationToken);

    private static async Task CreateTableWithLsiAsync(DynamoDbClient client, string tableName = "TestTable") => _ = await client.CreateTableAsync(new CreateTableRequest
    {
        TableName = tableName,
        KeySchema =
            [
                new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }
            ],
        AttributeDefinitions =
            [
                new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "LSI_SK", AttributeType = ScalarAttributeType.S }
            ],
        LocalSecondaryIndexes =
            [
                new LocalSecondaryIndex
                {
                    IndexName = "LSI1",
                    KeySchema =
                    [
                        new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                        new KeySchemaElement { AttributeName = "LSI_SK", KeyType = KeyType.RANGE }
                    ],
                    Projection = new Projection { ProjectionType = ProjectionType.ALL }
                }
            ]
    }, TestContext.Current.CancellationToken);

    // -- GSI Creation ----------------------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task CreateTable_WithGsi_DescribeShowsGsi(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        var response = await client.DescribeTableAsync("TestTable", TestContext.Current.CancellationToken);

        Assert.NotNull(response.Table.GlobalSecondaryIndexes);
        _ = Assert.Single(response.Table.GlobalSecondaryIndexes);
        var gsi = response.Table.GlobalSecondaryIndexes[0];
        Assert.Equal("GSI1", gsi.IndexName);
        Assert.Equal("ACTIVE", gsi.IndexStatus.Value);
        Assert.Equal(2, gsi.KeySchema.Count);
        Assert.Equal("GSI_PK", gsi.KeySchema.First(k => k.KeyType == KeyType.HASH).AttributeName);
        Assert.Equal("GSI_SK", gsi.KeySchema.First(k => k.KeyType == KeyType.RANGE).AttributeName);
        Assert.Equal(ProjectionType.ALL, gsi.Projection.ProjectionType);
    }

    // -- LSI Creation ----------------------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task CreateTable_WithLsi_DescribeShowsLsi(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithLsiAsync(client);

        var response = await client.DescribeTableAsync("TestTable", TestContext.Current.CancellationToken);

        Assert.NotNull(response.Table.LocalSecondaryIndexes);
        _ = Assert.Single(response.Table.LocalSecondaryIndexes);
        var lsi = response.Table.LocalSecondaryIndexes[0];
        Assert.Equal("LSI1", lsi.IndexName);
        Assert.Equal(2, lsi.KeySchema.Count);
        Assert.Equal("PK", lsi.KeySchema.First(k => k.KeyType == KeyType.HASH).AttributeName);
        Assert.Equal("LSI_SK", lsi.KeySchema.First(k => k.KeyType == KeyType.RANGE).AttributeName);
    }

    // -- Validation: Too many GSIs ---------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task CreateTable_TooManyGsis_Throws(StoreType st)
    {
        var client = Client(st);
        var gsis = Enumerable.Range(1, 6).Select(i => new GlobalSecondaryIndex
        {
            IndexName = $"GSI{i}",
            KeySchema = [new KeySchemaElement { AttributeName = $"GK{i}", KeyType = KeyType.HASH }],
            Projection = new Projection { ProjectionType = ProjectionType.ALL }
        }).ToList();

        var attrDefs = new List<AttributeDefinition>
        {
            new() { AttributeName = "PK", AttributeType = ScalarAttributeType.S }
        };
        attrDefs.AddRange(Enumerable.Range(1, 6).Select(i =>
            new AttributeDefinition { AttributeName = $"GK{i}", AttributeType = ScalarAttributeType.S }));

        _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.CreateTableAsync(new CreateTableRequest
            {
                TableName = "TestTable",
                KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
                AttributeDefinitions = attrDefs,
                GlobalSecondaryIndexes = gsis
            }, TestContext.Current.CancellationToken));
    }

    // -- Validation: Too many LSIs ---------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task CreateTable_TooManyLsis_Throws(StoreType st)
    {
        var client = Client(st);
        var lsis = Enumerable.Range(1, 6).Select(i => new LocalSecondaryIndex
        {
            IndexName = $"LSI{i}",
            KeySchema =
            [
                new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                new KeySchemaElement { AttributeName = $"LK{i}", KeyType = KeyType.RANGE }
            ],
            Projection = new Projection { ProjectionType = ProjectionType.ALL }
        }).ToList();

        var attrDefs = new List<AttributeDefinition>
        {
            new() { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
            new() { AttributeName = "SK", AttributeType = ScalarAttributeType.S }
        };
        attrDefs.AddRange(Enumerable.Range(1, 6).Select(i =>
            new AttributeDefinition { AttributeName = $"LK{i}", AttributeType = ScalarAttributeType.S }));

        _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.CreateTableAsync(new CreateTableRequest
            {
                TableName = "TestTable",
                KeySchema =
                [
                    new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                    new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }
                ],
                AttributeDefinitions = attrDefs,
                LocalSecondaryIndexes = lsis
            }, TestContext.Current.CancellationToken));
    }

    // -- Validation: LSI must share table PK -----------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task CreateTable_LsiDifferentPk_Throws(StoreType st)
        => _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            Client(st).CreateTableAsync(new CreateTableRequest
            {
                TableName = "TestTable",
                KeySchema =
                [
                    new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                    new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }
                ],
                AttributeDefinitions =
                [
                    new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                    new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S },
                    new AttributeDefinition { AttributeName = "OTHER_PK", AttributeType = ScalarAttributeType.S }
                ],
                LocalSecondaryIndexes =
                [
                    new LocalSecondaryIndex
                    {
                        IndexName = "BadLSI",
                        KeySchema =
                        [
                            new KeySchemaElement { AttributeName = "OTHER_PK", KeyType = KeyType.HASH },
                            new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }
                        ],
                        Projection = new Projection { ProjectionType = ProjectionType.ALL }
                    }
                ]
            }, TestContext.Current.CancellationToken));

    // -- Validation: Missing attribute definition ------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task CreateTable_GsiKeyNotInAttributeDefinitions_Throws(StoreType st)
        => _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            Client(st).CreateTableAsync(new CreateTableRequest
            {
                TableName = "TestTable",
                KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
                AttributeDefinitions =
                [
                    new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }
                ],
                GlobalSecondaryIndexes =
                [
                    new GlobalSecondaryIndex
                    {
                        IndexName = "GSI1",
                        KeySchema = [new KeySchemaElement { AttributeName = "MISSING", KeyType = KeyType.HASH }],
                        Projection = new Projection { ProjectionType = ProjectionType.ALL }
                    }
                ]
            }, TestContext.Current.CancellationToken));

    // -- Validation: Unused attribute definitions ------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task CreateTable_UnusedAttributeDefinitions_Throws(StoreType st)
        => _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            Client(st).CreateTableAsync(new CreateTableRequest
            {
                TableName = "TestTable",
                KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
                AttributeDefinitions =
                [
                    new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                    new AttributeDefinition { AttributeName = "ExtraUnused", AttributeType = ScalarAttributeType.S }
                ]
            }, TestContext.Current.CancellationToken));

    // -- Validation: Duplicate index names -------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task CreateTable_DuplicateGsiNames_Throws(StoreType st)
        => _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            Client(st).CreateTableAsync(new CreateTableRequest
            {
                TableName = "TestTable",
                KeySchema = [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
                AttributeDefinitions =
                [
                    new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                    new AttributeDefinition { AttributeName = "GK1", AttributeType = ScalarAttributeType.S }
                ],
                GlobalSecondaryIndexes =
                [
                    new GlobalSecondaryIndex
                    {
                        IndexName = "DuplicateName",
                        KeySchema = [new KeySchemaElement { AttributeName = "GK1", KeyType = KeyType.HASH }],
                        Projection = new Projection { ProjectionType = ProjectionType.ALL }
                    },
                    new GlobalSecondaryIndex
                    {
                        IndexName = "DuplicateName",
                        KeySchema = [new KeySchemaElement { AttributeName = "GK1", KeyType = KeyType.HASH }],
                        Projection = new Projection { ProjectionType = ProjectionType.ALL }
                    }
                ]
            }, TestContext.Current.CancellationToken));

    // -- GSI Query -------------------------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_GsiIndex_ReturnsCorrectResults(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "gsi_pk1" },
                ["GSI_SK"] = new() { S = "gsi_sk_A" },
                ["data"] = new() { S = "first" }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk2" },
                ["SK"] = new() { S = "sk2" },
                ["GSI_PK"] = new() { S = "gsi_pk1" },
                ["GSI_SK"] = new() { S = "gsi_sk_B" },
                ["data"] = new() { S = "second" }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk3" },
                ["SK"] = new() { S = "sk3" },
                ["GSI_PK"] = new() { S = "other_gsi_pk" },
                ["GSI_SK"] = new() { S = "gsi_sk_C" },
                ["data"] = new() { S = "third" }
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            IndexName = "GSI1",
            KeyConditionExpression = "GSI_PK = :gpk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gpk"] = new() { S = "gsi_pk1" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Count);
        Assert.Equal("gsi_sk_A", response.Items[0]["GSI_SK"].S);
        Assert.Equal("gsi_sk_B", response.Items[1]["GSI_SK"].S);
    }

    // -- GSI Scan --------------------------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ScanAsync_GsiIndex_ReturnsAllIndexItems(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "gsi_pk1" },
                ["GSI_SK"] = new() { S = "gsi_sk_A" }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk2" },
                ["SK"] = new() { S = "sk2" },
                ["GSI_PK"] = new() { S = "gsi_pk2" },
                ["GSI_SK"] = new() { S = "gsi_sk_B" }
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = "TestTable",
            IndexName = "GSI1"
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Count);
    }

    // -- LSI Query -------------------------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_LsiIndex_ReturnsCorrectResults(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithLsiAsync(client);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user1" },
                ["SK"] = new() { S = "sk1" },
                ["LSI_SK"] = new() { S = "Z" },
                ["data"] = new() { S = "first" }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user1" },
                ["SK"] = new() { S = "sk2" },
                ["LSI_SK"] = new() { S = "A" },
                ["data"] = new() { S = "second" }
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            IndexName = "LSI1",
            KeyConditionExpression = "PK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "user1" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Count);
        // LSI sorts by LSI_SK, so "A" comes before "Z"
        Assert.Equal("A", response.Items[0]["LSI_SK"].S);
        Assert.Equal("Z", response.Items[1]["LSI_SK"].S);
    }

    // -- Sparse indexes --------------------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_SparseIndex_ExcludesItemsMissingKeys(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        // Item with GSI keys
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "gsi_pk1" },
                ["GSI_SK"] = new() { S = "gsi_sk1" },
                ["data"] = new() { S = "indexed" }
            }
        }, TestContext.Current.CancellationToken);

        // Item WITHOUT GSI keys (sparse)
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk2" },
                ["SK"] = new() { S = "sk2" },
                ["data"] = new() { S = "not_indexed" }
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = "TestTable",
            IndexName = "GSI1"
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Items);
        Assert.Equal("indexed", response.Items[0]["data"].S);
    }

    // -- Projection: KEYS_ONLY ------------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_KeysOnlyProjection_ReturnsOnlyKeys(StoreType st)
    {
        var client = Client(st);
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "TestTable",
            KeySchema =
            [
                new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "GSI_PK", AttributeType = ScalarAttributeType.S }
            ],
            GlobalSecondaryIndexes =
            [
                new GlobalSecondaryIndex
                {
                    IndexName = "KeysOnlyGSI",
                    KeySchema = [new KeySchemaElement { AttributeName = "GSI_PK", KeyType = KeyType.HASH }],
                    Projection = new Projection { ProjectionType = ProjectionType.KEYS_ONLY }
                }
            ]
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "gsi_pk1" },
                ["data"] = new() { S = "should_be_excluded" }
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            IndexName = "KeysOnlyGSI",
            KeyConditionExpression = "GSI_PK = :gpk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gpk"] = new() { S = "gsi_pk1" }
            }
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Items);
        var item = response.Items[0];
        // Should only have key attributes: PK, SK, GSI_PK
        Assert.True(item.ContainsKey("PK"));
        Assert.True(item.ContainsKey("SK"));
        Assert.True(item.ContainsKey("GSI_PK"));
        Assert.False(item.ContainsKey("data"));
    }

    // -- Projection: INCLUDE --------------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_IncludeProjection_ReturnsKeysAndIncludedAttributes(StoreType st)
    {
        var client = Client(st);
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "TestTable",
            KeySchema =
            [
                new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "GSI_PK", AttributeType = ScalarAttributeType.S }
            ],
            GlobalSecondaryIndexes =
            [
                new GlobalSecondaryIndex
                {
                    IndexName = "IncludeGSI",
                    KeySchema = [new KeySchemaElement { AttributeName = "GSI_PK", KeyType = KeyType.HASH }],
                    Projection = new Projection
                    {
                        ProjectionType = ProjectionType.INCLUDE,
                        NonKeyAttributes = ["name"]
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "gsi_pk1" },
                ["name"] = new() { S = "Alice" },
                ["secret"] = new() { S = "should_be_excluded" }
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            IndexName = "IncludeGSI",
            KeyConditionExpression = "GSI_PK = :gpk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gpk"] = new() { S = "gsi_pk1" }
            }
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Items);
        var item = response.Items[0];
        Assert.True(item.ContainsKey("PK"));
        Assert.True(item.ContainsKey("SK"));
        Assert.True(item.ContainsKey("GSI_PK"));
        Assert.True(item.ContainsKey("name"));
        Assert.False(item.ContainsKey("secret"));
    }

    // -- Index maintenance: update ---------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task UpdateItem_ChangesIndexKey_UpdatesIndex(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "old_gsi_pk" },
                ["GSI_SK"] = new() { S = "gsi_sk1" }
            }
        }, TestContext.Current.CancellationToken);

        // Update the GSI_PK
        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" }
            },
            UpdateExpression = "SET GSI_PK = :newPk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":newPk"] = new() { S = "new_gsi_pk" }
            }
        }, TestContext.Current.CancellationToken);

        // Old key should not return results
        var oldResponse = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            IndexName = "GSI1",
            KeyConditionExpression = "GSI_PK = :gpk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gpk"] = new() { S = "old_gsi_pk" }
            }
        }, TestContext.Current.CancellationToken);
        Assert.Equal(0, oldResponse.Count);

        // New key should return the item
        var newResponse = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            IndexName = "GSI1",
            KeyConditionExpression = "GSI_PK = :gpk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gpk"] = new() { S = "new_gsi_pk" }
            }
        }, TestContext.Current.CancellationToken);
        Assert.Equal(1, newResponse.Count);
    }

    // -- Index maintenance: delete ---------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DeleteItem_RemovesFromIndex(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "gsi_pk1" },
                ["GSI_SK"] = new() { S = "gsi_sk1" }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.DeleteItemAsync(new DeleteItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" }
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            IndexName = "GSI1",
            KeyConditionExpression = "GSI_PK = :gpk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gpk"] = new() { S = "gsi_pk1" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(0, response.Count);
    }

    // -- Batch write with indexes ----------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task BatchWriteItem_UpdatesIndexes(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        _ = await client.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>
            {
                ["TestTable"] =
                [
                    new WriteRequest
                    {
                        PutRequest = new PutRequest
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                ["PK"] = new() { S = "pk1" },
                                ["SK"] = new() { S = "sk1" },
                                ["GSI_PK"] = new() { S = "batch_gsi" },
                                ["GSI_SK"] = new() { S = "A" }
                            }
                        }
                    },
                    new WriteRequest
                    {
                        PutRequest = new PutRequest
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                ["PK"] = new() { S = "pk2" },
                                ["SK"] = new() { S = "sk2" },
                                ["GSI_PK"] = new() { S = "batch_gsi" },
                                ["GSI_SK"] = new() { S = "B" }
                            }
                        }
                    }
                ]
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            IndexName = "GSI1",
            KeyConditionExpression = "GSI_PK = :gpk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gpk"] = new() { S = "batch_gsi" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Count);
    }

    // -- GSI Query with FilterExpression ---------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_GsiWithFilter_FiltersResults(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "gsi_pk1" },
                ["GSI_SK"] = new() { S = "A" },
                ["active"] = new() { BOOL = true }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk2" },
                ["SK"] = new() { S = "sk2" },
                ["GSI_PK"] = new() { S = "gsi_pk1" },
                ["GSI_SK"] = new() { S = "B" },
                ["active"] = new() { BOOL = false }
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            IndexName = "GSI1",
            KeyConditionExpression = "GSI_PK = :gpk",
            FilterExpression = "active = :a",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gpk"] = new() { S = "gsi_pk1" },
                [":a"] = new() { BOOL = true }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.ScannedCount);
        Assert.Equal(1, response.Count);
        Assert.Equal("pk1", response.Items[0]["PK"].S);
    }

    // -- Pagination on index ---------------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_GsiPagination_WorksCorrectly(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        for (var i = 0; i < 5; i++)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = $"pk{i}" },
                    ["SK"] = new() { S = $"sk{i}" },
                    ["GSI_PK"] = new() { S = "same_gsi_pk" },
                    ["GSI_SK"] = new() { S = $"gsi_sk_{i:D2}" }
                }
            }, TestContext.Current.CancellationToken);
        }

        var allItems = new List<Dictionary<string, AttributeValue>>();
        Dictionary<string, AttributeValue>? lastKey = null;

        do
        {
            var response = await client.QueryAsync(new QueryRequest
            {
                TableName = "TestTable",
                IndexName = "GSI1",
                KeyConditionExpression = "GSI_PK = :gpk",
                Limit = 2,
                ExclusiveStartKey = lastKey,
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":gpk"] = new() { S = "same_gsi_pk" }
                }
            }, TestContext.Current.CancellationToken);

            allItems.AddRange(response.Items);
            lastKey = response.LastEvaluatedKey;
        }
        while (lastKey is not null);

        Assert.Equal(5, allItems.Count);
    }

    // -- ConsistentRead on GSI throws ------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_ConsistentReadOnGsi_Throws(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.QueryAsync(new QueryRequest
            {
                TableName = "TestTable",
                IndexName = "GSI1",
                KeyConditionExpression = "GSI_PK = :gpk",
                ConsistentRead = true,
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":gpk"] = new() { S = "x" }
                }
            }, TestContext.Current.CancellationToken));
    }

    // -- UpdateTable: Create GSI with backfill ---------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task UpdateTableAsync_CreateGsi_BackfillsExistingItems(StoreType st)
    {
        var client = Client(st);

        // Create table without GSI
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "TestTable",
            KeySchema =
            [
                new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S }
            ]
        }, TestContext.Current.CancellationToken);

        // Add items
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "gsi_pk1" },
                ["GSI_SK"] = new() { S = "gsi_sk1" },
                ["data"] = new() { S = "hello" }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk2" },
                ["SK"] = new() { S = "sk2" },
                ["data"] = new() { S = "no_gsi_keys" }
            }
        }, TestContext.Current.CancellationToken);

        // Create GSI via UpdateTable
        _ = await client.UpdateTableAsync(new UpdateTableRequest
        {
            TableName = "TestTable",
            AttributeDefinitions =
            [
                new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "GSI_PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "GSI_SK", AttributeType = ScalarAttributeType.S }
            ],
            GlobalSecondaryIndexUpdates =
            [
                new GlobalSecondaryIndexUpdate
                {
                    Create = new CreateGlobalSecondaryIndexAction
                    {
                        IndexName = "GSI1",
                        KeySchema =
                        [
                            new KeySchemaElement { AttributeName = "GSI_PK", KeyType = KeyType.HASH },
                            new KeySchemaElement { AttributeName = "GSI_SK", KeyType = KeyType.RANGE }
                        ],
                        Projection = new Projection { ProjectionType = ProjectionType.ALL }
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        // Verify backfill: item with GSI keys should be queryable
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            IndexName = "GSI1",
            KeyConditionExpression = "GSI_PK = :gpk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gpk"] = new() { S = "gsi_pk1" }
            }
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Items);
        Assert.Equal("hello", response.Items[0]["data"].S);

        // DescribeTable should show the GSI
        var desc = await client.DescribeTableAsync("TestTable", TestContext.Current.CancellationToken);
        Assert.NotNull(desc.Table.GlobalSecondaryIndexes);
        _ = Assert.Single(desc.Table.GlobalSecondaryIndexes);
        Assert.Equal("GSI1", desc.Table.GlobalSecondaryIndexes[0].IndexName);
    }

    // -- UpdateTable: Delete GSI -----------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task UpdateTableAsync_DeleteGsi_RemovesIndex(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        _ = await client.UpdateTableAsync(new UpdateTableRequest
        {
            TableName = "TestTable",
            GlobalSecondaryIndexUpdates =
            [
                new GlobalSecondaryIndexUpdate
                {
                    Delete = new DeleteGlobalSecondaryIndexAction
                    {
                        IndexName = "GSI1"
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        var desc = await client.DescribeTableAsync("TestTable", TestContext.Current.CancellationToken);
        Assert.True(desc.Table.GlobalSecondaryIndexes is null or { Count: 0 });
    }

    // -- DeleteTable drops index tables ----------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DeleteTable_WithIndexes_Succeeds(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "gsi1" },
                ["GSI_SK"] = new() { S = "gsi_sk1" }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.DeleteTableAsync("TestTable", TestContext.Current.CancellationToken);

        _ = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            client.DescribeTableAsync("TestTable", TestContext.Current.CancellationToken));
    }

    // -- GSI Query with SK condition -------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_GsiWithSkCondition_FiltersCorrectly(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        for (var i = 0; i < 5; i++)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = $"pk{i}" },
                    ["SK"] = new() { S = $"sk{i}" },
                    ["GSI_PK"] = new() { S = "shared_pk" },
                    ["GSI_SK"] = new() { S = ((char)('A' + i)).ToString() }
                }
            }, TestContext.Current.CancellationToken);
        }

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            IndexName = "GSI1",
            KeyConditionExpression = "GSI_PK = :gpk AND GSI_SK < :gsk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gpk"] = new() { S = "shared_pk" },
                [":gsk"] = new() { S = "C" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Count);
        Assert.Equal("A", response.Items[0]["GSI_SK"].S);
        Assert.Equal("B", response.Items[1]["GSI_SK"].S);
    }

    // -- Select.ALL_PROJECTED_ATTRIBUTES ---------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_SelectAllProjectedAttributes_ReturnsProjectedOnly(StoreType st)
    {
        var client = Client(st);
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "TestTable",
            KeySchema =
            [
                new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "GSI_PK", AttributeType = ScalarAttributeType.S }
            ],
            GlobalSecondaryIndexes =
            [
                new GlobalSecondaryIndex
                {
                    IndexName = "GSI1",
                    KeySchema = [new KeySchemaElement { AttributeName = "GSI_PK", KeyType = KeyType.HASH }],
                    Projection = new Projection
                    {
                        ProjectionType = ProjectionType.INCLUDE,
                        NonKeyAttributes = ["name"]
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "gsi_pk1" },
                ["name"] = new() { S = "Alice" },
                ["secret"] = new() { S = "hidden" }
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            IndexName = "GSI1",
            KeyConditionExpression = "GSI_PK = :gpk",
            Select = Select.ALL_PROJECTED_ATTRIBUTES,
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gpk"] = new() { S = "gsi_pk1" }
            }
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Items);
        var item = response.Items[0];
        Assert.True(item.ContainsKey("PK"));
        Assert.True(item.ContainsKey("SK"));
        Assert.True(item.ContainsKey("GSI_PK"));
        Assert.True(item.ContainsKey("name"));
        Assert.False(item.ContainsKey("secret"));
    }

    // -- ScanIndexForward on GSI -----------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_GsiDescending_ReturnsReverseOrder(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        for (var i = 0; i < 3; i++)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = $"pk{i}" },
                    ["SK"] = new() { S = $"sk{i}" },
                    ["GSI_PK"] = new() { S = "shared" },
                    ["GSI_SK"] = new() { S = ((char)('A' + i)).ToString() }
                }
            }, TestContext.Current.CancellationToken);
        }

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            IndexName = "GSI1",
            KeyConditionExpression = "GSI_PK = :gpk",
            ScanIndexForward = false,
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gpk"] = new() { S = "shared" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(3, response.Count);
        Assert.Equal("C", response.Items[0]["GSI_SK"].S);
        Assert.Equal("B", response.Items[1]["GSI_SK"].S);
        Assert.Equal("A", response.Items[2]["GSI_SK"].S);
    }

    // -- Non-existent index ----------------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_NonExistentIndex_Throws(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.QueryAsync(new QueryRequest
            {
                TableName = "TestTable",
                IndexName = "NonExistentIndex",
                KeyConditionExpression = "GSI_PK = :gpk",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":gpk"] = new() { S = "x" }
                }
            }, TestContext.Current.CancellationToken));
    }

    // -- UpdateTable: legacy throughput overload --------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task UpdateTableAsync_LegacyThroughput_ReturnsDescription(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        var response = await client.UpdateTableAsync(
            "TestTable",
            new ProvisionedThroughput { ReadCapacityUnits = 10, WriteCapacityUnits = 5 },
            TestContext.Current.CancellationToken);

        Assert.NotNull(response.TableDescription);
        Assert.Equal("TestTable", response.TableDescription.TableName);
    }

    // -- Scan pagination on index ----------------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ScanAsync_GsiPagination_WorksCorrectly(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        for (var i = 0; i < 5; i++)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = $"pk{i}" },
                    ["SK"] = new() { S = $"sk{i}" },
                    ["GSI_PK"] = new() { S = $"gsi_pk_{i}" },
                    ["GSI_SK"] = new() { S = $"gsi_sk_{i}" }
                }
            }, TestContext.Current.CancellationToken);
        }

        var allItems = new List<Dictionary<string, AttributeValue>>();
        Dictionary<string, AttributeValue>? lastKey = null;

        do
        {
            var response = await client.ScanAsync(new ScanRequest
            {
                TableName = "TestTable",
                IndexName = "GSI1",
                Limit = 2,
                ExclusiveStartKey = lastKey
            }, TestContext.Current.CancellationToken);

            allItems.AddRange(response.Items);
            lastKey = response.LastEvaluatedKey;
        }
        while (lastKey is not null);

        Assert.Equal(5, allItems.Count);
    }

    // -- GSI with ProjectionExpression -----------------------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task QueryAsync_GsiWithProjectionExpression_AppliesProjection(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "gsi_pk1" },
                ["GSI_SK"] = new() { S = "gsi_sk1" },
                ["name"] = new() { S = "Alice" },
                ["age"] = new() { N = "30" }
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            IndexName = "GSI1",
            KeyConditionExpression = "GSI_PK = :gpk",
            ProjectionExpression = "#n",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#n"] = "name"
            },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gpk"] = new() { S = "gsi_pk1" }
            }
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Items);
        _ = Assert.Single(response.Items[0]);
        Assert.Equal("Alice", response.Items[0]["name"].S);
    }

    // -- PutItem overwrites updates index correctly ----------------------

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task PutItem_Overwrite_UpdatesIndex(StoreType st)
    {
        var client = Client(st);
        await CreateTableWithGsiAsync(client);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "old_gsi" },
                ["GSI_SK"] = new() { S = "A" }
            }
        }, TestContext.Current.CancellationToken);

        // Overwrite same table key with different GSI key
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "new_gsi" },
                ["GSI_SK"] = new() { S = "B" }
            }
        }, TestContext.Current.CancellationToken);

        // Old GSI key should be empty
        var oldResp = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            IndexName = "GSI1",
            KeyConditionExpression = "GSI_PK = :gpk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gpk"] = new() { S = "old_gsi" }
            }
        }, TestContext.Current.CancellationToken);
        Assert.Equal(0, oldResp.Count);

        // New GSI key should have the item
        var newResp = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            IndexName = "GSI1",
            KeyConditionExpression = "GSI_PK = :gpk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gpk"] = new() { S = "new_gsi" }
            }
        }, TestContext.Current.CancellationToken);
        Assert.Equal(1, newResp.Count);
    }
}
