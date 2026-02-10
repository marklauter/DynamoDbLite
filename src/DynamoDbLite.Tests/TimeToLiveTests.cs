using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;
using System.Globalization;

namespace DynamoDbLite.Tests;

public abstract class TimeToLiveTestsBase
    : IAsyncLifetime
{
    protected DynamoDbClient client = null!;

    protected abstract DynamoDbClient CreateClient();

    public async ValueTask InitializeAsync()
    {
        client = CreateClient();
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
    }

    public virtual ValueTask DisposeAsync()
    {
        client.Dispose();
        return ValueTask.CompletedTask;
    }

    private static long FutureEpoch() => DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 86400;
    private static long PastEpoch() => DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 86400;

    private async Task EnableTtlAsync() =>
        _ = await client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
        {
            TableName = "TestTable",
            TimeToLiveSpecification = new TimeToLiveSpecification { Enabled = true, AttributeName = "ttl" }
        }, TestContext.Current.CancellationToken);

    private async Task PutItemWithTtlAsync(string pk, string sk, long ttlValue) =>
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = pk },
                ["SK"] = new() { S = sk },
                ["ttl"] = new() { N = ttlValue.ToString(CultureInfo.InvariantCulture) }
            }
        }, TestContext.Current.CancellationToken);

    private async Task PutItemWithoutTtlAsync(string pk, string sk) =>
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = pk },
                ["SK"] = new() { S = sk }
            }
        }, TestContext.Current.CancellationToken);

    private async Task<Dictionary<string, AttributeValue>?> GetItemAsync(string pk, string sk)
    {
        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = pk },
                ["SK"] = new() { S = sk }
            }
        }, TestContext.Current.CancellationToken);
        return response.IsItemSet ? response.Item : null;
    }

    // ── TTL Config API ──────────────────────────────────────────────

    [Fact]
    public async Task DescribeTimeToLive_Disabled_By_Default()
    {
        var response = await client.DescribeTimeToLiveAsync("TestTable", TestContext.Current.CancellationToken);

        Assert.Equal(TimeToLiveStatus.DISABLED, response.TimeToLiveDescription.TimeToLiveStatus);
        Assert.Null(response.TimeToLiveDescription.AttributeName);
    }

    [Fact]
    public async Task DescribeTimeToLive_Missing_Table_Throws()
    {
        var ex = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            client.DescribeTimeToLiveAsync("NoSuchTable", TestContext.Current.CancellationToken));
        Assert.Contains("NoSuchTable", ex.Message);
    }

    [Fact]
    public async Task Enable_Disable_Roundtrip()
    {
        var enableResponse = await client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
        {
            TableName = "TestTable",
            TimeToLiveSpecification = new TimeToLiveSpecification { Enabled = true, AttributeName = "ttl" }
        }, TestContext.Current.CancellationToken);

        Assert.True(enableResponse.TimeToLiveSpecification.Enabled);
        Assert.Equal("ttl", enableResponse.TimeToLiveSpecification.AttributeName);

        var describe = await client.DescribeTimeToLiveAsync("TestTable", TestContext.Current.CancellationToken);
        Assert.Equal(TimeToLiveStatus.ENABLED, describe.TimeToLiveDescription.TimeToLiveStatus);
        Assert.Equal("ttl", describe.TimeToLiveDescription.AttributeName);

        var disableResponse = await client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
        {
            TableName = "TestTable",
            TimeToLiveSpecification = new TimeToLiveSpecification { Enabled = false, AttributeName = "ttl" }
        }, TestContext.Current.CancellationToken);

        Assert.False(disableResponse.TimeToLiveSpecification.Enabled);

        var describe2 = await client.DescribeTimeToLiveAsync("TestTable", TestContext.Current.CancellationToken);
        Assert.Equal(TimeToLiveStatus.DISABLED, describe2.TimeToLiveDescription.TimeToLiveStatus);
    }

    [Fact]
    public async Task Enable_When_Already_Enabled_Throws()
    {
        await EnableTtlAsync();

        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
            {
                TableName = "TestTable",
                TimeToLiveSpecification = new TimeToLiveSpecification { Enabled = true, AttributeName = "ttl" }
            }, TestContext.Current.CancellationToken));
        Assert.Contains("already enabled", ex.Message);
    }

    [Fact]
    public async Task Disable_When_Already_Disabled_Throws()
    {
        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
            {
                TableName = "TestTable",
                TimeToLiveSpecification = new TimeToLiveSpecification { Enabled = false, AttributeName = "ttl" }
            }, TestContext.Current.CancellationToken));
        Assert.Contains("already disabled", ex.Message);
    }

    [Fact]
    public async Task Enable_On_Missing_Table_Throws()
    {
        var ex = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
            {
                TableName = "NoSuchTable",
                TimeToLiveSpecification = new TimeToLiveSpecification { Enabled = true, AttributeName = "ttl" }
            }, TestContext.Current.CancellationToken));
        Assert.Contains("NoSuchTable", ex.Message);
    }

    [Fact]
    public async Task DescribeTimeToLive_With_Request_Object()
    {
        await EnableTtlAsync();

        var response = await client.DescribeTimeToLiveAsync(new DescribeTimeToLiveRequest
        {
            TableName = "TestTable"
        }, TestContext.Current.CancellationToken);

        Assert.Equal(TimeToLiveStatus.ENABLED, response.TimeToLiveDescription.TimeToLiveStatus);
        Assert.Equal("ttl", response.TimeToLiveDescription.AttributeName);
    }

    // ── Read Filtering ──────────────────────────────────────────────

    [Fact]
    public async Task GetItem_Expired_Item_Returns_Null()
    {
        await EnableTtlAsync();
        await PutItemWithTtlAsync("pk1", "sk1", PastEpoch());

        var result = await GetItemAsync("pk1", "sk1");
        Assert.Null(result);
    }

    [Fact]
    public async Task GetItem_NonExpired_Item_Returns_Item()
    {
        await EnableTtlAsync();
        await PutItemWithTtlAsync("pk1", "sk1", FutureEpoch());

        var result = await GetItemAsync("pk1", "sk1");
        Assert.NotNull(result);
        Assert.Equal("pk1", result["PK"].S);
    }

    [Fact]
    public async Task GetItem_Missing_Ttl_Attribute_Returns_Item()
    {
        await EnableTtlAsync();
        await PutItemWithoutTtlAsync("pk1", "sk1");

        var result = await GetItemAsync("pk1", "sk1");
        Assert.NotNull(result);
    }

    [Fact]
    public async Task GetItem_NonNumeric_Ttl_Attribute_Returns_Item()
    {
        await EnableTtlAsync();
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["ttl"] = new() { S = "not-a-number" }
            }
        }, TestContext.Current.CancellationToken);

        var result = await GetItemAsync("pk1", "sk1");
        Assert.NotNull(result);
    }

    [Fact]
    public async Task Query_Excludes_Expired_Items()
    {
        await EnableTtlAsync();
        await PutItemWithTtlAsync("pk1", "sk1", FutureEpoch());
        await PutItemWithTtlAsync("pk1", "sk2", PastEpoch());
        await PutItemWithTtlAsync("pk1", "sk3", FutureEpoch());

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "TestTable",
            KeyConditionExpression = "PK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "pk1" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Items.Count);
        Assert.All(response.Items, item => Assert.NotEqual("sk2", item["SK"].S));
    }

    [Fact]
    public async Task Scan_Excludes_Expired_Items()
    {
        await EnableTtlAsync();
        await PutItemWithTtlAsync("pk1", "sk1", FutureEpoch());
        await PutItemWithTtlAsync("pk2", "sk1", PastEpoch());

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = "TestTable"
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Items);
        Assert.Equal("pk1", response.Items[0]["PK"].S);
    }

    [Fact]
    public async Task BatchGetItem_Excludes_Expired_Items()
    {
        await EnableTtlAsync();
        await PutItemWithTtlAsync("pk1", "sk1", PastEpoch());
        await PutItemWithTtlAsync("pk2", "sk1", FutureEpoch());

        var response = await client.BatchGetItemAsync(new BatchGetItemRequest
        {
            RequestItems = new Dictionary<string, KeysAndAttributes>
            {
                ["TestTable"] = new()
                {
                    Keys =
                    [
                        new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "pk1" },
                            ["SK"] = new() { S = "sk1" }
                        },
                        new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "pk2" },
                            ["SK"] = new() { S = "sk1" }
                        }
                    ]
                }
            }
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Responses["TestTable"]);
        Assert.Equal("pk2", response.Responses["TestTable"][0]["PK"].S);
    }

    [Fact]
    public async Task TransactGetItems_Excludes_Expired_Items()
    {
        await EnableTtlAsync();
        await PutItemWithTtlAsync("pk1", "sk1", PastEpoch());
        await PutItemWithTtlAsync("pk2", "sk1", FutureEpoch());

        var response = await client.TransactGetItemsAsync(new TransactGetItemsRequest
        {
            TransactItems =
            [
                new TransactGetItem
                {
                    Get = new Get
                    {
                        TableName = "TestTable",
                        Key = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "pk1" },
                            ["SK"] = new() { S = "sk1" }
                        }
                    }
                },
                new TransactGetItem
                {
                    Get = new Get
                    {
                        TableName = "TestTable",
                        Key = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "pk2" },
                            ["SK"] = new() { S = "sk1" }
                        }
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        Assert.Null(response.Responses[0].Item);
        Assert.NotNull(response.Responses[1].Item);
    }

    // ── Write Path ──────────────────────────────────────────────────

    [Fact]
    public async Task PutItem_With_Ttl_Stores_TtlEpoch()
    {
        await EnableTtlAsync();
        var futureEpoch = FutureEpoch();
        await PutItemWithTtlAsync("pk1", "sk1", futureEpoch);

        var result = await GetItemAsync("pk1", "sk1");
        Assert.NotNull(result);
        Assert.Equal(futureEpoch.ToString(CultureInfo.InvariantCulture), result["ttl"].N);
    }

    [Fact]
    public async Task UpdateItem_With_Ttl_Stores_TtlEpoch()
    {
        await EnableTtlAsync();
        await PutItemWithoutTtlAsync("pk1", "sk1");

        var futureEpoch = FutureEpoch();
        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" }
            },
            UpdateExpression = "SET #t = :t",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#t"] = "ttl" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":t"] = new() { N = futureEpoch.ToString(CultureInfo.InvariantCulture) }
            }
        }, TestContext.Current.CancellationToken);

        var result = await GetItemAsync("pk1", "sk1");
        Assert.NotNull(result);
        Assert.Equal(futureEpoch.ToString(CultureInfo.InvariantCulture), result["ttl"].N);
    }

    [Fact]
    public async Task BatchWrite_With_Ttl_Stores_TtlEpoch()
    {
        await EnableTtlAsync();
        var futureEpoch = FutureEpoch();

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
                                ["ttl"] = new() { N = futureEpoch.ToString(CultureInfo.InvariantCulture) }
                            }
                        }
                    }
                ]
            }
        }, TestContext.Current.CancellationToken);

        var result = await GetItemAsync("pk1", "sk1");
        Assert.NotNull(result);
        Assert.Equal(futureEpoch.ToString(CultureInfo.InvariantCulture), result["ttl"].N);
    }

    [Fact]
    public async Task TransactWrite_With_Ttl_Stores_TtlEpoch()
    {
        await EnableTtlAsync();
        var futureEpoch = FutureEpoch();

        _ = await client.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            TransactItems =
            [
                new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = "TestTable",
                        Item = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "pk1" },
                            ["SK"] = new() { S = "sk1" },
                            ["ttl"] = new() { N = futureEpoch.ToString(CultureInfo.InvariantCulture) }
                        }
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        var result = await GetItemAsync("pk1", "sk1");
        Assert.NotNull(result);
    }

    // ── Config Changes ──────────────────────────────────────────────

    [Fact]
    public async Task Disable_Ttl_Makes_Expired_Items_Visible_Again()
    {
        await EnableTtlAsync();
        await PutItemWithTtlAsync("pk1", "sk1", PastEpoch());

        // Don't read the item while TTL is enabled — reading triggers background cleanup
        // that could physically delete the expired row before we disable TTL.

        // Disable TTL (clears ttl_epoch to NULL for all items)
        _ = await client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
        {
            TableName = "TestTable",
            TimeToLiveSpecification = new TimeToLiveSpecification { Enabled = false, AttributeName = "ttl" }
        }, TestContext.Current.CancellationToken);

        // Item should be visible again since ttl_epoch is now NULL
        var result = await GetItemAsync("pk1", "sk1");
        Assert.NotNull(result);
    }

    [Fact]
    public async Task Enable_Ttl_Backfills_Existing_Items()
    {
        // Put items before enabling TTL
        await PutItemWithTtlAsync("pk1", "sk1", PastEpoch());
        await PutItemWithTtlAsync("pk2", "sk1", FutureEpoch());

        // Enable TTL - should backfill ttl_epoch
        await EnableTtlAsync();

        // Expired item should now be invisible
        var result = await GetItemAsync("pk1", "sk1");
        Assert.Null(result);

        // Non-expired item should still be visible
        var result2 = await GetItemAsync("pk2", "sk1");
        Assert.NotNull(result2);
    }

    // ── Condition Expressions ───────────────────────────────────────

    [Fact]
    public async Task Condition_On_Expired_Item_Treats_As_NonExistent()
    {
        await EnableTtlAsync();
        await PutItemWithTtlAsync("pk1", "sk1", PastEpoch());

        // attribute_not_exists should succeed because the expired item is treated as non-existent
        var response = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["ttl"] = new() { N = FutureEpoch().ToString(CultureInfo.InvariantCulture) },
                ["data"] = new() { S = "new" }
            },
            ConditionExpression = "attribute_not_exists(PK)"
        }, TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.HttpStatusCode);

        var result = await GetItemAsync("pk1", "sk1");
        Assert.NotNull(result);
        Assert.Equal("new", result["data"].S);
    }

    [Fact]
    public async Task Update_On_Expired_Item_Behaves_Like_New_Item()
    {
        await EnableTtlAsync();
        await PutItemWithTtlAsync("pk1", "sk1", PastEpoch());

        var futureEpoch = FutureEpoch();
        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" }
            },
            UpdateExpression = "SET #d = :d, #t = :t",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#d"] = "data",
                ["#t"] = "ttl"
            },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":d"] = new() { S = "updated" },
                [":t"] = new() { N = futureEpoch.ToString(CultureInfo.InvariantCulture) }
            },
            ReturnValues = ReturnValue.ALL_OLD
        }, TestContext.Current.CancellationToken);

        var result = await GetItemAsync("pk1", "sk1");
        Assert.NotNull(result);
        Assert.Equal("updated", result["data"].S);
    }

    // ── Index Integration ───────────────────────────────────────────

    [Fact]
    public async Task Query_On_Gsi_Filters_Expired_Items()
    {
        // Create table with GSI
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "GsiTtlTable",
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
                    Projection = new Projection { ProjectionType = ProjectionType.ALL }
                }
            ]
        }, TestContext.Current.CancellationToken);

        _ = await client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
        {
            TableName = "GsiTtlTable",
            TimeToLiveSpecification = new TimeToLiveSpecification { Enabled = true, AttributeName = "ttl" }
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "GsiTtlTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "gsi1" },
                ["ttl"] = new() { N = PastEpoch().ToString(CultureInfo.InvariantCulture) }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "GsiTtlTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk2" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "gsi1" },
                ["ttl"] = new() { N = FutureEpoch().ToString(CultureInfo.InvariantCulture) }
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "GsiTtlTable",
            IndexName = "GSI1",
            KeyConditionExpression = "GSI_PK = :gsi",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gsi"] = new() { S = "gsi1" }
            }
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Items);
        Assert.Equal("pk2", response.Items[0]["PK"].S);
    }

    [Fact]
    public async Task Scan_On_Gsi_Filters_Expired_Items()
    {
        // Create table with GSI
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "GsiScanTtlTable",
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
                    Projection = new Projection { ProjectionType = ProjectionType.ALL }
                }
            ]
        }, TestContext.Current.CancellationToken);

        _ = await client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
        {
            TableName = "GsiScanTtlTable",
            TimeToLiveSpecification = new TimeToLiveSpecification { Enabled = true, AttributeName = "ttl" }
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "GsiScanTtlTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "gsi1" },
                ["ttl"] = new() { N = PastEpoch().ToString(CultureInfo.InvariantCulture) }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "GsiScanTtlTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk2" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "gsi1" },
                ["ttl"] = new() { N = FutureEpoch().ToString(CultureInfo.InvariantCulture) }
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.ScanAsync(new ScanRequest
        {
            TableName = "GsiScanTtlTable",
            IndexName = "GSI1"
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Items);
        Assert.Equal("pk2", response.Items[0]["PK"].S);
    }

    [Fact]
    public async Task Gsi_Created_After_Ttl_Enabled_Backfills_TtlEpoch()
    {
        // Create table, enable TTL, put items — then add a GSI
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "GsiBackfillTtlTable",
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

        _ = await client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
        {
            TableName = "GsiBackfillTtlTable",
            TimeToLiveSpecification = new TimeToLiveSpecification { Enabled = true, AttributeName = "ttl" }
        }, TestContext.Current.CancellationToken);

        // Put one expired and one non-expired item before the GSI exists
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "GsiBackfillTtlTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "gsi1" },
                ["ttl"] = new() { N = PastEpoch().ToString(CultureInfo.InvariantCulture) }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "GsiBackfillTtlTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk2" },
                ["SK"] = new() { S = "sk1" },
                ["GSI_PK"] = new() { S = "gsi1" },
                ["ttl"] = new() { N = FutureEpoch().ToString(CultureInfo.InvariantCulture) }
            }
        }, TestContext.Current.CancellationToken);

        // Now create the GSI — backfill should propagate ttlEpoch
        _ = await client.UpdateTableAsync(new UpdateTableRequest
        {
            TableName = "GsiBackfillTtlTable",
            AttributeDefinitions =
            [
                new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "GSI_PK", AttributeType = ScalarAttributeType.S }
            ],
            GlobalSecondaryIndexUpdates =
            [
                new GlobalSecondaryIndexUpdate
                {
                    Create = new CreateGlobalSecondaryIndexAction
                    {
                        IndexName = "GSI1",
                        KeySchema = [new KeySchemaElement { AttributeName = "GSI_PK", KeyType = KeyType.HASH }],
                        Projection = new Projection { ProjectionType = ProjectionType.ALL }
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        // Query the GSI — expired item should be filtered out
        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "GsiBackfillTtlTable",
            IndexName = "GSI1",
            KeyConditionExpression = "GSI_PK = :gsi",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gsi"] = new() { S = "gsi1" }
            }
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Items);
        Assert.Equal("pk2", response.Items[0]["PK"].S);
    }

    [Fact]
    public async Task Query_On_Lsi_Filters_Expired_Items()
    {
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "LsiTtlTable",
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

        _ = await client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
        {
            TableName = "LsiTtlTable",
            TimeToLiveSpecification = new TimeToLiveSpecification { Enabled = true, AttributeName = "ttl" }
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "LsiTtlTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["LSI_SK"] = new() { S = "lsi_a" },
                ["ttl"] = new() { N = PastEpoch().ToString(CultureInfo.InvariantCulture) }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "LsiTtlTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk2" },
                ["LSI_SK"] = new() { S = "lsi_b" },
                ["ttl"] = new() { N = FutureEpoch().ToString(CultureInfo.InvariantCulture) }
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = "LsiTtlTable",
            IndexName = "LSI1",
            KeyConditionExpression = "PK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "pk1" }
            }
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Items);
        Assert.Equal("lsi_b", response.Items[0]["LSI_SK"].S);
    }

    // ── Cleanup ─────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteTable_Cleans_Up_TtlConfig()
    {
        await EnableTtlAsync();

        _ = await client.DeleteTableAsync(new DeleteTableRequest { TableName = "TestTable" },
            TestContext.Current.CancellationToken);

        // Re-create table
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

        // TTL should be disabled for the re-created table
        var response = await client.DescribeTimeToLiveAsync("TestTable", TestContext.Current.CancellationToken);
        Assert.Equal(TimeToLiveStatus.DISABLED, response.TimeToLiveDescription.TimeToLiveStatus);
    }

    [Fact]
    public async Task Background_Cleanup_Removes_Expired_Rows()
    {
        await EnableTtlAsync();
        await PutItemWithTtlAsync("pk1", "sk1", PastEpoch());
        await PutItemWithTtlAsync("pk2", "sk1", FutureEpoch());

        // Scan to see only non-expired items
        var scanBefore = await client.ScanAsync(new ScanRequest { TableName = "TestTable" },
            TestContext.Current.CancellationToken);
        _ = Assert.Single(scanBefore.Items);

        // The scan triggers fire-and-forget background cleanup. The delay is a reviewed
        // tradeoff: exposing the Task for await would leak internals into the public API.
        // Acceptable for in-memory SQLite where the cleanup completes in microseconds.
        await Task.Delay(200, TestContext.Current.CancellationToken);

        // After cleanup, the table stats should reflect only the non-expired item
        var description = await client.DescribeTableAsync("TestTable", TestContext.Current.CancellationToken);
        Assert.Equal(1, description.Table.ItemCount);
    }
}

public sealed class InMemoryTimeToLiveTests : TimeToLiveTestsBase
{
    protected override DynamoDbClient CreateClient() =>
        new(new DynamoDbLiteOptions($"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));
}

public sealed class FileBasedTimeToLiveTests : TimeToLiveTestsBase
{
    private string? dbPath;

    protected override DynamoDbClient CreateClient()
    {
        var (c, path) = FileBasedTestHelper.CreateFileBasedClient();
        dbPath = path;
        return c;
    }

    public override ValueTask DisposeAsync()
    {
        var result = base.DisposeAsync();
        FileBasedTestHelper.Cleanup(dbPath);
        return result;
    }
}
