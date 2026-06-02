using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

public sealed class BatchOperationsTests
    : DynamoDbClientFixture
{
    protected override async ValueTask SetupAsync(CancellationToken ct)
    {
        await CreateTestTableAsync(Client(StoreType.DdbLite), ct);
        await CreateTestTableAsync(Client(StoreType.DdbLiteFile), ct);
    }

    private async Task PutTestItemAsync(DynamoDbClient client, string pk, string sk, string name)
        => _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = TestTableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = pk },
                ["SK"] = new() { S = sk },
                ["name"] = new() { S = name }
            }
        }, TestContext.Current.CancellationToken);

    private async Task CreateSecondTableAsync(DynamoDbClient client)
        => _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = SecondTableName,
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

    // ── BatchGetItemAsync ──────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchGetItemAsync_MultipleItems_ReturnsAll(StoreType st)
    {
        var client = Client(st);
        await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");
        await PutTestItemAsync(client, "USER#2", "PROFILE", "Bob");
        await PutTestItemAsync(client, "USER#3", "PROFILE", "Charlie");

        var response = await client.BatchGetItemAsync(new BatchGetItemRequest
        {
            RequestItems = new Dictionary<string, KeysAndAttributes>
            {
                [TestTableName] = new()
                {
                    Keys =
                    [
                        new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#1" },
                            ["SK"] = new() { S = "PROFILE" }
                        },
                        new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#2" },
                            ["SK"] = new() { S = "PROFILE" }
                        },
                        new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#3" },
                            ["SK"] = new() { S = "PROFILE" }
                        }
                    ]
                }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.HttpStatusCode);
        Assert.Equal(3, response.Responses[TestTableName].Count);
        Assert.Empty(response.UnprocessedKeys);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchGetItemAsync_SimpleOverload_Succeeds(StoreType st)
    {
        var client = Client(st);
        await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.BatchGetItemAsync(
            new Dictionary<string, KeysAndAttributes>
            {
                [TestTableName] = new()
                {
                    Keys =
                    [
                        new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#1" },
                            ["SK"] = new() { S = "PROFILE" }
                        }
                    ]
                }
            },
            ReturnConsumedCapacity.NONE,
            TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.HttpStatusCode);
        _ = Assert.Single(response.Responses[TestTableName]);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchGetItemAsync_DictionaryOverload_Succeeds(StoreType st)
    {
        var client = Client(st);
        await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.BatchGetItemAsync(
            new Dictionary<string, KeysAndAttributes>
            {
                [TestTableName] = new()
                {
                    Keys =
                    [
                        new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#1" },
                            ["SK"] = new() { S = "PROFILE" }
                        }
                    ]
                }
            },
            TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.HttpStatusCode);
        _ = Assert.Single(response.Responses[TestTableName]);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchGetItemAsync_WithProjectionExpression_ReturnsProjected(StoreType st)
    {
        var client = Client(st);
        await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.BatchGetItemAsync(new BatchGetItemRequest
        {
            RequestItems = new Dictionary<string, KeysAndAttributes>
            {
                [TestTableName] = new()
                {
                    Keys =
                    [
                        new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#1" },
                            ["SK"] = new() { S = "PROFILE" }
                        }
                    ],
                    ProjectionExpression = "PK, #n",
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        ["#n"] = "name"
                    }
                }
            }
        }, TestContext.Current.CancellationToken);

        var item = Assert.Single(response.Responses[TestTableName]);
        Assert.Equal("USER#1", item["PK"].S);
        Assert.Equal("Alice", item["name"].S);
        Assert.False(item.ContainsKey("SK"));
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchGetItemAsync_NonExistentKey_OmittedFromResponse(StoreType st)
    {
        var client = Client(st);
        await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.BatchGetItemAsync(new BatchGetItemRequest
        {
            RequestItems = new Dictionary<string, KeysAndAttributes>
            {
                [TestTableName] = new()
                {
                    Keys =
                    [
                        new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#1" },
                            ["SK"] = new() { S = "PROFILE" }
                        },
                        new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#999" },
                            ["SK"] = new() { S = "PROFILE" }
                        }
                    ]
                }
            }
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Responses[TestTableName]);
        Assert.Equal("Alice", response.Responses[TestTableName][0]["name"].S);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchGetItemAsync_MultipleTables_ReturnsFromBoth(StoreType st)
    {
        var client = Client(st);
        await CreateSecondTableAsync(client);
        await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = SecondTableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "ORDER#1" },
                ["SK"] = new() { S = "DETAIL" },
                ["total"] = new() { N = "100" }
            }
        }, TestContext.Current.CancellationToken);

        var response = await client.BatchGetItemAsync(new BatchGetItemRequest
        {
            RequestItems = new Dictionary<string, KeysAndAttributes>
            {
                [TestTableName] = new()
                {
                    Keys =
                    [
                        new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#1" },
                            ["SK"] = new() { S = "PROFILE" }
                        }
                    ]
                },
                [SecondTableName] = new()
                {
                    Keys =
                    [
                        new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "ORDER#1" },
                            ["SK"] = new() { S = "DETAIL" }
                        }
                    ]
                }
            }
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Responses[TestTableName]);
        _ = Assert.Single(response.Responses[SecondTableName]);
        Assert.Equal("Alice", response.Responses[TestTableName][0]["name"].S);
        Assert.Equal("100", response.Responses[SecondTableName][0]["total"].N);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchGetItemAsync_ExceedsLimit_ThrowsException(StoreType st)
    {
        var client = Client(st);
        var keys = Enumerable.Range(1, 101).Select(i => new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = $"USER#{i}" },
            ["SK"] = new() { S = "PROFILE" }
        }).ToList();

        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.BatchGetItemAsync(new BatchGetItemRequest
            {
                RequestItems = new Dictionary<string, KeysAndAttributes>
                {
                    [TestTableName] = new() { Keys = keys }
                }
            }, TestContext.Current.CancellationToken));

        Assert.Contains("Too many items", ex.Message);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchGetItemAsync_NonExistentTable_ThrowsResourceNotFoundException(StoreType st)
        => _ = await Assert.ThrowsAsync<ResourceNotFoundException>(()
            => Client(st).BatchGetItemAsync(new BatchGetItemRequest
            {
                RequestItems = new Dictionary<string, KeysAndAttributes>
                {
                    ["NonExistent"] = new()
                    {
                        Keys =
                        [
                            new Dictionary<string, AttributeValue>
                            {
                                ["PK"] = new() { S = "X" },
                                ["SK"] = new() { S = "Y" }
                            }
                        ]
                    }
                }
            }, TestContext.Current.CancellationToken));

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchGetItemAsync_EmptyRequestItems_ThrowsException(StoreType st)
    {
        var client = Client(st);
        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.BatchGetItemAsync(new BatchGetItemRequest
            {
                RequestItems = []
            }, TestContext.Current.CancellationToken));

        Assert.Contains("requestItems", ex.Message);
    }

    [Fact]
    public async Task BatchGetItemAsync_AfterDispose_ThrowsObjectDisposedException()
    {
#pragma warning disable IDISP016, IDISP017
        var disposableClient = new DynamoDbClient(new DynamoDbLiteOptions(
            $"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));
        disposableClient.Dispose();

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            disposableClient.BatchGetItemAsync(new BatchGetItemRequest
            {
                RequestItems = new Dictionary<string, KeysAndAttributes>
                {
                    [TestTableName] = new()
                    {
                        Keys =
                        [
                            new Dictionary<string, AttributeValue>
                            {
                                ["PK"] = new() { S = "X" },
                                ["SK"] = new() { S = "Y" }
                            }
                        ]
                    }
                }
            }, TestContext.Current.CancellationToken));
#pragma warning restore IDISP016, IDISP017
    }

    // ── BatchWriteItemAsync ────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchWriteItemAsync_MultiplePuts_AllSucceed(StoreType st)
    {
        var client = Client(st);
        var response = await client.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>
            {
                [TestTableName] =
                [
                    new WriteRequest { PutRequest = new PutRequest { Item = new Dictionary<string, AttributeValue>
                    {
                        ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" }, ["name"] = new() { S = "Alice" }
                    }}},
                    new WriteRequest { PutRequest = new PutRequest { Item = new Dictionary<string, AttributeValue>
                    {
                        ["PK"] = new() { S = "USER#2" }, ["SK"] = new() { S = "PROFILE" }, ["name"] = new() { S = "Bob" }
                    }}},
                    new WriteRequest { PutRequest = new PutRequest { Item = new Dictionary<string, AttributeValue>
                    {
                        ["PK"] = new() { S = "USER#3" }, ["SK"] = new() { S = "PROFILE" }, ["name"] = new() { S = "Charlie" }
                    }}}
                ]
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.HttpStatusCode);
        Assert.Empty(response.UnprocessedItems);

        var get1 = await client.GetItemAsync(TestTableName,
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);
        var get2 = await client.GetItemAsync(TestTableName,
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#2" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);
        var get3 = await client.GetItemAsync(TestTableName,
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#3" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);

        Assert.Equal("Alice", get1.Item["name"].S);
        Assert.Equal("Bob", get2.Item["name"].S);
        Assert.Equal("Charlie", get3.Item["name"].S);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchWriteItemAsync_SimpleOverload_Succeeds(StoreType st)
    {
        var client = Client(st);
        var response = await client.BatchWriteItemAsync(
            new Dictionary<string, List<WriteRequest>>
            {
                [TestTableName] =
                [
                    new WriteRequest { PutRequest = new PutRequest { Item = new Dictionary<string, AttributeValue>
                    {
                        ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" }, ["name"] = new() { S = "Alice" }
                    }}}
                ]
            },
            TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.HttpStatusCode);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchWriteItemAsync_MultipleDeletes_AllSucceed(StoreType st)
    {
        var client = Client(st);
        await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");
        await PutTestItemAsync(client, "USER#2", "PROFILE", "Bob");

        _ = await client.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>
            {
                [TestTableName] =
                [
                    new WriteRequest { DeleteRequest = new DeleteRequest { Key = new Dictionary<string, AttributeValue>
                    {
                        ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" }
                    }}},
                    new WriteRequest { DeleteRequest = new DeleteRequest { Key = new Dictionary<string, AttributeValue>
                    {
                        ["PK"] = new() { S = "USER#2" }, ["SK"] = new() { S = "PROFILE" }
                    }}}
                ]
            }
        }, TestContext.Current.CancellationToken);

        var get1 = await client.GetItemAsync(TestTableName,
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);
        var get2 = await client.GetItemAsync(TestTableName,
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#2" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);

        Assert.False(get1.IsItemSet);
        Assert.False(get2.IsItemSet);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchWriteItemAsync_MixedPutAndDelete_Succeeds(StoreType st)
    {
        var client = Client(st);
        await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        _ = await client.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>
            {
                [TestTableName] =
                [
                    new WriteRequest { DeleteRequest = new DeleteRequest { Key = new Dictionary<string, AttributeValue>
                    {
                        ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" }
                    }}},
                    new WriteRequest { PutRequest = new PutRequest { Item = new Dictionary<string, AttributeValue>
                    {
                        ["PK"] = new() { S = "USER#2" }, ["SK"] = new() { S = "PROFILE" }, ["name"] = new() { S = "Bob" }
                    }}}
                ]
            }
        }, TestContext.Current.CancellationToken);

        var get1 = await client.GetItemAsync(TestTableName,
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);
        var get2 = await client.GetItemAsync(TestTableName,
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#2" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);

        Assert.False(get1.IsItemSet);
        Assert.Equal("Bob", get2.Item["name"].S);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchWriteItemAsync_PutOverExistingKey_ReplacesItem(StoreType st)
    {
        var client = Client(st);

        // Seed an item with two attributes.
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = TestTableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" },
                ["name"] = new() { S = "Alice" },
                ["email"] = new() { S = "alice@example.com" }
            }
        }, TestContext.Current.CancellationToken);

        // Batch-put the same key with a different shape (no email).
        _ = await client.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>
            {
                [TestTableName] =
                [
                    new WriteRequest { PutRequest = new PutRequest { Item = new Dictionary<string, AttributeValue>
                    {
                        ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" }, ["name"] = new() { S = "Alice2" }
                    }}}
                ]
            }
        }, TestContext.Current.CancellationToken);

        var get = await client.GetItemAsync(TestTableName,
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);

        Assert.Equal("Alice2", get.Item["name"].S);     // value overwritten
        Assert.False(get.Item.ContainsKey("email"));     // full replacement, not a merge
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchWriteItemAsync_PutOverExistingKey_UpdatesTtl(StoreType st)
    {
        var client = Client(st);
        _ = await client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
        {
            TableName = TestTableName,
            TimeToLiveSpecification = new TimeToLiveSpecification { Enabled = true, AttributeName = "ttl" }
        }, TestContext.Current.CancellationToken);

        // Seed with a far-future TTL so the item is live.
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = TestTableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" },
                ["ttl"] = new() { N = "99999999999" }
            }
        }, TestContext.Current.CancellationToken);

        // Batch-put the same key with an already-expired TTL.
        _ = await client.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>
            {
                [TestTableName] =
                [
                    new WriteRequest { PutRequest = new PutRequest { Item = new Dictionary<string, AttributeValue>
                    {
                        ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" }, ["ttl"] = new() { N = "1" }
                    }}}
                ]
            }
        }, TestContext.Current.CancellationToken);

        var get = await client.GetItemAsync(TestTableName,
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);

        // The upsert overwrote ttl_epoch with the past value, so the item is now filtered out.
        Assert.False(get.IsItemSet);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchWriteItemAsync_MultipleTables_Succeeds(StoreType st)
    {
        var client = Client(st);
        await CreateSecondTableAsync(client);

        _ = await client.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>
            {
                [TestTableName] =
                [
                    new WriteRequest { PutRequest = new PutRequest { Item = new Dictionary<string, AttributeValue>
                    {
                        ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" }, ["name"] = new() { S = "Alice" }
                    }}}
                ],
                [SecondTableName] =
                [
                    new WriteRequest { PutRequest = new PutRequest { Item = new Dictionary<string, AttributeValue>
                    {
                        ["PK"] = new() { S = "ORDER#1" }, ["SK"] = new() { S = "DETAIL" }, ["total"] = new() { N = "50" }
                    }}}
                ]
            }
        }, TestContext.Current.CancellationToken);

        var get1 = await client.GetItemAsync(TestTableName,
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);
        var get2 = await client.GetItemAsync(SecondTableName,
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "ORDER#1" }, ["SK"] = new() { S = "DETAIL" } },
            TestContext.Current.CancellationToken);

        Assert.Equal("Alice", get1.Item["name"].S);
        Assert.Equal("50", get2.Item["total"].N);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchWriteItemAsync_ExceedsLimit_ThrowsException(StoreType st)
    {
        var client = Client(st);
        var writes = Enumerable.Range(1, 26).Select(i =>
            new WriteRequest
            {
                PutRequest = new PutRequest
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        ["PK"] = new() { S = $"USER#{i}" },
                        ["SK"] = new() { S = "PROFILE" },
                        ["name"] = new() { S = $"User{i}" }
                    }
                }
            }).ToList();

        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.BatchWriteItemAsync(new BatchWriteItemRequest
            {
                RequestItems = new Dictionary<string, List<WriteRequest>>
                {
                    [TestTableName] = writes
                }
            }, TestContext.Current.CancellationToken));

        Assert.Contains("Too many items", ex.Message);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchWriteItemAsync_DuplicateKeys_ThrowsException(StoreType st)
    {
        var client = Client(st);
        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.BatchWriteItemAsync(new BatchWriteItemRequest
            {
                RequestItems = new Dictionary<string, List<WriteRequest>>
                {
                    [TestTableName] =
                    [
                        new WriteRequest { PutRequest = new PutRequest { Item = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" }, ["name"] = new() { S = "Alice" }
                        }}},
                        new WriteRequest { PutRequest = new PutRequest { Item = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" }, ["name"] = new() { S = "Alice2" }
                        }}}
                    ]
                }
            }, TestContext.Current.CancellationToken));

        Assert.Contains("duplicates", ex.Message);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchWriteItemAsync_NonExistentTable_ThrowsResourceNotFoundException(StoreType st)
        => _ = await Assert.ThrowsAsync<ResourceNotFoundException>(()
            => Client(st).BatchWriteItemAsync(new BatchWriteItemRequest
            {
                RequestItems = new Dictionary<string, List<WriteRequest>>
                {
                    ["NonExistent"] =
                    [
                        new WriteRequest
                        {
                            PutRequest = new PutRequest
                            {
                                Item = new Dictionary<string, AttributeValue>
                                {
                                    ["PK"] = new() { S = "X" },
                                    ["SK"] = new() { S = "Y" }
                                }
                            }
                        }
                    ]
                }
            }, TestContext.Current.CancellationToken));

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task BatchWriteItemAsync_EmptyRequestItems_ThrowsException(StoreType st)
    {
        var client = Client(st);
        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.BatchWriteItemAsync(new BatchWriteItemRequest
            {
                RequestItems = []
            }, TestContext.Current.CancellationToken));

        Assert.Contains("requestItems", ex.Message);
    }

    [Fact]
    public async Task BatchWriteItemAsync_AfterDispose_ThrowsObjectDisposedException()
    {
#pragma warning disable IDISP016, IDISP017
        var disposableClient = new DynamoDbClient(new DynamoDbLiteOptions(
            $"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));
        disposableClient.Dispose();

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            disposableClient.BatchWriteItemAsync(new BatchWriteItemRequest
            {
                RequestItems = new Dictionary<string, List<WriteRequest>>
                {
                    [TestTableName] =
                    [
                        new WriteRequest { PutRequest = new PutRequest { Item = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "X" }, ["SK"] = new() { S = "Y" }
                        }}}
                    ]
                }
            }, TestContext.Current.CancellationToken));
#pragma warning restore IDISP016, IDISP017
    }
}
