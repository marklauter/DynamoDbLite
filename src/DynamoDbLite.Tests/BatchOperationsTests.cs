using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

public sealed class BatchOperationsTests
    : DynamoDbClientFixture
{
    protected override async ValueTask SetupAsync(CancellationToken ct)
    {
        await CreateTestTableAsync(Client(StoreType.MemoryBased), ct);
        await CreateTestTableAsync(Client(StoreType.FileBased), ct);
    }

    private static async Task PutTestItemAsync(DynamoDbClient client, string pk, string sk, string name)
        => _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = pk },
                ["SK"] = new() { S = sk },
                ["name"] = new() { S = name }
            }
        }, TestContext.Current.CancellationToken);

    private static async Task CreateSecondTableAsync(DynamoDbClient client)
        => _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "SecondTable",
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
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
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
                ["TestTable"] = new()
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
        Assert.Equal(3, response.Responses["TestTable"].Count);
        Assert.Empty(response.UnprocessedKeys);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task BatchGetItemAsync_SimpleOverload_Succeeds(StoreType st)
    {
        var client = Client(st);
        await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.BatchGetItemAsync(
            new Dictionary<string, KeysAndAttributes>
            {
                ["TestTable"] = new()
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
        _ = Assert.Single(response.Responses["TestTable"]);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task BatchGetItemAsync_DictionaryOverload_Succeeds(StoreType st)
    {
        var client = Client(st);
        await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.BatchGetItemAsync(
            new Dictionary<string, KeysAndAttributes>
            {
                ["TestTable"] = new()
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
        _ = Assert.Single(response.Responses["TestTable"]);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task BatchGetItemAsync_WithProjectionExpression_ReturnsProjected(StoreType st)
    {
        var client = Client(st);
        await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

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

        var item = Assert.Single(response.Responses["TestTable"]);
        Assert.Equal("USER#1", item["PK"].S);
        Assert.Equal("Alice", item["name"].S);
        Assert.False(item.ContainsKey("SK"));
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task BatchGetItemAsync_NonExistentKey_OmittedFromResponse(StoreType st)
    {
        var client = Client(st);
        await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

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

        _ = Assert.Single(response.Responses["TestTable"]);
        Assert.Equal("Alice", response.Responses["TestTable"][0]["name"].S);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task BatchGetItemAsync_MultipleTables_ReturnsFromBoth(StoreType st)
    {
        var client = Client(st);
        await CreateSecondTableAsync(client);
        await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "SecondTable",
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
                ["TestTable"] = new()
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
                ["SecondTable"] = new()
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

        _ = Assert.Single(response.Responses["TestTable"]);
        _ = Assert.Single(response.Responses["SecondTable"]);
        Assert.Equal("Alice", response.Responses["TestTable"][0]["name"].S);
        Assert.Equal("100", response.Responses["SecondTable"][0]["total"].N);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
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
                    ["TestTable"] = new() { Keys = keys }
                }
            }, TestContext.Current.CancellationToken));

        Assert.Contains("Too many items", ex.Message);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
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
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
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
                    ["TestTable"] = new()
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
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task BatchWriteItemAsync_MultiplePuts_AllSucceed(StoreType st)
    {
        var client = Client(st);
        var response = await client.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>
            {
                ["TestTable"] =
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

        var get1 = await client.GetItemAsync("TestTable",
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);
        var get2 = await client.GetItemAsync("TestTable",
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#2" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);
        var get3 = await client.GetItemAsync("TestTable",
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#3" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);

        Assert.Equal("Alice", get1.Item["name"].S);
        Assert.Equal("Bob", get2.Item["name"].S);
        Assert.Equal("Charlie", get3.Item["name"].S);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task BatchWriteItemAsync_SimpleOverload_Succeeds(StoreType st)
    {
        var client = Client(st);
        var response = await client.BatchWriteItemAsync(
            new Dictionary<string, List<WriteRequest>>
            {
                ["TestTable"] =
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
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task BatchWriteItemAsync_MultipleDeletes_AllSucceed(StoreType st)
    {
        var client = Client(st);
        await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");
        await PutTestItemAsync(client, "USER#2", "PROFILE", "Bob");

        _ = await client.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>
            {
                ["TestTable"] =
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

        var get1 = await client.GetItemAsync("TestTable",
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);
        var get2 = await client.GetItemAsync("TestTable",
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#2" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);

        Assert.False(get1.IsItemSet);
        Assert.False(get2.IsItemSet);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task BatchWriteItemAsync_MixedPutAndDelete_Succeeds(StoreType st)
    {
        var client = Client(st);
        await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        _ = await client.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>
            {
                ["TestTable"] =
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

        var get1 = await client.GetItemAsync("TestTable",
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);
        var get2 = await client.GetItemAsync("TestTable",
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#2" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);

        Assert.False(get1.IsItemSet);
        Assert.Equal("Bob", get2.Item["name"].S);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task BatchWriteItemAsync_MultipleTables_Succeeds(StoreType st)
    {
        var client = Client(st);
        await CreateSecondTableAsync(client);

        _ = await client.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>
            {
                ["TestTable"] =
                [
                    new WriteRequest { PutRequest = new PutRequest { Item = new Dictionary<string, AttributeValue>
                    {
                        ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" }, ["name"] = new() { S = "Alice" }
                    }}}
                ],
                ["SecondTable"] =
                [
                    new WriteRequest { PutRequest = new PutRequest { Item = new Dictionary<string, AttributeValue>
                    {
                        ["PK"] = new() { S = "ORDER#1" }, ["SK"] = new() { S = "DETAIL" }, ["total"] = new() { N = "50" }
                    }}}
                ]
            }
        }, TestContext.Current.CancellationToken);

        var get1 = await client.GetItemAsync("TestTable",
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "USER#1" }, ["SK"] = new() { S = "PROFILE" } },
            TestContext.Current.CancellationToken);
        var get2 = await client.GetItemAsync("SecondTable",
            new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "ORDER#1" }, ["SK"] = new() { S = "DETAIL" } },
            TestContext.Current.CancellationToken);

        Assert.Equal("Alice", get1.Item["name"].S);
        Assert.Equal("50", get2.Item["total"].N);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
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
                    ["TestTable"] = writes
                }
            }, TestContext.Current.CancellationToken));

        Assert.Contains("Too many items", ex.Message);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task BatchWriteItemAsync_DuplicateKeys_ThrowsException(StoreType st)
    {
        var client = Client(st);
        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.BatchWriteItemAsync(new BatchWriteItemRequest
            {
                RequestItems = new Dictionary<string, List<WriteRequest>>
                {
                    ["TestTable"] =
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
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
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
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
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
                    ["TestTable"] =
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
