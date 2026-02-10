using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

public abstract class TransactionTestsBase
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

    private async Task PutTestItemAsync(string pk, string sk, string name)
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

    private async Task<Dictionary<string, AttributeValue>?> GetTestItemAsync(string pk, string sk)
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

    private async Task CreateSecondTableAsync()
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

    // ── TransactWriteItems — happy path ─────────────────────────────────

    [Fact]
    public async Task TransactWriteItems_PutMultipleItems_AllWritten()
    {
        var response = await client.TransactWriteItemsAsync(new TransactWriteItemsRequest
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
                            ["PK"] = new() { S = "USER#1" },
                            ["SK"] = new() { S = "PROFILE" },
                            ["name"] = new() { S = "Alice" }
                        }
                    }
                },
                new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = "TestTable",
                        Item = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#2" },
                            ["SK"] = new() { S = "PROFILE" },
                            ["name"] = new() { S = "Bob" }
                        }
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.HttpStatusCode);

        var alice = await GetTestItemAsync("USER#1", "PROFILE");
        var bob = await GetTestItemAsync("USER#2", "PROFILE");
        Assert.NotNull(alice);
        Assert.Equal("Alice", alice["name"].S);
        Assert.NotNull(bob);
        Assert.Equal("Bob", bob["name"].S);
    }

    [Fact]
    public async Task TransactWriteItems_UpdateAndDelete_BothApplied()
    {
        await PutTestItemAsync("USER#1", "PROFILE", "Alice");
        await PutTestItemAsync("USER#2", "PROFILE", "Bob");

        _ = await client.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            TransactItems =
            [
                new TransactWriteItem
                {
                    Update = new Update
                    {
                        TableName = "TestTable",
                        Key = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#1" },
                            ["SK"] = new() { S = "PROFILE" }
                        },
                        UpdateExpression = "SET #n = :n",
                        ExpressionAttributeNames = new Dictionary<string, string> { ["#n"] = "name" },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            [":n"] = new() { S = "Alice Updated" }
                        }
                    }
                },
                new TransactWriteItem
                {
                    Delete = new Delete
                    {
                        TableName = "TestTable",
                        Key = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#2" },
                            ["SK"] = new() { S = "PROFILE" }
                        }
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        var alice = await GetTestItemAsync("USER#1", "PROFILE");
        var bob = await GetTestItemAsync("USER#2", "PROFILE");
        Assert.NotNull(alice);
        Assert.Equal("Alice Updated", alice["name"].S);
        Assert.Null(bob);
    }

    [Fact]
    public async Task TransactWriteItems_ConditionCheckPasses_WritesSucceed()
    {
        await PutTestItemAsync("USER#1", "PROFILE", "Alice");

        _ = await client.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            TransactItems =
            [
                new TransactWriteItem
                {
                    ConditionCheck = new ConditionCheck
                    {
                        TableName = "TestTable",
                        Key = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#1" },
                            ["SK"] = new() { S = "PROFILE" }
                        },
                        ConditionExpression = "attribute_exists(PK)"
                    }
                },
                new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = "TestTable",
                        Item = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#2" },
                            ["SK"] = new() { S = "PROFILE" },
                            ["name"] = new() { S = "Bob" }
                        }
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        var bob = await GetTestItemAsync("USER#2", "PROFILE");
        Assert.NotNull(bob);
        Assert.Equal("Bob", bob["name"].S);
    }

    [Fact]
    public async Task TransactWriteItems_MultipleTablesWork()
    {
        await CreateSecondTableAsync();

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
                            ["PK"] = new() { S = "T1#1" },
                            ["SK"] = new() { S = "DATA" },
                            ["val"] = new() { S = "table1" }
                        }
                    }
                },
                new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = "SecondTable",
                        Item = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "T2#1" },
                            ["SK"] = new() { S = "DATA" },
                            ["val"] = new() { S = "table2" }
                        }
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        var item1 = await GetTestItemAsync("T1#1", "DATA");
        Assert.NotNull(item1);
        Assert.Equal("table1", item1["val"].S);

        var response2 = await client.GetItemAsync(new GetItemRequest
        {
            TableName = "SecondTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "T2#1" },
                ["SK"] = new() { S = "DATA" }
            }
        }, TestContext.Current.CancellationToken);
        Assert.Equal("table2", response2.Item["val"].S);
    }

    // ── TransactWriteItems — condition failures ─────────────────────────

    [Fact]
    public async Task TransactWriteItems_ConditionFails_ThrowsTransactionCanceledException()
    {
        await PutTestItemAsync("USER#1", "PROFILE", "Alice");

        var ex = await Assert.ThrowsAsync<TransactionCanceledException>(() =>
            client.TransactWriteItemsAsync(new TransactWriteItemsRequest
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
                                ["PK"] = new() { S = "USER#1" },
                                ["SK"] = new() { S = "PROFILE" },
                                ["name"] = new() { S = "New Alice" }
                            },
                            ConditionExpression = "attribute_not_exists(PK)"
                        }
                    },
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "TestTable",
                            Item = new Dictionary<string, AttributeValue>
                            {
                                ["PK"] = new() { S = "USER#2" },
                                ["SK"] = new() { S = "PROFILE" },
                                ["name"] = new() { S = "Bob" }
                            }
                        }
                    }
                ]
            }, TestContext.Current.CancellationToken));

        Assert.Equal(2, ex.CancellationReasons.Count);
        Assert.Equal("ConditionalCheckFailed", ex.CancellationReasons[0].Code);
        Assert.Equal("None", ex.CancellationReasons[1].Code);

        // No writes should have happened
        var alice = await GetTestItemAsync("USER#1", "PROFILE");
        Assert.Equal("Alice", alice!["name"].S);
        var bob = await GetTestItemAsync("USER#2", "PROFILE");
        Assert.Null(bob);
    }

    [Fact]
    public async Task TransactWriteItems_ConditionFails_ReturnValuesOnFailure_ReturnsOldItem()
    {
        await PutTestItemAsync("USER#1", "PROFILE", "Alice");

        var ex = await Assert.ThrowsAsync<TransactionCanceledException>(() =>
            client.TransactWriteItemsAsync(new TransactWriteItemsRequest
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
                                ["PK"] = new() { S = "USER#1" },
                                ["SK"] = new() { S = "PROFILE" },
                                ["name"] = new() { S = "New Alice" }
                            },
                            ConditionExpression = "attribute_not_exists(PK)",
                            ReturnValuesOnConditionCheckFailure = "ALL_OLD"
                        }
                    }
                ]
            }, TestContext.Current.CancellationToken));

        _ = Assert.Single(ex.CancellationReasons);
        Assert.Equal("ConditionalCheckFailed", ex.CancellationReasons[0].Code);
        Assert.NotNull(ex.CancellationReasons[0].Item);
        Assert.Equal("Alice", ex.CancellationReasons[0].Item["name"].S);
    }

    [Fact]
    public async Task TransactWriteItems_ConditionCheckFails_NoWritesOccur()
    {
        await PutTestItemAsync("USER#1", "PROFILE", "Alice");

        _ = await Assert.ThrowsAsync<TransactionCanceledException>(() =>
            client.TransactWriteItemsAsync(new TransactWriteItemsRequest
            {
                TransactItems =
                [
                    new TransactWriteItem
                    {
                        ConditionCheck = new ConditionCheck
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                ["PK"] = new() { S = "USER#1" },
                                ["SK"] = new() { S = "PROFILE" }
                            },
                            ConditionExpression = "#n = :n",
                            ExpressionAttributeNames = new Dictionary<string, string> { ["#n"] = "name" },
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                            {
                                [":n"] = new() { S = "WrongName" }
                            }
                        }
                    },
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "TestTable",
                            Item = new Dictionary<string, AttributeValue>
                            {
                                ["PK"] = new() { S = "USER#99" },
                                ["SK"] = new() { S = "PROFILE" },
                                ["name"] = new() { S = "Should Not Exist" }
                            }
                        }
                    }
                ]
            }, TestContext.Current.CancellationToken));

        var shouldNotExist = await GetTestItemAsync("USER#99", "PROFILE");
        Assert.Null(shouldNotExist);
    }

    // ── TransactWriteItems — validation errors ──────────────────────────

    [Fact]
    public async Task TransactWriteItems_MoreThan100Actions_Throws()
    {
        var items = Enumerable.Range(1, 101).Select(i => new TransactWriteItem
        {
            Put = new Put
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = $"PK#{i}" },
                    ["SK"] = new() { S = "DATA" }
                }
            }
        }).ToList();

        _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.TransactWriteItemsAsync(new TransactWriteItemsRequest
            {
                TransactItems = items
            }, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TransactWriteItems_DuplicateKeys_Throws()
        => _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(()
            => client.TransactWriteItemsAsync(new TransactWriteItemsRequest
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
                                ["PK"] = new() { S = "SAME" },
                                ["SK"] = new() { S = "KEY" },
                                ["v"] = new() { S = "1" }
                            }
                        }
                    },
                    new TransactWriteItem
                    {
                        Delete = new Delete
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                ["PK"] = new() { S = "SAME" },
                                ["SK"] = new() { S = "KEY" }
                            }
                        }
                    }
                ]
            }, TestContext.Current.CancellationToken));

    [Fact]
    public async Task TransactWriteItems_MissingTable_ThrowsResourceNotFound()
        => _ = await Assert.ThrowsAsync<ResourceNotFoundException>(()
            => client.TransactWriteItemsAsync(new TransactWriteItemsRequest
            {
                TransactItems =
                [
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "NonExistent",
                            Item = new Dictionary<string, AttributeValue>
                            {
                                ["PK"] = new() { S = "X" },
                                ["SK"] = new() { S = "Y" }
                            }
                        }
                    }
                ]
            }, TestContext.Current.CancellationToken));

    [Fact]
    public async Task TransactWriteItems_EmptyTransactItems_Throws()
        => _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(()
            => client.TransactWriteItemsAsync(new TransactWriteItemsRequest
            {
                TransactItems = []
            }, TestContext.Current.CancellationToken));

    // ── TransactWriteItems — idempotency ────────────────────────────────

    [Fact]
    public async Task TransactWriteItems_SameClientRequestToken_ReturnsCachedResponse()
    {
        var token = Guid.NewGuid().ToString();
        var request = new TransactWriteItemsRequest
        {
            ClientRequestToken = token,
            TransactItems =
            [
                new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = "TestTable",
                        Item = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "IDEMPOTENT#1" },
                            ["SK"] = new() { S = "DATA" },
                            ["counter"] = new() { N = "1" }
                        }
                    }
                }
            ]
        };

        var response1 = await client.TransactWriteItemsAsync(request, TestContext.Current.CancellationToken);

        // Modify the item directly, then replay the same token
        await PutTestItemAsync("IDEMPOTENT#1", "DATA", "modified");

        var response2 = await client.TransactWriteItemsAsync(request, TestContext.Current.CancellationToken);

        // Should return same cached response, not re-execute (item should still be "modified")
        Assert.Same(response1, response2);
        var item = await GetTestItemAsync("IDEMPOTENT#1", "DATA");
        Assert.Equal("modified", item!["name"].S);
    }

    [Fact]
    public async Task TransactWriteItems_DifferentToken_ExecutesNormally()
    {
        _ = await client.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            ClientRequestToken = "token-1",
            TransactItems =
            [
                new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = "TestTable",
                        Item = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "TOKEN#1" },
                            ["SK"] = new() { S = "DATA" },
                            ["val"] = new() { S = "first" }
                        }
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        _ = await client.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            ClientRequestToken = "token-2",
            TransactItems =
            [
                new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = "TestTable",
                        Item = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "TOKEN#1" },
                            ["SK"] = new() { S = "DATA" },
                            ["val"] = new() { S = "second" }
                        }
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        var item = await GetTestItemAsync("TOKEN#1", "DATA");
        Assert.Equal("second", item!["val"].S);
    }

    // ── TransactGetItems ────────────────────────────────────────────────

    [Fact]
    public async Task TransactGetItems_MultipleItems_ReturnsAll()
    {
        await PutTestItemAsync("USER#1", "PROFILE", "Alice");
        await PutTestItemAsync("USER#2", "PROFILE", "Bob");

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
                            ["PK"] = new() { S = "USER#1" },
                            ["SK"] = new() { S = "PROFILE" }
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
                            ["PK"] = new() { S = "USER#2" },
                            ["SK"] = new() { S = "PROFILE" }
                        }
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.HttpStatusCode);
        Assert.Equal(2, response.Responses.Count);
        Assert.Equal("Alice", response.Responses[0].Item["name"].S);
        Assert.Equal("Bob", response.Responses[1].Item["name"].S);
    }

    [Fact]
    public async Task TransactGetItems_WithProjectionExpression_FiltersAttributes()
    {
        await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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
                            ["PK"] = new() { S = "USER#1" },
                            ["SK"] = new() { S = "PROFILE" }
                        },
                        ProjectionExpression = "#n",
                        ExpressionAttributeNames = new Dictionary<string, string> { ["#n"] = "name" }
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Responses);
        Assert.True(response.Responses[0].Item.ContainsKey("name"));
        Assert.False(response.Responses[0].Item.ContainsKey("PK"));
    }

    [Fact]
    public async Task TransactGetItems_NonexistentItem_ReturnsNullItem()
    {
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
                            ["PK"] = new() { S = "DOES_NOT_EXIST" },
                            ["SK"] = new() { S = "NOPE" }
                        }
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Responses);
        Assert.Null(response.Responses[0].Item);
    }

    [Fact]
    public async Task TransactGetItems_MoreThan100Items_Throws()
    {
        var items = Enumerable.Range(1, 101).Select(i => new TransactGetItem
        {
            Get = new Get
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = $"PK#{i}" },
                    ["SK"] = new() { S = "DATA" }
                }
            }
        }).ToList();

        _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.TransactGetItemsAsync(new TransactGetItemsRequest
            {
                TransactItems = items
            }, TestContext.Current.CancellationToken));
    }

    // ── TransactWriteItems — index maintenance ──────────────────────────

    [Fact]
    public async Task TransactWriteItems_WithGSI_IndexMaintained()
    {
        // Create a table with a GSI
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "IndexedTable",
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
                    KeySchema =
                    [
                        new KeySchemaElement { AttributeName = "GSI_PK", KeyType = KeyType.HASH }
                    ],
                    Projection = new Projection { ProjectionType = ProjectionType.ALL }
                }
            ]
        }, TestContext.Current.CancellationToken);

        _ = await client.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            TransactItems =
            [
                new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = "IndexedTable",
                        Item = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "ITEM#1" },
                            ["SK"] = new() { S = "DATA" },
                            ["GSI_PK"] = new() { S = "INDEX#1" },
                            ["val"] = new() { S = "indexed" }
                        }
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        // Query the GSI to verify index was maintained
        var queryResponse = await client.QueryAsync(new QueryRequest
        {
            TableName = "IndexedTable",
            IndexName = "GSI1",
            KeyConditionExpression = "GSI_PK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "INDEX#1" }
            }
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(queryResponse.Items);
        Assert.Equal("indexed", queryResponse.Items[0]["val"].S);
    }

    // ── TransactWriteItems — Update creates new item ────────────────────

    [Fact]
    public async Task TransactWriteItems_UpdateNonexistentItem_CreatesIt()
    {
        _ = await client.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            TransactItems =
            [
                new TransactWriteItem
                {
                    Update = new Update
                    {
                        TableName = "TestTable",
                        Key = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "NEW#1" },
                            ["SK"] = new() { S = "DATA" }
                        },
                        UpdateExpression = "SET #n = :n",
                        ExpressionAttributeNames = new Dictionary<string, string> { ["#n"] = "name" },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            [":n"] = new() { S = "Created" }
                        }
                    }
                }
            ]
        }, TestContext.Current.CancellationToken);

        var item = await GetTestItemAsync("NEW#1", "DATA");
        Assert.NotNull(item);
        Assert.Equal("Created", item["name"].S);
    }

    // ── TransactWriteItems — Update with failing condition ───────────────

    [Fact]
    public async Task TransactWriteItems_UpdateWithFailingCondition_Throws()
    {
        await PutTestItemAsync("USER#1", "PROFILE", "Alice");

        var ex = await Assert.ThrowsAsync<TransactionCanceledException>(() =>
            client.TransactWriteItemsAsync(new TransactWriteItemsRequest
            {
                TransactItems =
                [
                    new TransactWriteItem
                    {
                        Update = new Update
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                ["PK"] = new() { S = "USER#1" },
                                ["SK"] = new() { S = "PROFILE" }
                            },
                            UpdateExpression = "SET #n = :n",
                            ConditionExpression = "#n = :old",
                            ExpressionAttributeNames = new Dictionary<string, string> { ["#n"] = "name" },
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                            {
                                [":n"] = new() { S = "Updated" },
                                [":old"] = new() { S = "WrongName" }
                            }
                        }
                    }
                ]
            }, TestContext.Current.CancellationToken));

        _ = Assert.Single(ex.CancellationReasons);
        Assert.Equal("ConditionalCheckFailed", ex.CancellationReasons[0].Code);

        // Item should be unchanged
        var item = await GetTestItemAsync("USER#1", "PROFILE");
        Assert.Equal("Alice", item!["name"].S);
    }

    // ── TransactWriteItems — Update without UpdateExpression ─────────────

    [Fact]
    public async Task TransactWriteItems_UpdateWithoutUpdateExpression_Throws()
        => _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(()
            => client.TransactWriteItemsAsync(new TransactWriteItemsRequest
            {
                TransactItems =
                [
                    new TransactWriteItem
                    {
                        Update = new Update
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                ["PK"] = new() { S = "USER#1" },
                                ["SK"] = new() { S = "PROFILE" }
                            }
                        }
                    }
                ]
            }, TestContext.Current.CancellationToken));

    // ── TransactGetItems — missing table ─────────────────────────────────

    [Fact]
    public async Task TransactGetItems_MissingTable_ThrowsResourceNotFound()
        => _ = await Assert.ThrowsAsync<ResourceNotFoundException>(()
            => client.TransactGetItemsAsync(new TransactGetItemsRequest
            {
                TransactItems =
                [
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = "NonExistent",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                ["PK"] = new() { S = "X" },
                                ["SK"] = new() { S = "Y" }
                            }
                        }
                    }
                ]
            }, TestContext.Current.CancellationToken));
}

public sealed class InMemoryTransactionTests : TransactionTestsBase
{
    protected override DynamoDbClient CreateClient() =>
        new(new DynamoDbLiteOptions($"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));
}

public sealed class FileBasedTransactionTests : TransactionTestsBase
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
