using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

public sealed class ItemCrudTests
    : DynamoDbClientFixture
{
    protected override async ValueTask SetupAsync(CancellationToken ct)
    {
        await CreateTestTableAsync(Client(StoreType.MemoryBased), ct);
        await CreateTestTableAsync(Client(StoreType.FileBased), ct);
    }

    // ── PutItemAsync ───────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task PutItemAsync_NewItem_Succeeds(StoreType st)
    {
        var client = Client(st);

        var response = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" },
                ["name"] = new() { S = "Alice" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.HttpStatusCode);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task PutItemAsync_SimpleOverload_Succeeds(StoreType st)
    {
        var client = Client(st);

        var response = await client.PutItemAsync(
            "TestTable",
            new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#2" },
                ["SK"] = new() { S = "PROFILE" },
                ["name"] = new() { S = "Bob" }
            },
            TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.HttpStatusCode);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task PutItemAsync_ReturnValues_AllOld_ReturnsOldItem(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" },
                ["name"] = new() { S = "Alice Updated" }
            },
            ReturnValues = ReturnValue.ALL_OLD
        }, TestContext.Current.CancellationToken);

        Assert.NotNull(response.Attributes);
        Assert.Equal("Alice", response.Attributes["name"].S);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task PutItemAsync_ReturnValues_None_ReturnsNoAttributes(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" },
                ["name"] = new() { S = "Alice Updated" }
            },
            ReturnValues = ReturnValue.NONE
        }, TestContext.Current.CancellationToken);

        Assert.Null(response.Attributes);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task PutItemAsync_NonExistentTable_ThrowsResourceNotFoundException(StoreType st)
        => _ = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            Client(st).PutItemAsync(new PutItemRequest
            {
                TableName = "DoesNotExist",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = "X" },
                    ["SK"] = new() { S = "Y" }
                }
            }, TestContext.Current.CancellationToken));

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task PutItemAsync_MissingKeyAttribute_ThrowsException(StoreType st)
        => _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            Client(st).PutItemAsync(new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = "USER#1" }
                    // Missing SK
                }
            }, TestContext.Current.CancellationToken));

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task PutItemAsync_WrongKeyType_ThrowsException(StoreType st)
        => _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            Client(st).PutItemAsync(new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { N = "123" },  // Should be S
                    ["SK"] = new() { S = "PROFILE" }
                }
            }, TestContext.Current.CancellationToken));

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task PutItemAsync_ConditionExpression_Passes(StoreType st)
    {
        var client = Client(st);

        var response = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#99" },
                ["SK"] = new() { S = "PROFILE" },
                ["name"] = new() { S = "New User" }
            },
            ConditionExpression = "attribute_not_exists(PK)"
        }, TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.HttpStatusCode);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task PutItemAsync_ConditionExpression_Fails_ThrowsConditionalCheckFailedException(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#100", "PROFILE", "Existing");

        _ = await Assert.ThrowsAsync<ConditionalCheckFailedException>(() =>
            client.PutItemAsync(new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = "USER#100" },
                    ["SK"] = new() { S = "PROFILE" },
                    ["name"] = new() { S = "Should Fail" }
                },
                ConditionExpression = "attribute_not_exists(PK)"
            }, TestContext.Current.CancellationToken));
    }

    // ── GetItemAsync ───────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task GetItemAsync_ExistingItem_ReturnsItem(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.True(response.IsItemSet);
        Assert.Equal("Alice", response.Item["name"].S);
        Assert.Equal("USER#1", response.Item["PK"].S);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task GetItemAsync_SimpleOverload_ReturnsItem(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.GetItemAsync(
            "TestTable",
            new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            },
            TestContext.Current.CancellationToken);

        Assert.Equal("Alice", response.Item["name"].S);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task GetItemAsync_NonExistentItem_ReturnsEmptyResponse(StoreType st)
    {
        var client = Client(st);

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "NONEXISTENT" },
                ["SK"] = new() { S = "PROFILE" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.False(response.IsItemSet);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task GetItemAsync_WithProjectionExpression_ReturnsSubset(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            },
            ProjectionExpression = "#n",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#n"] = "name"
            }
        }, TestContext.Current.CancellationToken);

        Assert.True(response.IsItemSet);
        _ = Assert.Single(response.Item);
        Assert.Equal("Alice", response.Item["name"].S);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task GetItemAsync_ConsistentRead_AcceptedAsNoOp(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.GetItemAsync(
            "TestTable",
            new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            },
            true,
            TestContext.Current.CancellationToken);

        Assert.Equal("Alice", response.Item["name"].S);
    }

    // ── DeleteItemAsync ────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DeleteItemAsync_ExistingItem_RemovesItem(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        _ = await client.DeleteItemAsync(new DeleteItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            }
        }, TestContext.Current.CancellationToken);

        var getResponse = await client.GetItemAsync(new GetItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.False(getResponse.IsItemSet);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DeleteItemAsync_SimpleOverload_RemovesItem(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        _ = await client.DeleteItemAsync(
            "TestTable",
            new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            },
            TestContext.Current.CancellationToken);

        var getResponse = await client.GetItemAsync(
            "TestTable",
            new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            },
            TestContext.Current.CancellationToken);

        Assert.False(getResponse.IsItemSet);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DeleteItemAsync_ReturnValues_AllOld_ReturnsOldItem(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.DeleteItemAsync(new DeleteItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            },
            ReturnValues = ReturnValue.ALL_OLD
        }, TestContext.Current.CancellationToken);

        Assert.NotNull(response.Attributes);
        Assert.Equal("Alice", response.Attributes["name"].S);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DeleteItemAsync_NonExistentItem_Succeeds(StoreType st)
    {
        var client = Client(st);

        var response = await client.DeleteItemAsync(new DeleteItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "NONEXISTENT" },
                ["SK"] = new() { S = "PROFILE" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.HttpStatusCode);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DeleteItemAsync_ConditionExpression_Passes(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.DeleteItemAsync(new DeleteItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            },
            ConditionExpression = "attribute_exists(PK)"
        }, TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.HttpStatusCode);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DeleteItemAsync_ConditionExpression_Fails_ThrowsConditionalCheckFailedException(StoreType st)
        => _ = await Assert.ThrowsAsync<ConditionalCheckFailedException>(() =>
            Client(st).DeleteItemAsync(new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = "NONEXISTENT" },
                    ["SK"] = new() { S = "PROFILE" }
                },
                ConditionExpression = "attribute_exists(PK)"
            }, TestContext.Current.CancellationToken));

    // ── UpdateItemAsync ────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task UpdateItemAsync_SetAttribute_UpdatesValue(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            },
            UpdateExpression = "SET #n = :name",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#n"] = "name" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":name"] = new() { S = "Alice Updated" }
            }
        }, TestContext.Current.CancellationToken);

        var getResponse = await client.GetItemAsync(new GetItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal("Alice Updated", getResponse.Item["name"].S);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task UpdateItemAsync_ReturnValues_AllNew_ReturnsUpdatedItem(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            },
            UpdateExpression = "SET #n = :name",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#n"] = "name" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":name"] = new() { S = "Alice Updated" }
            },
            ReturnValues = ReturnValue.ALL_NEW
        }, TestContext.Current.CancellationToken);

        Assert.NotNull(response.Attributes);
        Assert.Equal("Alice Updated", response.Attributes["name"].S);
        Assert.Equal("USER#1", response.Attributes["PK"].S);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task UpdateItemAsync_ReturnValues_AllOld_ReturnsOldItem(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            },
            UpdateExpression = "SET #n = :name",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#n"] = "name" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":name"] = new() { S = "Alice Updated" }
            },
            ReturnValues = ReturnValue.ALL_OLD
        }, TestContext.Current.CancellationToken);

        Assert.NotNull(response.Attributes);
        Assert.Equal("Alice", response.Attributes["name"].S);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task UpdateItemAsync_RemoveAttribute_RemovesFromItem(StoreType st)
    {
        var client = Client(st);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" },
                ["name"] = new() { S = "Alice" },
                ["email"] = new() { S = "alice@example.com" }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            },
            UpdateExpression = "REMOVE email"
        }, TestContext.Current.CancellationToken);

        var getResponse = await client.GetItemAsync(new GetItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.False(getResponse.Item.ContainsKey("email"));
        Assert.Equal("Alice", getResponse.Item["name"].S);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task UpdateItemAsync_ArithmeticAdd_IncrementsNumber(StoreType st)
    {
        var client = Client(st);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "COUNTER#1" },
                ["SK"] = new() { S = "VALUE" },
                ["count"] = new() { N = "10" }
            }
        }, TestContext.Current.CancellationToken);

        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "COUNTER#1" },
                ["SK"] = new() { S = "VALUE" }
            },
            UpdateExpression = "SET #c = #c + :inc",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#c"] = "count" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":inc"] = new() { N = "5" }
            }
        }, TestContext.Current.CancellationToken);

        var getResponse = await client.GetItemAsync(new GetItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "COUNTER#1" },
                ["SK"] = new() { S = "VALUE" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal("15", getResponse.Item["count"].N);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task UpdateItemAsync_IfNotExists_SetsDefault(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            },
            UpdateExpression = "SET score = if_not_exists(score, :default)",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":default"] = new() { N = "0" }
            }
        }, TestContext.Current.CancellationToken);

        var getResponse = await client.GetItemAsync(new GetItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.Equal("0", getResponse.Item["score"].N);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task UpdateItemAsync_NonExistentItem_CreatesNewItem(StoreType st)
    {
        var client = Client(st);

        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "NEW#1" },
                ["SK"] = new() { S = "ITEM" }
            },
            UpdateExpression = "SET #n = :name",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#n"] = "name" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":name"] = new() { S = "Created" }
            }
        }, TestContext.Current.CancellationToken);

        var getResponse = await client.GetItemAsync(new GetItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "NEW#1" },
                ["SK"] = new() { S = "ITEM" }
            }
        }, TestContext.Current.CancellationToken);

        Assert.True(getResponse.IsItemSet);
        Assert.Equal("Created", getResponse.Item["name"].S);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task UpdateItemAsync_ConditionExpression_Fails_ThrowsConditionalCheckFailedException(StoreType st)
        => _ = await Assert.ThrowsAsync<ConditionalCheckFailedException>(() =>
            Client(st).UpdateItemAsync(new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = "NONEXISTENT" },
                    ["SK"] = new() { S = "PROFILE" }
                },
                UpdateExpression = "SET #n = :name",
                ExpressionAttributeNames = new Dictionary<string, string> { ["#n"] = "name" },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":name"] = new() { S = "Test" }
                },
                ConditionExpression = "attribute_exists(PK)"
            }, TestContext.Current.CancellationToken));

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task UpdateItemAsync_ModifyKeyAttribute_ThrowsException(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.UpdateItemAsync(new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = "USER#1" },
                    ["SK"] = new() { S = "PROFILE" }
                },
                UpdateExpression = "SET PK = :newPk",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":newPk"] = new() { S = "USER#2" }
                }
            }, TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task UpdateItemAsync_ReturnValues_UpdatedOld_ReturnsModifiedAttributes(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            },
            UpdateExpression = "SET #n = :name",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#n"] = "name" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":name"] = new() { S = "Alice Updated" }
            },
            ReturnValues = ReturnValue.UPDATED_OLD
        }, TestContext.Current.CancellationToken);

        Assert.NotNull(response.Attributes);
        _ = Assert.Single(response.Attributes);
        Assert.Equal("Alice", response.Attributes["name"].S);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task UpdateItemAsync_ReturnValues_UpdatedNew_ReturnsModifiedAttributes(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");

        var response = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            },
            UpdateExpression = "SET #n = :name",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#n"] = "name" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":name"] = new() { S = "Alice Updated" }
            },
            ReturnValues = ReturnValue.UPDATED_NEW
        }, TestContext.Current.CancellationToken);

        Assert.NotNull(response.Attributes);
        _ = Assert.Single(response.Attributes);
        Assert.Equal("Alice Updated", response.Attributes["name"].S);
    }

    // ── Item count tracking ────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task PutItem_UpdatesItemCount(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");
        _ = await PutTestItemAsync(client, "USER#2", "PROFILE", "Bob");

        var describeResponse = await client.DescribeTableAsync("TestTable", TestContext.Current.CancellationToken);

        Assert.Equal(2, describeResponse.Table.ItemCount);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DeleteItem_UpdatesItemCount(StoreType st)
    {
        var client = Client(st);
        _ = await PutTestItemAsync(client, "USER#1", "PROFILE", "Alice");
        _ = await PutTestItemAsync(client, "USER#2", "PROFILE", "Bob");

        _ = await client.DeleteItemAsync(new DeleteItemRequest
        {
            TableName = "TestTable",
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "PROFILE" }
            }
        }, TestContext.Current.CancellationToken);

        var describeResponse = await client.DescribeTableAsync("TestTable", TestContext.Current.CancellationToken);

        Assert.Equal(1, describeResponse.Table.ItemCount);
    }

    // ── Disposal ───────────────────────────────────────────────────────

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task PutItem_AfterDispose_ThrowsObjectDisposedException(StoreType st)
    {
#pragma warning disable IDISP016, IDISP017
        var client = Client(st);
        client.Dispose();
        _ = await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.PutItemAsync(new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = "X" },
                    ["SK"] = new() { S = "Y" }
                }
            }, TestContext.Current.CancellationToken));
#pragma warning restore IDISP016, IDISP017
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private static Task<PutItemResponse> PutTestItemAsync(DynamoDbClient client, string pk, string sk, string name) =>
        client.PutItemAsync(new PutItemRequest
        {
            TableName = "TestTable",
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = pk },
                ["SK"] = new() { S = sk },
                ["name"] = new() { S = name }
            }
        }, TestContext.Current.CancellationToken);
}
