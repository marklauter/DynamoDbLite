using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite.Tests;

public abstract class ItemCrudTestsBase
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

    // ── PutItemAsync ───────────────────────────────────────────────────

    [Fact]
    public async Task PutItemAsync_NewItem_Succeeds()
    {
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

    [Fact]
    public async Task PutItemAsync_SimpleOverload_Succeeds()
    {
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

    [Fact]
    public async Task PutItemAsync_ReturnValues_AllOld_ReturnsOldItem()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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

    [Fact]
    public async Task PutItemAsync_ReturnValues_None_ReturnsNoAttributes()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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

    [Fact]
    public async Task PutItemAsync_NonExistentTable_ThrowsResourceNotFoundException()
        => _ = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            client.PutItemAsync(new PutItemRequest
            {
                TableName = "DoesNotExist",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = "X" },
                    ["SK"] = new() { S = "Y" }
                }
            }, TestContext.Current.CancellationToken));

    [Fact]
    public async Task PutItemAsync_MissingKeyAttribute_ThrowsException()
        => _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.PutItemAsync(new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = "USER#1" }
                    // Missing SK
                }
            }, TestContext.Current.CancellationToken));

    [Fact]
    public async Task PutItemAsync_WrongKeyType_ThrowsException()
        => _ = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.PutItemAsync(new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { N = "123" },  // Should be S
                    ["SK"] = new() { S = "PROFILE" }
                }
            }, TestContext.Current.CancellationToken));

    [Fact]
    public async Task PutItemAsync_ConditionExpression_Passes()
    {
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

    [Fact]
    public async Task PutItemAsync_ConditionExpression_Fails_ThrowsConditionalCheckFailedException()
    {
        _ = await PutTestItemAsync("USER#100", "PROFILE", "Existing");

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

    [Fact]
    public async Task GetItemAsync_ExistingItem_ReturnsItem()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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

    [Fact]
    public async Task GetItemAsync_SimpleOverload_ReturnsItem()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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

    [Fact]
    public async Task GetItemAsync_NonExistentItem_ReturnsEmptyResponse()
    {
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

    [Fact]
    public async Task GetItemAsync_WithProjectionExpression_ReturnsSubset()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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

    [Fact]
    public async Task GetItemAsync_ConsistentRead_AcceptedAsNoOp()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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

    [Fact]
    public async Task DeleteItemAsync_ExistingItem_RemovesItem()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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

    [Fact]
    public async Task DeleteItemAsync_SimpleOverload_RemovesItem()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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

    [Fact]
    public async Task DeleteItemAsync_ReturnValues_AllOld_ReturnsOldItem()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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

    [Fact]
    public async Task DeleteItemAsync_NonExistentItem_Succeeds()
    {
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

    [Fact]
    public async Task DeleteItemAsync_ConditionExpression_Passes()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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

    [Fact]
    public async Task DeleteItemAsync_ConditionExpression_Fails_ThrowsConditionalCheckFailedException()
        => _ = await Assert.ThrowsAsync<ConditionalCheckFailedException>(() =>
            client.DeleteItemAsync(new DeleteItemRequest
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

    [Fact]
    public async Task UpdateItemAsync_SetAttribute_UpdatesValue()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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

    [Fact]
    public async Task UpdateItemAsync_ReturnValues_AllNew_ReturnsUpdatedItem()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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

    [Fact]
    public async Task UpdateItemAsync_ReturnValues_AllOld_ReturnsOldItem()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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

    [Fact]
    public async Task UpdateItemAsync_RemoveAttribute_RemovesFromItem()
    {
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

    [Fact]
    public async Task UpdateItemAsync_ArithmeticAdd_IncrementsNumber()
    {
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

    [Fact]
    public async Task UpdateItemAsync_IfNotExists_SetsDefault()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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

    [Fact]
    public async Task UpdateItemAsync_NonExistentItem_CreatesNewItem()
    {
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

    [Fact]
    public async Task UpdateItemAsync_ConditionExpression_Fails_ThrowsConditionalCheckFailedException()
        => _ = await Assert.ThrowsAsync<ConditionalCheckFailedException>(() =>
            client.UpdateItemAsync(new UpdateItemRequest
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

    [Fact]
    public async Task UpdateItemAsync_ModifyKeyAttribute_ThrowsException()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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

    [Fact]
    public async Task UpdateItemAsync_ReturnValues_UpdatedOld_ReturnsModifiedAttributes()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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

    [Fact]
    public async Task UpdateItemAsync_ReturnValues_UpdatedNew_ReturnsModifiedAttributes()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");

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

    [Fact]
    public async Task PutItem_UpdatesItemCount()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");
        _ = await PutTestItemAsync("USER#2", "PROFILE", "Bob");

        var describeResponse = await client.DescribeTableAsync("TestTable", TestContext.Current.CancellationToken);

        Assert.Equal(2, describeResponse.Table.ItemCount);
    }

    [Fact]
    public async Task DeleteItem_UpdatesItemCount()
    {
        _ = await PutTestItemAsync("USER#1", "PROFILE", "Alice");
        _ = await PutTestItemAsync("USER#2", "PROFILE", "Bob");

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

    [Fact]
    public async Task PutItem_AfterDispose_ThrowsObjectDisposedException()
    {
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
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private Task<PutItemResponse> PutTestItemAsync(string pk, string sk, string name) =>
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

public sealed class InMemoryItemCrudTests : ItemCrudTestsBase
{
    protected override DynamoDbClient CreateClient() =>
        new(new DynamoDbLiteOptions($"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));
}

public sealed class FileBasedItemCrudTests : ItemCrudTestsBase
{
    private string? dbPath;

    protected override DynamoDbClient CreateClient()
    {
        var (c, path) = Fixtures.FileBasedTestHelper.CreateFileBasedClient();
        dbPath = path;
        return c;
    }

    public override ValueTask DisposeAsync()
    {
        var result = base.DisposeAsync();
        Fixtures.FileBasedTestHelper.Cleanup(dbPath);
        return result;
    }
}
