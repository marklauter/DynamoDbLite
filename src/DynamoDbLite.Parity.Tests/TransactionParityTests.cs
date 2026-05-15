using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class TransactionParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task TransactWriteItems_with_one_failing_condition_rolls_back_all(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("tx_rollback");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyStringSortKeyString(tableName), ct);

        // Seed a row that the second Put's condition will fail against.
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#1" },
                ["SK"] = new() { S = "X" },
            },
        }, ct);

        var exception = await Assert.ThrowsAsync<TransactionCanceledException>(() => client.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            TransactItems =
            [
                new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = tableName,
                        Item = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#2" },
                            ["SK"] = new() { S = "Y" },
                        },
                    },
                },
                new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = tableName,
                        Item = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "USER#1" },
                            ["SK"] = new() { S = "X" },
                        },
                        ConditionExpression = "attribute_not_exists(PK)",
                    },
                },
            ],
        }, ct));

        Assert.Equal(2, exception.CancellationReasons.Count);
        Assert.Equal("ConditionalCheckFailed", exception.CancellationReasons[1].Code);

        // First Put rolled back — (USER#2, Y) must not exist.
        var probe = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "USER#2" },
                ["SK"] = new() { S = "Y" },
            },
        }, ct);
        Assert.False(probe.IsItemSet);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task TransactWriteItems_with_multiple_failing_conditions_reports_each_index(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("tx_multi");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        // Seed two rows so two separate condition checks fail.
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "A" } },
        }, ct);
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "B" } },
        }, ct);

        var exception = await Assert.ThrowsAsync<TransactionCanceledException>(() => client.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            TransactItems =
            [
                new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = tableName,
                        Item = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "A" } },
                        ConditionExpression = "attribute_not_exists(PK)",
                    },
                },
                new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = tableName,
                        Item = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "B" } },
                        ConditionExpression = "attribute_not_exists(PK)",
                    },
                },
            ],
        }, ct));

        Assert.Equal(2, exception.CancellationReasons.Count);
        Assert.Equal("ConditionalCheckFailed", exception.CancellationReasons[0].Code);
        Assert.Equal("ConditionalCheckFailed", exception.CancellationReasons[1].Code);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task TransactWriteItems_with_repeated_ClientRequestToken_is_idempotent(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("tx_idemp");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var token = Guid.NewGuid().ToString("N");
        var request = new TransactWriteItemsRequest
        {
            ClientRequestToken = token,
            TransactItems =
            [
                new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = tableName,
                        Item = new Dictionary<string, AttributeValue>
                        {
                            ["PK"] = new() { S = "user-1" },
                            ["counter"] = new() { N = "1" },
                        },
                        ConditionExpression = "attribute_not_exists(PK)",
                    },
                },
            ],
        };

        _ = await client.TransactWriteItemsAsync(request, ct);
        // Replay the exact same request — must succeed (idempotent), not throw the conditional failure.
        _ = await client.TransactWriteItemsAsync(request, ct);

        var probe = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
        }, ct);
        Assert.Equal("1", probe.Item["counter"].N);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task TransactWriteItems_with_ReturnValuesOnConditionCheckFailure_ALL_OLD_includes_prior_item(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("tx_retold");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["label"] = new() { S = "before" },
            },
        }, ct);

        var exception = await Assert.ThrowsAsync<TransactionCanceledException>(() => client.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            TransactItems =
            [
                new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = tableName,
                        Item = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
                        ConditionExpression = "attribute_not_exists(PK)",
                        ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure.ALL_OLD,
                    },
                },
            ],
        }, ct));

        var reason = Assert.Single(exception.CancellationReasons);
        Assert.Equal("ConditionalCheckFailed", reason.Code);
        Assert.NotNull(reason.Item);
        Assert.Equal("before", reason.Item["label"].S);
    }
}
