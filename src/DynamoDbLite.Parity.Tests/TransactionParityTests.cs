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
}
