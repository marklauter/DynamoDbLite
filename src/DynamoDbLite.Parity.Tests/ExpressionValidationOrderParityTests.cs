using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

// Every API surface with an expression parameter must reject a malformed expression
// (here: a raw reserved word) BEFORE looking up or mutating any item — same contract
// as ReservedWordParityTests, but proven across the full request matrix.
[Collection("DynamoDbFixtureCollection")]
public sealed class ExpressionValidationOrderParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task DeleteItem_with_raw_reserved_word_in_ConditionExpression_throws(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("evo_del");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() => client.DeleteItemAsync(new DeleteItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "ghost" } },
            ConditionExpression = "attribute_exists(Name)",
        }, ct));

        Assert.Equal("ValidationException", ex.ErrorCode);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task Query_with_raw_reserved_word_in_FilterExpression_throws(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("evo_query");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyStringSortKeyString(tableName), ct);

        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() => client.QueryAsync(new QueryRequest
        {
            TableName = tableName,
            KeyConditionExpression = "PK = :pk",
            FilterExpression = "Name = :n",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = "ghost" },
                [":n"] = new() { S = "Alice" },
            },
        }, ct));

        Assert.Equal("ValidationException", ex.ErrorCode);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task Scan_with_raw_reserved_word_in_FilterExpression_throws(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("evo_scan");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() => client.ScanAsync(new ScanRequest
        {
            TableName = tableName,
            FilterExpression = "Name = :n",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":n"] = new() { S = "Alice" } },
        }, ct));

        Assert.Equal("ValidationException", ex.ErrorCode);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task TransactWriteItems_with_raw_reserved_word_in_ConditionExpression_throws(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("evo_tx");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() => client.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            TransactItems =
            [
                new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = tableName,
                        Item = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
                        ConditionExpression = "attribute_not_exists(Name)",
                    },
                },
            ],
        }, ct));

        Assert.Equal("ValidationException", ex.ErrorCode);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task TransactGetItems_with_raw_reserved_word_in_ProjectionExpression_throws(ParityBackend backend)
    {
        if (backend is ParityBackend.DdbLite or ParityBackend.DdbLiteFile)
            Assert.Skip("DynamoDbLite TransactGetItems does not validate reserved words in ProjectionExpression — tracked in docs/parity.md Library gaps");

        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("evo_tget");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() => client.TransactGetItemsAsync(new TransactGetItemsRequest
        {
            TransactItems =
            [
                new TransactGetItem
                {
                    Get = new Get
                    {
                        TableName = tableName,
                        Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "ghost" } },
                        ProjectionExpression = "Name",
                    },
                },
            ],
        }, ct));

        Assert.Equal("ValidationException", ex.ErrorCode);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task BatchGetItem_with_raw_reserved_word_in_ProjectionExpression_throws(ParityBackend backend)
    {
        if (backend is ParityBackend.DdbLite or ParityBackend.DdbLiteFile)
            Assert.Skip("DynamoDbLite BatchGetItem does not validate reserved words in ProjectionExpression — tracked in docs/parity.md Library gaps");

        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("evo_bget");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() => client.BatchGetItemAsync(new BatchGetItemRequest
        {
            RequestItems = new Dictionary<string, KeysAndAttributes>
            {
                [tableName] = new()
                {
                    Keys =
                    [
                        new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "ghost" } },
                    ],
                    ProjectionExpression = "Name",
                },
            },
        }, ct));

        Assert.Equal("ValidationException", ex.ErrorCode);
    }
}
