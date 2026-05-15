using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

// Real DynamoDB and amazon/dynamodb-local reject raw reserved words in expressions with
// AmazonDynamoDBException (ErrorCode = "ValidationException"). DynamoDbLite matches.
[Collection("DynamoDbFixtureCollection")]
public sealed class ReservedWordParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task UpdateExpression_with_raw_reserved_word_throws(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("rw_update");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() => client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
            UpdateExpression = "SET Name = :n",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":n"] = new() { S = "Alice" } },
        }, ct));

        Assert.Equal("ValidationException", ex.ErrorCode);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task ConditionExpression_with_raw_reserved_word_throws(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("rw_condition");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() => client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
            ConditionExpression = "attribute_not_exists(Name)",
        }, ct));

        Assert.Equal("ValidationException", ex.ErrorCode);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task ProjectionExpression_with_raw_reserved_word_throws(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("rw_projection");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() => client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { ["PK"] = new() { S = "user-1" } },
            ProjectionExpression = "Name",
        }, ct));

        Assert.Equal("ValidationException", ex.ErrorCode);
    }

    [Theory]
    [InlineData(ParityBackend.DdbLite)]
    [InlineData(ParityBackend.DdbLiteFile)]
    [InlineData(ParityBackend.DynamoDbLocal)]
    public async Task EscapedViaExpressionAttributeName_is_accepted(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("rw_escaped");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var response = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "user-1" },
                ["Name"] = new() { S = "Alice" },
            },
            ConditionExpression = "attribute_not_exists(#n)",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#n"] = "Name" },
        }, ct);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.HttpStatusCode);
    }
}
