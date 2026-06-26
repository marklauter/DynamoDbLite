using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

// Regression coverage for issue #28: harden the legacy ApplyAttributeUpdates path
// (UpdateItemRequest.AttributeUpdates, the deprecated AWS API still used by the
// DynamoDBContext Document Model layer). Each theory asserts real-DynamoDB behavior,
// so it fails against the current defect and guards against its return after the fix.
public sealed class LegacyAttributeUpdatesTests
    : DynamoDbClientFixture
{
    protected override async ValueTask SetupAsync(CancellationToken ct)
    {
        await CreateTestTableAsync(Client(StoreType.DdbLite), ct);
        await CreateTestTableAsync(Client(StoreType.DdbLiteFile), ct);
    }

    // ── Defect 1: key attributes can be silently overwritten ───────────────
    // Real DynamoDB rejects an AttributeUpdates entry that targets a key attribute
    // (PK/SK). The legacy path skips the key-validation the UpdateExpression path does,
    // so it silently rewrites the key in-place and corrupts the stored item.

    [Theory]
    [InlineData(StoreType.DdbLite, "PK")]
    [InlineData(StoreType.DdbLite, "SK")]
    [InlineData(StoreType.DdbLiteFile, "PK")]
    [InlineData(StoreType.DdbLiteFile, "SK")]
    public async Task AttributeUpdates_PutOnKeyAttribute_ThrowsException(StoreType st, string keyAttribute)
    {
        var client = Client(st);
        var ct = TestContext.Current.CancellationToken;
        _ = await PutItemAsync(client, "USER#1", "PROFILE", new Dictionary<string, AttributeValue>
        {
            ["status"] = new() { S = "active" }
        });

        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.UpdateItemAsync(new UpdateItemRequest
            {
                TableName = TestTableName,
                Key = Key("USER#1", "PROFILE"),
                AttributeUpdates = new Dictionary<string, AttributeValueUpdate>
                {
                    [keyAttribute] = new()
                    {
                        Action = AttributeAction.PUT,
                        Value = new AttributeValue { S = "MUTATED" }
                    }
                }
            }, ct));

        Assert.Contains(keyAttribute, ex.Message, StringComparison.Ordinal);

        // The reject must be atomic: the original item is left fully intact, with no
        // partial write of the rejected key or any other attribute.
        var item = await GetItemAsync(client, "USER#1", "PROFILE", ct);
        Assert.Equal("USER#1", item["PK"].S);
        Assert.Equal("PROFILE", item["SK"].S);
        Assert.Equal("active", item["status"].S);
        Assert.DoesNotContain("MUTATED", item.Values.Select(static v => v.S));
    }

    // ── Defect 2: ADD to a string/number set must enforce set uniqueness ───
    // Real DynamoDB treats SS/NS as mathematical sets; ADD unions and dedups.
    // The legacy path uses List.AddRange, leaving duplicates behind.

    [Theory]
    [InlineData(StoreType.DdbLite)]
    [InlineData(StoreType.DdbLiteFile)]
    public async Task AttributeUpdates_AddToStringSet_DeduplicatesOverlap(StoreType st)
    {
        var client = Client(st);
        var ct = TestContext.Current.CancellationToken;
        _ = await PutItemAsync(client, "USER#1", "PROFILE", new Dictionary<string, AttributeValue>
        {
            ["colors"] = new() { SS = ["red", "green"] }
        });

        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = TestTableName,
            Key = Key("USER#1", "PROFILE"),
            AttributeUpdates = new Dictionary<string, AttributeValueUpdate>
            {
                ["colors"] = new()
                {
                    Action = AttributeAction.ADD,
                    Value = new AttributeValue { SS = ["green", "blue"] }
                }
            }
        }, ct);

        var item = await GetItemAsync(client, "USER#1", "PROFILE", ct);
        Assert.Equal(["blue", "green", "red"], item["colors"].SS.OrderBy(static s => s, StringComparer.Ordinal));
    }

    [Theory]
    [InlineData(StoreType.DdbLite)]
    [InlineData(StoreType.DdbLiteFile)]
    public async Task AttributeUpdates_AddToNumberSet_DeduplicatesOverlap(StoreType st)
    {
        var client = Client(st);
        var ct = TestContext.Current.CancellationToken;
        _ = await PutItemAsync(client, "USER#1", "PROFILE", new Dictionary<string, AttributeValue>
        {
            ["scores"] = new() { NS = ["1", "2"] }
        });

        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = TestTableName,
            Key = Key("USER#1", "PROFILE"),
            AttributeUpdates = new Dictionary<string, AttributeValueUpdate>
            {
                ["scores"] = new()
                {
                    Action = AttributeAction.ADD,
                    Value = new AttributeValue { NS = ["2", "3"] }
                }
            }
        }, ct);

        var item = await GetItemAsync(client, "USER#1", "PROFILE", ct);
        Assert.Equal(["1", "2", "3"], item["scores"].NS.OrderBy(static n => n, StringComparer.Ordinal));
    }

    // ── Defect 3: ADD to a binary set is unhandled and overwrites ──────────
    // Real DynamoDB unions BS the same way it does SS/NS. The legacy path has no
    // BS case, so ADD falls through to the default and replaces the whole set with
    // only the added elements.

    [Theory]
    [InlineData(StoreType.DdbLite)]
    [InlineData(StoreType.DdbLiteFile)]
    public async Task AttributeUpdates_AddToBinarySet_UnionsValues(StoreType st)
    {
        var client = Client(st);
        var ct = TestContext.Current.CancellationToken;
        byte[] a = [0x0A], b = [0x0B], c = [0x0C];
        _ = await PutItemAsync(client, "USER#1", "PROFILE", new Dictionary<string, AttributeValue>
        {
            ["blobs"] = new() { BS = [new MemoryStream(a), new MemoryStream(b)] }
        });

        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = TestTableName,
            Key = Key("USER#1", "PROFILE"),
            AttributeUpdates = new Dictionary<string, AttributeValueUpdate>
            {
                ["blobs"] = new()
                {
                    Action = AttributeAction.ADD,
                    Value = new AttributeValue { BS = [new MemoryStream(b), new MemoryStream(c)] }
                }
            }
        }, ct);

        var item = await GetItemAsync(client, "USER#1", "PROFILE", ct);
        Assert.Equal(["0A", "0B", "0C"], HexSet(item["blobs"].BS));
    }

    // ── Defect 4: DELETE from a binary set is unhandled and overwrites ─────
    // Real DynamoDB removes the named elements from BS. The legacy path has no BS
    // DELETE case, so it falls through to the default and replaces the attribute
    // with the delete-set itself.

    [Theory]
    [InlineData(StoreType.DdbLite)]
    [InlineData(StoreType.DdbLiteFile)]
    public async Task AttributeUpdates_DeleteFromBinarySet_RemovesElements(StoreType st)
    {
        var client = Client(st);
        var ct = TestContext.Current.CancellationToken;
        byte[] a = [0x0A], b = [0x0B], c = [0x0C];
        _ = await PutItemAsync(client, "USER#1", "PROFILE", new Dictionary<string, AttributeValue>
        {
            ["blobs"] = new() { BS = [new MemoryStream(a), new MemoryStream(b), new MemoryStream(c)] }
        });

        _ = await client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = TestTableName,
            Key = Key("USER#1", "PROFILE"),
            AttributeUpdates = new Dictionary<string, AttributeValueUpdate>
            {
                ["blobs"] = new()
                {
                    Action = AttributeAction.DELETE,
                    Value = new AttributeValue { BS = [new MemoryStream(b)] }
                }
            }
        }, ct);

        var item = await GetItemAsync(client, "USER#1", "PROFILE", ct);
        Assert.Equal(["0A", "0C"], HexSet(item["blobs"].BS));
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static Dictionary<string, AttributeValue> Key(string pk, string sk) => new()
    {
        ["PK"] = new() { S = pk },
        ["SK"] = new() { S = sk }
    };

    private Task<PutItemResponse> PutItemAsync(
        DynamoDbClient client, string pk, string sk, Dictionary<string, AttributeValue> extra)
    {
        var item = Key(pk, sk);
        foreach (var (k, v) in extra)
            item[k] = v;
        return client.PutItemAsync(new PutItemRequest
        {
            TableName = TestTableName,
            Item = item
        }, TestContext.Current.CancellationToken);
    }

    private async Task<Dictionary<string, AttributeValue>> GetItemAsync(
        DynamoDbClient client, string pk, string sk, CancellationToken ct)
    {
        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = TestTableName,
            Key = Key(pk, sk)
        }, ct);
        return response.Item;
    }

    private static IEnumerable<string> HexSet(IEnumerable<MemoryStream> streams) =>
        streams.Select(static s => Convert.ToHexString(s.ToArray()))
            .OrderBy(static h => h, StringComparer.Ordinal);
}
