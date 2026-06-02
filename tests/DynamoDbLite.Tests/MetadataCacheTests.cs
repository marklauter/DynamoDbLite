using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite.Tests;

// The store caches per-table metadata (key schema, attribute/index definitions, TTL attribute) and
// invalidates it on the DDL paths that change it. Each test warms the cache with a batch write, runs a
// DDL operation, then asserts the next write reflects the change — so a missed invalidation fails here.
public sealed class MetadataCacheTests
{
    private const string Table = "MetaCacheTable";

    private static DynamoDbClient NewClient()
        => new(new DynamoDbLiteOptions($"Data Source=MetaCache_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));

    private static AttributeValue S(string value) => new() { S = value };

    private static WriteRequest Put(Dictionary<string, AttributeValue> item)
        => new() { PutRequest = new PutRequest { Item = item } };

    private static async Task BatchPutAsync(DynamoDbClient client, Dictionary<string, AttributeValue> item, CancellationToken ct)
        => _ = await client.BatchWriteItemAsync(new BatchWriteItemRequest
        {
            RequestItems = new() { [Table] = [Put(item)] }
        }, ct);

    private static async Task CreateTableAsync(DynamoDbClient client, CancellationToken ct)
        => _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = Table,
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
        }, ct);

    [Fact]
    public async Task EnablingTtl_AfterMetadataCached_AppliesTtlToSubsequentWrites()
    {
        using var client = NewClient();
        var ct = TestContext.Current.CancellationToken;
        await CreateTableAsync(client, ct);

        // Warm the cache while no TTL is configured (cached TtlAttributeName is null).
        await BatchPutAsync(client, new() { ["PK"] = S("warm"), ["SK"] = S("s") }, ct);

        // Enabling TTL must invalidate the cached metadata.
        _ = await client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
        {
            TableName = Table,
            TimeToLiveSpecification = new TimeToLiveSpecification { Enabled = true, AttributeName = "ttl" }
        }, ct);

        // Write an already-expired item (ttl = epoch 1). With fresh metadata the write records ttl_epoch and
        // the item is filtered on read; with stale metadata (TtlAttributeName still null) it is returned.
        await BatchPutAsync(client, new() { ["PK"] = S("expired"), ["SK"] = S("s"), ["ttl"] = new() { N = "1" } }, ct);

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = Table,
            Key = new() { ["PK"] = S("expired"), ["SK"] = S("s") }
        }, ct);

        Assert.False(response.IsItemSet);
    }

    [Fact]
    public async Task AddingGsi_AfterMetadataCached_MaintainsIndexOnSubsequentWrites()
    {
        using var client = NewClient();
        var ct = TestContext.Current.CancellationToken;
        await CreateTableAsync(client, ct);

        // Warm the cache while the table has no secondary index.
        await BatchPutAsync(client, new() { ["PK"] = S("warm"), ["SK"] = S("s") }, ct);

        // Adding a GSI must invalidate the cached metadata.
        _ = await client.UpdateTableAsync(new UpdateTableRequest
        {
            TableName = Table,
            AttributeDefinitions =
            [
                new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "GSI_PK", AttributeType = ScalarAttributeType.S }
            ],
            GlobalSecondaryIndexUpdates =
            [
                new GlobalSecondaryIndexUpdate
                {
                    Create = new CreateGlobalSecondaryIndexAction
                    {
                        IndexName = "GSI1",
                        KeySchema = [new KeySchemaElement { AttributeName = "GSI_PK", KeyType = KeyType.HASH }],
                        Projection = new Projection { ProjectionType = ProjectionType.ALL }
                    }
                }
            ]
        }, ct);

        // Write an item carrying the GSI key. Stale metadata (no index) would skip index maintenance.
        await BatchPutAsync(client, new() { ["PK"] = S("u1"), ["SK"] = S("s"), ["GSI_PK"] = S("g1") }, ct);

        var response = await client.QueryAsync(new QueryRequest
        {
            TableName = Table,
            IndexName = "GSI1",
            KeyConditionExpression = "GSI_PK = :g",
            ExpressionAttributeValues = new() { [":g"] = S("g1") }
        }, ct);

        _ = Assert.Single(response.Items);
    }

    [Fact]
    public async Task BatchWrite_ToMissingTable_DoesNotNegativeCache()
    {
        using var client = NewClient();
        var ct = TestContext.Current.CancellationToken;

        // The table does not exist: must throw, and must not cache the "not found" result.
        _ = await Assert.ThrowsAsync<ResourceNotFoundException>(
            () => BatchPutAsync(client, new() { ["PK"] = S("p"), ["SK"] = S("s") }, ct));

        await CreateTableAsync(client, ct);

        // The same write now succeeds — the earlier null was not cached as a negative entry.
        await BatchPutAsync(client, new() { ["PK"] = S("p"), ["SK"] = S("s") }, ct);

        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = Table,
            Key = new() { ["PK"] = S("p"), ["SK"] = S("s") }
        }, ct);

        Assert.True(response.IsItemSet);
    }
}
