using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class TableManagementParityTests(DynamoDbFixture fixture)
{
    [Theory]
    [BackendData]
    public async Task DescribeTableAsync_returns_ACTIVE_with_supplied_schema(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("desc_tbl");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyStringSortKeyString(tableName), ct);

        var response = await client.DescribeTableAsync(tableName, ct);

        Assert.Equal(tableName, response.Table.TableName);
        Assert.Equal(TableStatus.ACTIVE, response.Table.TableStatus);
        Assert.Equal(2, response.Table.KeySchema.Count);
        Assert.Equal(2, response.Table.AttributeDefinitions.Count);
        Assert.Contains(response.Table.KeySchema, k => k.AttributeName == "PK" && k.KeyType == KeyType.HASH);
        Assert.Contains(response.Table.KeySchema, k => k.AttributeName == "SK" && k.KeyType == KeyType.RANGE);
    }

    [Theory]
    [BackendData]
    public async Task ListTablesAsync_pagination_with_ExclusiveStartTableName(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);

        // Shared prefix is load-bearing: amazon/dynamodb-local accumulates tables across the
        // entire parity run, so we filter the listing to just the two we created to avoid
        // depending on global table count.
        var prefix = TestTables.UniqueName("lst_pg");
        var table1 = $"{prefix}_a";
        var table2 = $"{prefix}_b";
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(table1), ct);
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(table2), ct);

        var seen = new HashSet<string>();
        string? cursor = null;
        for (var i = 0; i < 100; i++)
        {
            var request = new ListTablesRequest { Limit = 1 };
            if (cursor is not null)
                request.ExclusiveStartTableName = cursor;

            var page = await client.ListTablesAsync(request, ct);
            foreach (var name in page.TableNames)
            {
                if (name.StartsWith(prefix, StringComparison.Ordinal))
                    _ = seen.Add(name);
            }

            if (page.LastEvaluatedTableName is null)
                break;
            cursor = page.LastEvaluatedTableName;
        }

        Assert.Contains(table1, seen);
        Assert.Contains(table2, seen);
    }

    [Theory]
    [BackendData]
    public async Task UpdateTableAsync_adds_GSI_and_index_becomes_queryable(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("upd_gsi");
        const string indexName = "GsiAddedPostCreate";

        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyStringSortKeyString(tableName), ct);

        // Pre-existing item must be backfilled into the new GSI.
        _ = await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "pk1" },
                ["SK"] = new() { S = "sk1" },
                ["GsiPK"] = new() { S = "gpk1" },
                ["GsiSK"] = new() { S = "gsk1" },
            },
        }, ct);

        _ = await client.UpdateTableAsync(new UpdateTableRequest
        {
            TableName = tableName,
            AttributeDefinitions =
            [
                new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "GsiPK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "GsiSK", AttributeType = ScalarAttributeType.S },
            ],
            GlobalSecondaryIndexUpdates =
            [
                new GlobalSecondaryIndexUpdate
                {
                    Create = new CreateGlobalSecondaryIndexAction
                    {
                        IndexName = indexName,
                        KeySchema =
                        [
                            new KeySchemaElement { AttributeName = "GsiPK", KeyType = KeyType.HASH },
                            new KeySchemaElement { AttributeName = "GsiSK", KeyType = KeyType.RANGE },
                        ],
                        Projection = new Projection { ProjectionType = ProjectionType.ALL },
                    },
                },
            ],
        }, ct);

        await TestTables.WaitForGsiActiveAsync(client, tableName, indexName, ct);

        var describe = await client.DescribeTableAsync(tableName, ct);
        Assert.NotNull(describe.Table.GlobalSecondaryIndexes);
        Assert.Contains(describe.Table.GlobalSecondaryIndexes, g => g.IndexName == indexName);

        var query = await client.QueryAsync(new QueryRequest
        {
            TableName = tableName,
            IndexName = indexName,
            KeyConditionExpression = "GsiPK = :gpk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gpk"] = new() { S = "gpk1" },
            },
        }, ct);

        _ = Assert.Single(query.Items);
        Assert.Equal("pk1", query.Items[0]["PK"].S);
    }

    [Theory]
    [BackendData]
    public async Task DeleteTableAsync_removes_from_ListTables_and_DescribeTable_throws_ResourceNotFound(ParityBackend backend)
    {
        var ct = TestContext.Current.CancellationToken;
        var client = await fixture.ClientAsync(backend, ct);
        var tableName = TestTables.UniqueName("del_tbl");
        await TestTables.CreateAndWaitAsync(client, TestTables.HashKeyString(tableName), ct);

        var listBefore = await ListAllTableNamesAsync(client, ct);
        Assert.Contains(tableName, listBefore);

        _ = await client.DeleteTableAsync(tableName, ct);

        var listAfter = await ListAllTableNamesAsync(client, ct);
        Assert.DoesNotContain(tableName, listAfter);

        _ = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            client.DescribeTableAsync(tableName, ct));
    }

    private static async Task<HashSet<string>> ListAllTableNamesAsync(Amazon.DynamoDBv2.IAmazonDynamoDB client, CancellationToken ct)
    {
        var all = new HashSet<string>();
        string? cursor = null;
        for (var i = 0; i < 100; i++)
        {
            var request = new ListTablesRequest();
            if (cursor is not null)
                request.ExclusiveStartTableName = cursor;
            var page = await client.ListTablesAsync(request, ct);
            foreach (var name in page.TableNames)
                _ = all.Add(name);
            if (page.LastEvaluatedTableName is null)
                break;
            cursor = page.LastEvaluatedTableName;
        }

        return all;
    }
}
