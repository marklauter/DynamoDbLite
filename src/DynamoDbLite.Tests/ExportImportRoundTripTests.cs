using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

public sealed class ExportImportRoundTripTests
    : DynamoDbClientFixture
{
    private readonly string tempDir = Path.Combine(Path.GetTempPath(), $"dynamo_roundtrip_test_{Guid.NewGuid():N}");

    private const string SourceTable = "RoundTripSource";
    private const string SourceTableArn = "arn:aws:dynamodb:local:000000000000:table/RoundTripSource";
    private const string TargetTable = "RoundTripTarget";

    public override ValueTask DisposeAsync()
    {
        if (Directory.Exists(tempDir))
            Directory.Delete(tempDir, true);
        return base.DisposeAsync();
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task Export_And_Import_Preserves_All_Data(StoreType st)
    {
        var client = Client(st);
        var ct = TestContext.Current.CancellationToken;

        // Create source table with varied DynamoDB types
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = SourceTable,
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

        // Insert items with varied attribute types
        var items = new List<Dictionary<string, AttributeValue>>
        {
            new()
            {
                ["PK"] = new() { S = "user#1" },
                ["SK"] = new() { S = "profile" },
                ["Name"] = new() { S = "Alice" },
                ["Age"] = new() { N = "30" },
                ["Active"] = new() { BOOL = true },
                ["Tags"] = new() { SS = ["admin", "user"] }
            },
            new()
            {
                ["PK"] = new() { S = "user#2" },
                ["SK"] = new() { S = "profile" },
                ["Name"] = new() { S = "Bob" },
                ["Age"] = new() { N = "25" },
                ["Active"] = new() { BOOL = false },
                ["Scores"] = new() { NS = ["100", "200", "300"] }
            },
            new()
            {
                ["PK"] = new() { S = "user#3" },
                ["SK"] = new() { S = "settings" },
                ["NullField"] = new() { NULL = true },
                ["Nested"] = new()
                {
                    M = new Dictionary<string, AttributeValue>
                    {
                        ["Key"] = new() { S = "nested-value" }
                    }
                }
            }
        };

        foreach (var item in items)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = SourceTable,
                Item = item
            }, ct);
        }

        // Export
        var exportResponse = await client.ExportTableToPointInTimeAsync(new ExportTableToPointInTimeRequest
        {
            TableArn = SourceTableArn,
            S3Bucket = tempDir,
            ExportFormat = ExportFormat.DYNAMODB_JSON
        }, ct);

        // Wait for export to complete
        for (var i = 0; i < 50; i++)
        {
            await Task.Delay(100, ct);
            var desc = await client.DescribeExportAsync(new DescribeExportRequest
            {
                ExportArn = exportResponse.ExportDescription.ExportArn
            }, ct);
            if (desc.ExportDescription.ExportStatus != ExportStatus.IN_PROGRESS)
                break;
        }

        // Import into new table
        var importResponse = await client.ImportTableAsync(new ImportTableRequest
        {
            S3BucketSource = new S3BucketSource { S3Bucket = tempDir },
            InputFormat = InputFormat.DYNAMODB_JSON,
            TableCreationParameters = new TableCreationParameters
            {
                TableName = TargetTable,
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
            }
        }, ct);

        // Wait for import to complete
        for (var i = 0; i < 50; i++)
        {
            await Task.Delay(100, ct);
            var desc = await client.DescribeImportAsync(new DescribeImportRequest
            {
                ImportArn = importResponse.ImportTableDescription.ImportArn
            }, ct);
            if (desc.ImportTableDescription.ImportStatus != ImportStatus.IN_PROGRESS)
                break;
        }

        // Scan target table and compare
        var scanResponse = await client.ScanAsync(new ScanRequest { TableName = TargetTable }, ct);

        Assert.Equal(3, scanResponse.Count);

        // Verify user#1 item
        var user1 = scanResponse.Items.First(i => i["PK"].S == "user#1");
        Assert.Equal("Alice", user1["Name"].S);
        Assert.Equal("30", user1["Age"].N);
        Assert.True(user1["Active"].BOOL);
        Assert.Contains("admin", user1["Tags"].SS);
        Assert.Contains("user", user1["Tags"].SS);

        // Verify user#2 item
        var user2 = scanResponse.Items.First(i => i["PK"].S == "user#2");
        Assert.Equal("Bob", user2["Name"].S);
        Assert.False(user2["Active"].BOOL);
        Assert.Contains("100", user2["Scores"].NS);
        Assert.Contains("200", user2["Scores"].NS);
        Assert.Contains("300", user2["Scores"].NS);

        // Verify user#3 item with nested map and null
        var user3 = scanResponse.Items.First(i => i["PK"].S == "user#3");
        Assert.True(user3["NullField"].NULL);
        Assert.Equal("nested-value", user3["Nested"].M["Key"].S);
    }
}
