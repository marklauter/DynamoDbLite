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

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        try
        {
            if (Directory.Exists(tempDir))
                Directory.Delete(tempDir, true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Best-effort: background export/import task may still hold file handles
        }
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
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
        ExportStatus? exportFinalStatus = null;
        for (var i = 0; i < 100; i++)
        {
            await Task.Delay(100, ct);
            var desc = await client.DescribeExportAsync(new DescribeExportRequest
            {
                ExportArn = exportResponse.ExportDescription.ExportArn
            }, ct);
            if (desc.ExportDescription.ExportStatus != ExportStatus.IN_PROGRESS)
            {
                exportFinalStatus = desc.ExportDescription.ExportStatus;
                break;
            }
        }

        Assert.NotNull(exportFinalStatus);

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
        ImportStatus? importFinalStatus = null;
        for (var i = 0; i < 100; i++)
        {
            await Task.Delay(100, ct);
            var desc = await client.DescribeImportAsync(new DescribeImportRequest
            {
                ImportArn = importResponse.ImportTableDescription.ImportArn
            }, ct);
            if (desc.ImportTableDescription.ImportStatus != ImportStatus.IN_PROGRESS)
            {
                importFinalStatus = desc.ImportTableDescription.ImportStatus;
                break;
            }
        }

        Assert.NotNull(importFinalStatus);

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

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Export_And_Import_Empty_Table_Completes_With_Zero_Items(StoreType st)
    {
        var client = Client(st);
        var ct = TestContext.Current.CancellationToken;
        const string sourceTable = "EmptyRoundTripSource";
        const string sourceArn = "arn:aws:dynamodb:local:000000000000:table/EmptyRoundTripSource";
        const string targetTable = "EmptyRoundTripTarget";

        // Create the source table but leave it EMPTY — no PutItem calls.
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = sourceTable,
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

        // Export the empty table — this produces a valid AWSDynamoDB export with zero data rows.
        var exportResponse = await client.ExportTableToPointInTimeAsync(new ExportTableToPointInTimeRequest
        {
            TableArn = sourceArn,
            S3Bucket = tempDir,
            ExportFormat = ExportFormat.DYNAMODB_JSON
        }, ct);

        ExportStatus? exportFinalStatus = null;
        for (var i = 0; i < 100; i++)
        {
            await Task.Delay(100, ct);
            var desc = await client.DescribeExportAsync(new DescribeExportRequest
            {
                ExportArn = exportResponse.ExportDescription.ExportArn
            }, ct);
            if (desc.ExportDescription.ExportStatus != ExportStatus.IN_PROGRESS)
            {
                exportFinalStatus = desc.ExportDescription.ExportStatus;
                break;
            }
        }

        Assert.Equal(ExportStatus.COMPLETED, exportFinalStatus);

        // Import from that same valid (but empty) export location.
        var importResponse = await client.ImportTableAsync(new ImportTableRequest
        {
            S3BucketSource = new S3BucketSource { S3Bucket = tempDir },
            InputFormat = InputFormat.DYNAMODB_JSON,
            TableCreationParameters = new TableCreationParameters
            {
                TableName = targetTable,
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

        ImportTableDescription? importDescription = null;
        for (var i = 0; i < 100; i++)
        {
            await Task.Delay(100, ct);
            var desc = await client.DescribeImportAsync(new DescribeImportRequest
            {
                ImportArn = importResponse.ImportTableDescription.ImportArn
            }, ct);
            importDescription = desc.ImportTableDescription;
            if (importDescription.ImportStatus != ImportStatus.IN_PROGRESS)
                break;
        }

        Assert.NotNull(importDescription);

        // A valid-but-empty export is not a misconfiguration. Real DynamoDB completes such an
        // import successfully with zero imported items — it does NOT fail it.
        Assert.Equal(ImportStatus.COMPLETED, importDescription.ImportStatus);
        Assert.Equal(0, importDescription.ImportedItemCount);

        // The imported table exists and is empty.
        var scanResponse = await client.ScanAsync(new ScanRequest { TableName = targetTable }, ct);
        Assert.Equal(0, scanResponse.Count);
    }

    [Theory]
    [InlineData(StoreType.DdbLiteFile)]
    [InlineData(StoreType.DdbLite)]
    public async Task Export_And_Import_With_GSI_PreservesIndex(StoreType st)
    {
        var client = Client(st);
        var ct = TestContext.Current.CancellationToken;
        const string sourceTable = "GsiRoundTripSource";
        const string sourceArn = "arn:aws:dynamodb:local:000000000000:table/GsiRoundTripSource";
        const string targetTable = "GsiRoundTripTarget";
        const string indexName = "ByKindVersion";

        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = sourceTable,
            KeySchema =
            [
                new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "kind", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "version", AttributeType = ScalarAttributeType.S }
            ],
            GlobalSecondaryIndexes =
            [
                new GlobalSecondaryIndex
                {
                    IndexName = indexName,
                    KeySchema =
                    [
                        new KeySchemaElement { AttributeName = "kind", KeyType = KeyType.HASH },
                        new KeySchemaElement { AttributeName = "version", KeyType = KeyType.RANGE }
                    ],
                    Projection = new Projection { ProjectionType = ProjectionType.ALL }
                }
            ]
        }, ct);

        for (var i = 1; i <= 3; i++)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = sourceTable,
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = $"item#{i}" },
                    ["SK"] = new() { S = "data" },
                    ["kind"] = new() { S = i % 2 == 0 ? "even" : "odd" },
                    ["version"] = new() { S = $"v{i}" }
                }
            }, ct);
        }

        var exportResponse = await client.ExportTableToPointInTimeAsync(new ExportTableToPointInTimeRequest
        {
            TableArn = sourceArn,
            S3Bucket = tempDir,
            ExportFormat = ExportFormat.DYNAMODB_JSON
        }, ct);

        ExportStatus? exportFinalStatus = null;
        for (var i = 0; i < 100; i++)
        {
            await Task.Delay(100, ct);
            var desc = await client.DescribeExportAsync(new DescribeExportRequest
            {
                ExportArn = exportResponse.ExportDescription.ExportArn
            }, ct);
            if (desc.ExportDescription.ExportStatus != ExportStatus.IN_PROGRESS)
            {
                exportFinalStatus = desc.ExportDescription.ExportStatus;
                break;
            }
        }

        Assert.NotNull(exportFinalStatus);

        var importResponse = await client.ImportTableAsync(new ImportTableRequest
        {
            S3BucketSource = new S3BucketSource { S3Bucket = tempDir },
            InputFormat = InputFormat.DYNAMODB_JSON,
            TableCreationParameters = new TableCreationParameters
            {
                TableName = targetTable,
                KeySchema =
                [
                    new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                    new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }
                ],
                AttributeDefinitions =
                [
                    new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                    new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S },
                    new AttributeDefinition { AttributeName = "kind", AttributeType = ScalarAttributeType.S },
                    new AttributeDefinition { AttributeName = "version", AttributeType = ScalarAttributeType.S }
                ],
                GlobalSecondaryIndexes =
                [
                    new GlobalSecondaryIndex
                    {
                        IndexName = indexName,
                        KeySchema =
                        [
                            new KeySchemaElement { AttributeName = "kind", KeyType = KeyType.HASH },
                            new KeySchemaElement { AttributeName = "version", KeyType = KeyType.RANGE }
                        ],
                        Projection = new Projection { ProjectionType = ProjectionType.ALL }
                    }
                ]
            }
        }, ct);

        ImportStatus? importFinalStatus = null;
        for (var i = 0; i < 100; i++)
        {
            await Task.Delay(100, ct);
            var desc = await client.DescribeImportAsync(new DescribeImportRequest
            {
                ImportArn = importResponse.ImportTableDescription.ImportArn
            }, ct);
            if (desc.ImportTableDescription.ImportStatus != ImportStatus.IN_PROGRESS)
            {
                importFinalStatus = desc.ImportTableDescription.ImportStatus;
                break;
            }
        }

        Assert.NotNull(importFinalStatus);

        // Query the imported GSI to confirm both data and index were created
        var queryResponse = await client.QueryAsync(new QueryRequest
        {
            TableName = targetTable,
            IndexName = indexName,
            KeyConditionExpression = "#k = :k",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#k"] = "kind" },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":k"] = new() { S = "odd" } }
        }, ct);

        Assert.Equal(2, queryResponse.Count);
        Assert.All(queryResponse.Items, i => Assert.Equal("odd", i["kind"].S));
    }
}
