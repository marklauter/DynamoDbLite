using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

public abstract class ImportTestsBase
    : IAsyncLifetime
{
    protected DynamoDbClient client = null!;

    protected abstract DynamoDbClient CreateClient();

    private readonly string tempDir = Path.Combine(Path.GetTempPath(), $"dynamo_import_test_{Guid.NewGuid():N}");

    private const string SourceTable = "SourceTable";
    private const string SourceTableArn = "arn:aws:dynamodb:local:000000000000:table/SourceTable";
    private string exportArn = string.Empty;

    public async ValueTask InitializeAsync()
    {
        client = CreateClient();

        // Create and populate source table
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
        }, TestContext.Current.CancellationToken);

        for (var i = 1; i <= 3; i++)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = SourceTable,
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = $"pk{i}" },
                    ["SK"] = new() { S = $"sk{i}" },
                    ["Data"] = new() { S = $"value{i}" }
                }
            }, TestContext.Current.CancellationToken);
        }

        // Export source table to disk
        var exportResponse = await client.ExportTableToPointInTimeAsync(new ExportTableToPointInTimeRequest
        {
            TableArn = SourceTableArn,
            S3Bucket = tempDir,
            ExportFormat = ExportFormat.DYNAMODB_JSON
        }, TestContext.Current.CancellationToken);

        exportArn = exportResponse.ExportDescription.ExportArn;

        // Wait for export to complete
        for (var i = 0; i < 50; i++)
        {
            await Task.Delay(100, TestContext.Current.CancellationToken);
            var desc = await client.DescribeExportAsync(new DescribeExportRequest
            {
                ExportArn = exportArn
            }, TestContext.Current.CancellationToken);
            if (desc.ExportDescription.ExportStatus != ExportStatus.IN_PROGRESS)
                break;
        }
    }

    public virtual ValueTask DisposeAsync()
    {
        client.Dispose();
        if (Directory.Exists(tempDir))
            Directory.Delete(tempDir, true);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task Import_Creates_Table_And_Imports_Data()
    {
        var importResponse = await client.ImportTableAsync(new ImportTableRequest
        {
            S3BucketSource = new S3BucketSource { S3Bucket = tempDir },
            InputFormat = InputFormat.DYNAMODB_JSON,
            TableCreationParameters = new TableCreationParameters
            {
                TableName = "ImportedTable",
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
        }, TestContext.Current.CancellationToken);

        var importArn = importResponse.ImportTableDescription.ImportArn;
        Assert.Equal(ImportStatus.IN_PROGRESS, importResponse.ImportTableDescription.ImportStatus);

        // Wait for import to complete
        ImportTableDescription? description = null;
        for (var i = 0; i < 50; i++)
        {
            await Task.Delay(100, TestContext.Current.CancellationToken);
            var desc = await client.DescribeImportAsync(new DescribeImportRequest
            {
                ImportArn = importArn
            }, TestContext.Current.CancellationToken);
            description = desc.ImportTableDescription;
            if (description.ImportStatus != ImportStatus.IN_PROGRESS)
                break;
        }

        Assert.NotNull(description);
        Assert.Equal(ImportStatus.COMPLETED, description.ImportStatus);
        Assert.Equal(3, description.ImportedItemCount);

        // Verify data is actually in the new table
        var scanResponse = await client.ScanAsync(new ScanRequest { TableName = "ImportedTable" },
            TestContext.Current.CancellationToken);
        Assert.Equal(3, scanResponse.Count);
    }

    [Fact]
    public async Task Import_With_Existing_Table_Throws_ResourceInUseException() =>
        _ = await Assert.ThrowsAsync<ResourceInUseException>(() =>
            client.ImportTableAsync(new ImportTableRequest
            {
                S3BucketSource = new S3BucketSource { S3Bucket = tempDir },
                InputFormat = InputFormat.DYNAMODB_JSON,
                TableCreationParameters = new TableCreationParameters
                {
                    TableName = SourceTable, // already exists
                    KeySchema =
                    [
                        new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }
                    ],
                    AttributeDefinitions =
                    [
                        new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }
                    ]
                }
            }, TestContext.Current.CancellationToken));

    [Fact]
    public async Task ListImports_Returns_Summaries()
    {
        _ = await client.ImportTableAsync(new ImportTableRequest
        {
            S3BucketSource = new S3BucketSource { S3Bucket = tempDir },
            InputFormat = InputFormat.DYNAMODB_JSON,
            TableCreationParameters = new TableCreationParameters
            {
                TableName = "ListImportTable",
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
        }, TestContext.Current.CancellationToken);

        var response = await client.ListImportsAsync(new ListImportsRequest(),
            TestContext.Current.CancellationToken);

        Assert.NotEmpty(response.ImportSummaryList);
    }

    [Fact]
    public async Task DescribeImport_Nonexistent_Throws_ResourceNotFoundException() =>
        _ = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            client.DescribeImportAsync(new DescribeImportRequest
            {
                ImportArn = "arn:aws:dynamodb:local:000000000000:table/X/import/fake"
            }, TestContext.Current.CancellationToken));
}

public sealed class InMemoryImportTests : ImportTestsBase
{
    protected override DynamoDbClient CreateClient() =>
        new(new DynamoDbLiteOptions($"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));
}

public sealed class FileBasedImportTests : ImportTestsBase
{
    private string? dbPath;

    protected override DynamoDbClient CreateClient()
    {
        var (c, path) = FileBasedTestHelper.CreateFileBasedClient();
        dbPath = path;
        return c;
    }

    public override ValueTask DisposeAsync()
    {
        var result = base.DisposeAsync();
        FileBasedTestHelper.Cleanup(dbPath);
        return result;
    }
}
