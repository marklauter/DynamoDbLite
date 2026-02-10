using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

public abstract class ExportTestsBase
    : IAsyncLifetime
{
    protected DynamoDbClient client = null!;

    protected abstract DynamoDbClient CreateClient();

    private readonly string tempDir = Path.Combine(Path.GetTempPath(), $"dynamo_export_test_{Guid.NewGuid():N}");

    private const string TableName = "ExportTable";
    private const string TableArn = "arn:aws:dynamodb:local:000000000000:table/ExportTable";

    public async ValueTask InitializeAsync()
    {
        client = CreateClient();
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = TableName,
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

        for (var i = 1; i <= 5; i++)
        {
            _ = await client.PutItemAsync(new PutItemRequest
            {
                TableName = TableName,
                Item = new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = $"pk{i}" },
                    ["SK"] = new() { S = $"sk{i}" },
                    ["Data"] = new() { S = $"value{i}" }
                }
            }, TestContext.Current.CancellationToken);
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
    public async Task Export_Returns_InProgress_Status()
    {
        var response = await client.ExportTableToPointInTimeAsync(new ExportTableToPointInTimeRequest
        {
            TableArn = TableArn,
            S3Bucket = tempDir,
            ExportFormat = ExportFormat.DYNAMODB_JSON
        }, TestContext.Current.CancellationToken);

        Assert.Equal(ExportStatus.IN_PROGRESS, response.ExportDescription.ExportStatus);
        Assert.StartsWith("arn:aws:dynamodb:local:000000000000:table/ExportTable/export/", response.ExportDescription.ExportArn);
    }

    [Fact]
    public async Task Export_Completes_And_Creates_Files_On_Disk()
    {
        var response = await client.ExportTableToPointInTimeAsync(new ExportTableToPointInTimeRequest
        {
            TableArn = TableArn,
            S3Bucket = tempDir,
            ExportFormat = ExportFormat.DYNAMODB_JSON
        }, TestContext.Current.CancellationToken);

        var exportArn = response.ExportDescription.ExportArn;

        // Poll until completed
        ExportDescription? description = null;
        for (var i = 0; i < 50; i++)
        {
            await Task.Delay(100, TestContext.Current.CancellationToken);
            var desc = await client.DescribeExportAsync(new DescribeExportRequest
            {
                ExportArn = exportArn
            }, TestContext.Current.CancellationToken);
            description = desc.ExportDescription;
            if (description.ExportStatus != ExportStatus.IN_PROGRESS)
                break;
        }

        Assert.NotNull(description);
        Assert.Equal(ExportStatus.COMPLETED, description.ExportStatus);
        Assert.Equal(5, description.ItemCount);
        Assert.True(description.BilledSizeBytes > 0);
        Assert.NotNull(description.ExportManifest);

        // Verify files exist on disk
        var exportId = exportArn.Split('/')[^1];
        var exportDir = Path.Combine(tempDir, "AWSDynamoDB", exportId);
        Assert.True(File.Exists(Path.Combine(exportDir, "manifest-summary.json")));
        Assert.True(Directory.Exists(Path.Combine(exportDir, "data")));

        var dataFiles = Directory.GetFiles(Path.Combine(exportDir, "data"), "*.json");
        Assert.NotEmpty(dataFiles);
    }

    [Fact]
    public async Task ListExports_Returns_Summaries()
    {
        _ = await client.ExportTableToPointInTimeAsync(new ExportTableToPointInTimeRequest
        {
            TableArn = TableArn,
            S3Bucket = tempDir,
            ExportFormat = ExportFormat.DYNAMODB_JSON
        }, TestContext.Current.CancellationToken);

        var response = await client.ListExportsAsync(new ListExportsRequest
        {
            TableArn = TableArn
        }, TestContext.Current.CancellationToken);

        Assert.NotEmpty(response.ExportSummaries);
        Assert.All(response.ExportSummaries, s =>
            Assert.StartsWith("arn:aws:dynamodb:local:000000000000:table/ExportTable/export/", s.ExportArn));
    }

    [Fact]
    public async Task Export_Nonexistent_Table_Throws_ResourceNotFoundException()
        => _ = await Assert.ThrowsAsync<ResourceNotFoundException>(()
            => client.ExportTableToPointInTimeAsync(new ExportTableToPointInTimeRequest
            {
                TableArn = "arn:aws:dynamodb:local:000000000000:table/NoSuchTable",
                S3Bucket = tempDir,
                ExportFormat = ExportFormat.DYNAMODB_JSON
            }, TestContext.Current.CancellationToken));

    [Fact]
    public async Task DescribeExport_Nonexistent_Throws_ResourceNotFoundException() =>
        _ = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            client.DescribeExportAsync(new DescribeExportRequest
            {
                ExportArn = "arn:aws:dynamodb:local:000000000000:table/X/export/fake"
            }, TestContext.Current.CancellationToken));
}

public sealed class InMemoryExportTests : ExportTestsBase
{
    protected override DynamoDbClient CreateClient() =>
        new(new DynamoDbLiteOptions($"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));
}

public sealed class FileBasedExportTests : ExportTestsBase
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
