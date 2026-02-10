using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using System.Globalization;
using System.Text.Json;

namespace DynamoDbLite;

// ── Export & Import ──────────────────────────────────────────────────
public sealed partial class DynamoDbClient
{
    public async Task<ExportTableToPointInTimeResponse> ExportTableToPointInTimeAsync(
        ExportTableToPointInTimeRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableArn);

        var tableName = SqliteStore.ExtractTableNameFromArn(request.TableArn);

        if (!await store.TableExistsAsync(tableName, cancellationToken))
            throw new ResourceNotFoundException($"Requested resource not found: Table: {tableName} not found");

        if (request.ExportFormat is not null
            && request.ExportFormat != ExportFormat.DYNAMODB_JSON)
            throw new AmazonDynamoDBException("Only DYNAMODB_JSON export format is supported");

        var s3Bucket = request.S3Bucket ?? throw new AmazonDynamoDBException("S3Bucket is required");
        var s3Prefix = request.S3Prefix ?? string.Empty;
        var format = "DYNAMODB_JSON";
        var exportArn = ExportHelper.GenerateExportArn(tableName);
        var startTime = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);

        await store.CreateExportRecordAsync(
            exportArn, tableName, format, s3Bucket, s3Prefix, startTime, request.ClientToken, cancellationToken);

        _ = Task.Run(() => ExecuteExportAsync(exportArn, tableName, s3Bucket, s3Prefix, format, startTime));

        return new ExportTableToPointInTimeResponse
        {
            ExportDescription = new ExportDescription
            {
                ExportArn = exportArn,
                ExportStatus = ExportStatus.IN_PROGRESS,
                TableArn = request.TableArn,
                ExportFormat = ExportFormat.DYNAMODB_JSON,
                S3Bucket = s3Bucket,
                S3Prefix = s3Prefix,
                StartTime = DateTime.Parse(startTime, CultureInfo.InvariantCulture)
            },
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }

    private async Task ExecuteExportAsync(
        string exportArn, string tableName, string s3Bucket, string s3Prefix, string format, string startTime)
    {
        try
        {
            var items = await store.GetAllItemsAsync(tableName);
            var exportId = ExportHelper.ExtractExportId(exportArn);
            var exportDir = ExportHelper.GetExportDirectory(s3Bucket, s3Prefix, exportId);
            var dataDir = Path.Combine(exportDir, "data");

            _ = Directory.CreateDirectory(exportDir);

            var (itemCount, billedSize) = await ExportHelper.WriteDataFilesAsync(dataDir, items);
            var endTime = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);

            await ExportHelper.WriteManifestAsync(
                exportDir, exportArn, tableName, itemCount, billedSize, startTime, endTime, format);

            var manifestPath = Path.Combine(exportDir, "manifest-summary.json");
            await store.UpdateExportStatusAsync(
                exportArn, "COMPLETED", endTime, manifestPath, itemCount, billedSize, null, null);
        }
        catch (Exception ex)
        {
            var endTime = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);
            await store.UpdateExportStatusAsync(
                exportArn, "FAILED", endTime, null, null, null, "INTERNAL_ERROR", ex.Message);
        }
    }

    public async Task<DescribeExportResponse> DescribeExportAsync(
        DescribeExportRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.ExportArn);

        var row = await store.GetExportRecordAsync(request.ExportArn, cancellationToken)
            ?? throw new ResourceNotFoundException($"Export not found: {request.ExportArn}");

        return new DescribeExportResponse
        {
            ExportDescription = ToExportDescription(row),
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }

    public async Task<ListExportsResponse> ListExportsAsync(
        ListExportsRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);

        var rows = await store.ListExportRecordsAsync(
            request.TableArn, request.MaxResults, request.NextToken, cancellationToken);

        var summaries = rows.Select(static r => new ExportSummary
        {
            ExportArn = r.ExportArn,
            ExportStatus = r.Status
        }).ToList();

        string? nextToken = null;
        if (request.MaxResults is not null && rows.Count == request.MaxResults)
            nextToken = rows[^1].ExportArn;

        return new ListExportsResponse
        {
            ExportSummaries = summaries,
            NextToken = nextToken,
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }

    public async Task<ImportTableResponse> ImportTableAsync(
        ImportTableRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.S3BucketSource);
        ArgumentNullException.ThrowIfNull(request.TableCreationParameters);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableCreationParameters.TableName);

        var tableName = request.TableCreationParameters.TableName;

        if (await store.TableExistsAsync(tableName, cancellationToken))
            throw new ResourceInUseException($"Table already exists: {tableName}");

        if (request.InputFormat is not null
            && request.InputFormat != InputFormat.DYNAMODB_JSON)
            throw new AmazonDynamoDBException("Only DYNAMODB_JSON import format is supported");

        var s3Bucket = request.S3BucketSource.S3Bucket
            ?? throw new AmazonDynamoDBException("S3BucketSource.S3Bucket is required");
        var s3KeyPrefix = request.S3BucketSource.S3KeyPrefix ?? string.Empty;
        var format = "DYNAMODB_JSON";
        var compression = request.InputCompressionType?.Value ?? "NONE";
        var importArn = ExportHelper.GenerateImportArn(tableName);
        var startTime = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);

        var tableCreationJson = SerializeTableCreationParameters(request.TableCreationParameters);

        await store.CreateImportRecordAsync(
            importArn, tableName, format, compression, s3Bucket, s3KeyPrefix,
            tableCreationJson, startTime, request.ClientToken, cancellationToken);

        // Create the table synchronously before handing off to the background task
        // to avoid a race where another caller creates the same table between check and creation.
        var tableParams = request.TableCreationParameters;
        _ = await CreateTableAsync(new CreateTableRequest
        {
            TableName = tableParams.TableName,
            KeySchema = tableParams.KeySchema,
            AttributeDefinitions = tableParams.AttributeDefinitions,
            ProvisionedThroughput = tableParams.ProvisionedThroughput,
            GlobalSecondaryIndexes = tableParams.GlobalSecondaryIndexes,
        });

        _ = Task.Run(() => ExecuteImportAsync(
            importArn, tableName, s3Bucket, s3KeyPrefix));

        return new ImportTableResponse
        {
            ImportTableDescription = new ImportTableDescription
            {
                ImportArn = importArn,
                ImportStatus = ImportStatus.IN_PROGRESS,
                TableArn = $"arn:aws:dynamodb:local:000000000000:table/{tableName}",
                S3BucketSource = request.S3BucketSource,
                InputFormat = InputFormat.DYNAMODB_JSON,
                TableCreationParameters = request.TableCreationParameters,
                StartTime = DateTime.Parse(startTime, CultureInfo.InvariantCulture)
            },
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }

    private async Task ExecuteImportAsync(
        string importArn, string tableName,
        string s3Bucket, string s3KeyPrefix)
    {
        try
        {
            var basePath = string.IsNullOrEmpty(s3KeyPrefix)
                ? s3Bucket
                : Path.Combine(s3Bucket, s3KeyPrefix);
            var dataFiles = ExportHelper.FindDataFiles(basePath);

            long importedCount = 0;
            long processedCount = 0;
            long processedBytes = 0;

            foreach (var filePath in dataFiles)
            {
                var lines = await File.ReadAllLinesAsync(filePath);
                foreach (var line in lines)
                {
                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    processedCount++;
                    processedBytes += line.Length;

                    // Each line is {"Item":{...dynamodb json...}}
                    using var doc = JsonDocument.Parse(line);
                    var itemElement = doc.RootElement.GetProperty("Item");
                    var itemJson = itemElement.GetRawText();
                    var item = AttributeValueSerializer.Deserialize(itemJson);

                    _ = await PutItemAsync(new PutItemRequest
                    {
                        TableName = tableName,
                        Item = item
                    });

                    importedCount++;
                }
            }

            var endTime = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);
            await store.UpdateImportStatusAsync(
                importArn, "COMPLETED", endTime,
                importedCount, processedCount, processedBytes, 0L, null, null);
        }
        catch (Exception ex)
        {
            var endTime = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);
            await store.UpdateImportStatusAsync(
                importArn, "FAILED", endTime, null, null, null, null, "INTERNAL_ERROR", ex.Message);
        }
    }

    public async Task<DescribeImportResponse> DescribeImportAsync(
        DescribeImportRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.ImportArn);

        var row = await store.GetImportRecordAsync(request.ImportArn, cancellationToken)
            ?? throw new ResourceNotFoundException($"Import not found: {request.ImportArn}");

        return new DescribeImportResponse
        {
            ImportTableDescription = ToImportDescription(row),
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }

    public async Task<ListImportsResponse> ListImportsAsync(
        ListImportsRequest request,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);

        var rows = await store.ListImportRecordsAsync(
            request.TableArn, request.PageSize, request.NextToken, cancellationToken);

        var summaries = rows.Select(static r => new ImportSummary
        {
            ImportArn = r.ImportArn,
            ImportStatus = r.Status,
            TableArn = $"arn:aws:dynamodb:local:000000000000:table/{r.TableName}",
            S3BucketSource = new S3BucketSource { S3Bucket = r.S3Bucket, S3KeyPrefix = r.S3KeyPrefix },
            InputFormat = r.InputFormat,
            StartTime = DateTime.Parse(r.StartTime, CultureInfo.InvariantCulture),
            EndTime = r.EndTime is not null ? DateTime.Parse(r.EndTime, CultureInfo.InvariantCulture) : null
        }).ToList();

        string? nextToken = null;
        if (request.PageSize is not null && rows.Count == request.PageSize)
            nextToken = rows[^1].ImportArn;

        return new ListImportsResponse
        {
            ImportSummaryList = summaries,
            NextToken = nextToken,
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }

    private static ExportDescription ToExportDescription(ExportRow row) =>
        new()
        {
            ExportArn = row.ExportArn,
            ExportStatus = row.Status,
            TableArn = $"arn:aws:dynamodb:local:000000000000:table/{row.TableName}",
            ExportFormat = row.ExportFormat,
            S3Bucket = row.S3Bucket,
            S3Prefix = row.S3Prefix,
            ExportManifest = row.ExportManifest,
            ItemCount = row.ItemCount ?? 0,
            BilledSizeBytes = row.BilledSize ?? 0,
            StartTime = DateTime.Parse(row.StartTime, CultureInfo.InvariantCulture),
            EndTime = row.EndTime is not null ? DateTime.Parse(row.EndTime, CultureInfo.InvariantCulture) : null,
            FailureCode = row.FailureCode,
            FailureMessage = row.FailureMessage,
            ClientToken = row.ClientToken
        };

    private static ImportTableDescription ToImportDescription(ImportRow row)
    {
        var tableCreation = DeserializeTableCreationParameters(row.TableCreationJson);
        return new ImportTableDescription
        {
            ImportArn = row.ImportArn,
            ImportStatus = row.Status,
            TableArn = $"arn:aws:dynamodb:local:000000000000:table/{row.TableName}",
            S3BucketSource = new S3BucketSource { S3Bucket = row.S3Bucket, S3KeyPrefix = row.S3KeyPrefix },
            InputFormat = row.InputFormat,
            InputCompressionType = row.InputCompression,
            TableCreationParameters = tableCreation,
            ImportedItemCount = row.ImportedCount ?? 0,
            ProcessedItemCount = row.ProcessedCount ?? 0,
            ProcessedSizeBytes = row.ProcessedBytes ?? 0,
            ErrorCount = row.ErrorCount ?? 0,
            StartTime = DateTime.Parse(row.StartTime, CultureInfo.InvariantCulture),
            EndTime = row.EndTime is not null ? DateTime.Parse(row.EndTime, CultureInfo.InvariantCulture) : null,
            FailureCode = row.FailureCode,
            FailureMessage = row.FailureMessage,
            ClientToken = row.ClientToken
        };
    }

    private static string SerializeTableCreationParameters(TableCreationParameters p) =>
        JsonSerializer.Serialize(new
        {
            p.TableName,
            KeySchema = p.KeySchema.Select(static k => new { k.AttributeName, KeyType = k.KeyType.Value }).ToList(),
            AttributeDefinitions = p.AttributeDefinitions.Select(static a => new { a.AttributeName, AttributeType = a.AttributeType.Value }).ToList(),
            ProvisionedThroughput = p.ProvisionedThroughput is not null
                ? new { p.ProvisionedThroughput.ReadCapacityUnits, p.ProvisionedThroughput.WriteCapacityUnits }
                : null,
            GlobalSecondaryIndexes = p.GlobalSecondaryIndexes?.Select(static g => new
            {
                g.IndexName,
                KeySchema = g.KeySchema.Select(static k => new { k.AttributeName, KeyType = k.KeyType.Value }).ToList(),
                Projection = g.Projection is not null ? new { ProjectionType = g.Projection.ProjectionType?.Value, g.Projection.NonKeyAttributes } : null
            }).ToList()
        });

    [System.Diagnostics.CodeAnalysis.SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP004:Don't ignore created IDisposable", Justification = "ArrayEnumerator is a struct; foreach disposes it")]
    private static TableCreationParameters DeserializeTableCreationParameters(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        var keySchema = new List<KeySchemaElement>();
        foreach (var k in root.GetProperty("KeySchema").EnumerateArray())
        {
            keySchema.Add(new KeySchemaElement
            {
                AttributeName = k.GetProperty("AttributeName").GetString()!,
                KeyType = k.GetProperty("KeyType").GetString()!
            });
        }

        var attrDefs = new List<AttributeDefinition>();
        foreach (var a in root.GetProperty("AttributeDefinitions").EnumerateArray())
        {
            attrDefs.Add(new AttributeDefinition
            {
                AttributeName = a.GetProperty("AttributeName").GetString()!,
                AttributeType = a.GetProperty("AttributeType").GetString()!
            });
        }

        var throughput = root.TryGetProperty("ProvisionedThroughput", out var pt) && pt.ValueKind != JsonValueKind.Null
            ? DeserializeProvisionedThroughput(pt)
            : null;

        var gsis = root.TryGetProperty("GlobalSecondaryIndexes", out var gsiArray) && gsiArray.ValueKind == JsonValueKind.Array
            ? DeserializeGlobalSecondaryIndexes(gsiArray)
            : null;

        return new TableCreationParameters
        {
            TableName = root.GetProperty("TableName").GetString()!,
            KeySchema = keySchema,
            AttributeDefinitions = attrDefs,
            ProvisionedThroughput = throughput,
            GlobalSecondaryIndexes = gsis
        };
    }

    private static ProvisionedThroughput DeserializeProvisionedThroughput(JsonElement element) =>
        new()
        {
            ReadCapacityUnits = element.GetProperty("ReadCapacityUnits").GetInt64(),
            WriteCapacityUnits = element.GetProperty("WriteCapacityUnits").GetInt64()
        };

    [System.Diagnostics.CodeAnalysis.SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP004:Don't ignore created IDisposable", Justification = "ArrayEnumerator is a struct; foreach disposes it")]
    private static List<GlobalSecondaryIndex> DeserializeGlobalSecondaryIndexes(JsonElement array)
    {
        var gsis = new List<GlobalSecondaryIndex>();
        foreach (var g in array.EnumerateArray())
        {
            var gsiKeySchema = new List<KeySchemaElement>();
            foreach (var gk in g.GetProperty("KeySchema").EnumerateArray())
            {
                gsiKeySchema.Add(new KeySchemaElement
                {
                    AttributeName = gk.GetProperty("AttributeName").GetString()!,
                    KeyType = gk.GetProperty("KeyType").GetString()!
                });
            }

            var projection = DeserializeProjection(g);

            gsis.Add(new GlobalSecondaryIndex
            {
                IndexName = g.GetProperty("IndexName").GetString()!,
                KeySchema = gsiKeySchema,
                Projection = projection
            });
        }

        return gsis;
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP004:Don't ignore created IDisposable", Justification = "ArrayEnumerator is a struct; foreach disposes it")]
    private static Projection DeserializeProjection(JsonElement gsiElement)
    {
        if (!gsiElement.TryGetProperty("Projection", out var proj) || proj.ValueKind == JsonValueKind.Null)
            return new Projection { ProjectionType = ProjectionType.ALL };

        var projection = new Projection();
        if (proj.TryGetProperty("ProjectionType", out var projType) && projType.ValueKind != JsonValueKind.Null)
            projection.ProjectionType = projType.GetString()!;
        if (proj.TryGetProperty("NonKeyAttributes", out var nka) && nka.ValueKind == JsonValueKind.Array)
        {
            projection.NonKeyAttributes = [];
            foreach (var attr in nka.EnumerateArray())
                projection.NonKeyAttributes.Add(attr.GetString()!);
        }

        return projection;
    }
}
