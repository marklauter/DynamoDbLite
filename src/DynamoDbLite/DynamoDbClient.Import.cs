using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Serialization;
using DynamoDbLite.SqliteStores;
using DynamoDbLite.SqliteStores.Models;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text.Json;

namespace DynamoDbLite;

public sealed partial class DynamoDbClient
{
    /// <inheritdoc/>
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

        var tableCreationJson = request.TableCreationParameters.ToJson();

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
        }, cancellationToken);

        _ = ExecuteImportAsync(importArn, tableName, s3Bucket, s3KeyPrefix);

        return new ImportTableResponse
        {
            ImportTableDescription = new ImportTableDescription
            {
                ImportArn = importArn,
                ImportStatus = ImportStatus.IN_PROGRESS,
                TableArn = SqliteStore.TableArn(tableName),
                S3BucketSource = request.S3BucketSource,
                InputFormat = InputFormat.DYNAMODB_JSON,
                TableCreationParameters = request.TableCreationParameters,
                StartTime = DateTime.Parse(startTime, CultureInfo.InvariantCulture)
            },
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Fire-and-forget background task; failures are recorded as FAILED status")]
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
        catch (ObjectDisposedException)
        {
            // Store disposed during background operation (e.g. test cleanup) — silently abandon
        }
        catch (Exception ex)
        {
            LogImportFailed(ex, importArn);
            try
            {
                var endTime = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);
                await store.UpdateImportStatusAsync(
                    importArn, "FAILED", endTime, null, null, null, null, "INTERNAL_ERROR", ex.Message);
            }
            catch (ObjectDisposedException)
            {
                // Store disposed during background operation — silently abandon
            }
            catch (Exception writeEx)
            {
                LogImportStatusWriteFailed(writeEx, importArn, ex.Message);
            }
        }
    }

    /// <inheritdoc/>
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

    /// <inheritdoc/>
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
            TableArn = SqliteStore.TableArn(r.TableName),
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

    private static ImportTableDescription ToImportDescription(ImportRow row)
    {
        var tableCreation = DeserializeTableCreationParameters(row.TableCreationJson);
        return new ImportTableDescription
        {
            ImportArn = row.ImportArn,
            ImportStatus = row.Status,
            TableArn = SqliteStore.TableArn(row.TableName),
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

    private static TableCreationParameters DeserializeTableCreationParameters(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        using var ksArr = root.GetProperty("KeySchema").EnumerateArray();
        List<KeySchemaElement> keySchema =
        [
            .. ksArr.Select(k => new KeySchemaElement
            {
                AttributeName = k.GetProperty("AttributeName").GetString()!,
                KeyType = k.GetProperty("KeyType").GetString()!
            })
        ];

        using var adArr = root.GetProperty("AttributeDefinitions").EnumerateArray();
        List<AttributeDefinition> attrDefs =
        [
            .. adArr.Select(a => new AttributeDefinition
            {
                AttributeName = a.GetProperty("AttributeName").GetString()!,
                AttributeType = a.GetProperty("AttributeType").GetString()!
            })
        ];

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

    private static Amazon.DynamoDBv2.Model.ProvisionedThroughput DeserializeProvisionedThroughput(JsonElement element) =>
        new()
        {
            ReadCapacityUnits = element.GetProperty("ReadCapacityUnits").GetInt64(),
            WriteCapacityUnits = element.GetProperty("WriteCapacityUnits").GetInt64()
        };

    private static List<GlobalSecondaryIndex> DeserializeGlobalSecondaryIndexes(JsonElement array)
    {
        using var arr = array.EnumerateArray();
        return
        [
            .. arr.Select(g =>
            {
                using var ks = g.GetProperty("KeySchema").EnumerateArray();
                return new GlobalSecondaryIndex
                {
                    IndexName = g.GetProperty("IndexName").GetString()!,
                    KeySchema =
                    [
                        .. ks.Select(gk => new KeySchemaElement
                        {
                            AttributeName = gk.GetProperty("AttributeName").GetString()!,
                            KeyType = gk.GetProperty("KeyType").GetString()!
                        })
                    ],
                    Projection = DeserializeProjection(g)
                };
            })
        ];
    }

    private static Amazon.DynamoDBv2.Model.Projection DeserializeProjection(JsonElement gsiElement)
    {
        if (!gsiElement.TryGetProperty("Projection", out var proj) || proj.ValueKind == JsonValueKind.Null)
            return new Amazon.DynamoDBv2.Model.Projection { ProjectionType = ProjectionType.ALL };

        var projection = new Amazon.DynamoDBv2.Model.Projection();
        if (proj.TryGetProperty("ProjectionType", out var projType) && projType.ValueKind != JsonValueKind.Null)
            projection.ProjectionType = projType.GetString()!;
        if (proj.TryGetProperty("NonKeyAttributes", out var nka) && nka.ValueKind == JsonValueKind.Array)
        {
            using var nkaArr = nka.EnumerateArray();
            projection.NonKeyAttributes = [.. nkaArr.Select(a => a.GetString()!)];
        }

        return projection;
    }
}
