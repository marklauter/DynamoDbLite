using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.SqliteStores;
using DynamoDbLite.SqliteStores.Models;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace DynamoDbLite;

public sealed partial class DynamoDbClient
{
    /// <inheritdoc/>
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

        _ = ExecuteExportAsync(exportArn, tableName, s3Bucket, s3Prefix, format, startTime);

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

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Fire-and-forget background task; failures are recorded as FAILED status")]
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
        catch (ObjectDisposedException)
        {
            // Store disposed during background operation (e.g. test cleanup) — silently abandon
        }
        catch (Exception ex)
        {
            LogExportFailed(ex, exportArn);
            try
            {
                var endTime = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);
                await store.UpdateExportStatusAsync(
                    exportArn, "FAILED", endTime, null, null, null, "INTERNAL_ERROR", ex.Message);
            }
            catch (ObjectDisposedException)
            {
                // Store disposed during background operation — silently abandon
            }
            catch (Exception writeEx)
            {
                LogExportStatusWriteFailed(writeEx, exportArn, ex.Message);
            }
        }
    }

    /// <inheritdoc/>
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

    /// <inheritdoc/>
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

    private static ExportDescription ToExportDescription(ExportRow row) =>
        new()
        {
            ExportArn = row.ExportArn,
            ExportStatus = row.Status,
            TableArn = SqliteStore.TableArn(row.TableName),
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
}
