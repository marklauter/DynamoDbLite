using DynamoDbLite.SqlteStores.Models;
using System.Text.Json;

namespace DynamoDbLite;

internal static class ExportHelper
{
    private const int ItemsPerFile = 10_000;

    internal static string GenerateExportArn(string tableName)
    {
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var guid = Guid.NewGuid().ToString("N")[..8];
        return $"arn:aws:dynamodb:local:000000000000:table/{tableName}/export/{timestamp}-{guid}";
    }

    internal static string GenerateImportArn(string tableName)
    {
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var guid = Guid.NewGuid().ToString("N")[..8];
        return $"arn:aws:dynamodb:local:000000000000:table/{tableName}/import/{timestamp}-{guid}";
    }

    internal static string ExtractExportId(string exportArn)
    {
        var lastSlash = exportArn.LastIndexOf('/');
        return exportArn[(lastSlash + 1)..];
    }

    internal static string GetExportDirectory(string s3Bucket, string s3Prefix, string exportId) =>
        Path.Combine(s3Bucket, s3Prefix, "AWSDynamoDB", exportId);

    internal static async Task WriteManifestAsync(
        string exportDir, string exportArn, string tableName,
        long itemCount, long billedSize, string startTime, string endTime, string format)
    {
        var manifest = new
        {
            version = "2020-06-30",
            exportArn,
            tableArn = $"arn:aws:dynamodb:local:000000000000:table/{tableName}",
            tableId = tableName,
            exportTime = startTime,
            startTime,
            endTime,
            exportFormat = format,
            billedSizeBytes = billedSize,
            itemCount,
            outputType = "DYNAMODB_JSON",
            dataFileCount = (int)Math.Ceiling((double)itemCount / ItemsPerFile)
        };

        var json = JsonSerializer.Serialize(manifest, new JsonSerializerOptions { WriteIndented = true });
        await File.WriteAllTextAsync(Path.Combine(exportDir, "manifest-summary.json"), json);
    }

    internal static async Task<(long ItemCount, long BilledSize)> WriteDataFilesAsync(
        string dataDir, List<ItemRow> items)
    {
        _ = Directory.CreateDirectory(dataDir);
        long billedSize = 0;
        var fileIndex = 0;

        for (var i = 0; i < items.Count; i += ItemsPerFile)
        {
            var chunk = items.GetRange(i, Math.Min(ItemsPerFile, items.Count - i));
            var fileName = $"{fileIndex:D8}.json";
            var lines = new List<string>(chunk.Count);

            foreach (var item in chunk)
            {
                var line = $"{{\"Item\":{item.ItemJson}}}";
                lines.Add(line);
                billedSize += line.Length;
            }

            await File.WriteAllLinesAsync(Path.Combine(dataDir, fileName), lines);
            fileIndex++;
        }

        return (items.Count, billedSize);
    }

    internal static List<string> FindDataFiles(string basePath)
    {
        var awsDir = Path.Combine(basePath, "AWSDynamoDB");
        if (!Directory.Exists(awsDir))
            return [];

        var exportDirs = Directory.GetDirectories(awsDir);
        if (exportDirs.Length == 0)
            return [];

        var allFiles = new List<string>();
        foreach (var exportDir in exportDirs)
        {
            var dataDir = Path.Combine(exportDir, "data");
            if (!Directory.Exists(dataDir))
                continue;

            allFiles.AddRange(Directory.GetFiles(dataDir, "*.json"));
        }

        allFiles.Sort(StringComparer.Ordinal);
        return allFiles;
    }
}
