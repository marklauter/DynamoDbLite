using Microsoft.Data.Sqlite;

namespace DynamoDbLite.Tests.Fixtures;

internal static class FileBasedTestHelper
{
    internal static (DynamoDbClient Client, string DbPath) CreateFileBasedClient()
    {
        var path = Path.Combine(Path.GetTempPath(), $"dynamo_test_{Guid.NewGuid():N}.db");
        return (new DynamoDbClient(new DynamoDbLiteOptions($"Data Source={path}")), path);
    }

    internal static void Cleanup(string? dbPath)
    {
        if (dbPath is null)
            return;

        SqliteConnection.ClearAllPools();
        TryDelete(dbPath);
        TryDelete(dbPath + "-wal");
        TryDelete(dbPath + "-shm");
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // best-effort cleanup
        }
    }
}
