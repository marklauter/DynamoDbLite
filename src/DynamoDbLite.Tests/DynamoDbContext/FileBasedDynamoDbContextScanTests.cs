using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests.DynamoDbContext;

public sealed class FileBasedDynamoDbContextScanTests
    : DynamoDbContextScanTests
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
