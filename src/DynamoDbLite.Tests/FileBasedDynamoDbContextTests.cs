using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

public sealed class FileBasedDynamoDbContextTests
    : DynamoDbContextTests
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
