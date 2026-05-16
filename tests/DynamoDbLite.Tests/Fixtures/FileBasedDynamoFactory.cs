namespace DynamoDbLite.Tests.Fixtures;

internal sealed class FileBasedDynamoFactory
    : DynamoDbContextFactory
{
    public string DbPath { get; private set; } = null!;

    protected override DynamoDbClient CreateClient()
    {
        var (c, path) = FileBasedTestHelper.CreateFileBasedClient();
        DbPath = path;
        return c;
    }

    public override void Dispose()
    {
        base.Dispose();
        FileBasedTestHelper.Cleanup(DbPath);
    }
}
