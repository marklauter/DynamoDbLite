namespace DynamoDbLite.Tests.Fixtures;

internal sealed class FileBasedDynamoFactory
    : DynamoDbContextFactory
{
    public string DbPath { get; }

    public FileBasedDynamoFactory()
        : this(FileBasedTestHelper.NewDbPath())
    {
    }

    private FileBasedDynamoFactory(string dbPath)
        : base(() => FileBasedTestHelper.CreateFileBasedClient(dbPath)) => DbPath = dbPath;

    public override void Dispose()
    {
        base.Dispose();
        FileBasedTestHelper.Cleanup(DbPath);
    }
}
