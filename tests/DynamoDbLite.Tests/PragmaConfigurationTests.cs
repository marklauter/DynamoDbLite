using Amazon.DynamoDBv2;
using DynamoDbLite.Tests.Fixtures;
using Microsoft.Extensions.DependencyInjection;

namespace DynamoDbLite.Tests;

public sealed class PragmaConfigurationTests
{
    private static string MemoryConnectionString() =>
        $"Data Source=pragma_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";

    // ── Builder validation ───────────────────────────────────────────

    [Fact]
    public void WithPragma_Returns_Builder_For_Chaining()
    {
        var builder = new DynamoDbLiteOptionsBuilder();

        var result = builder.WithPragma("busy_timeout", "5000");

        Assert.Same(builder, result);
    }

    [Fact]
    public void WithConnectionInitializer_Returns_Builder_For_Chaining()
    {
        var builder = new DynamoDbLiteOptionsBuilder();

        var result = builder.WithConnectionInitializer(static _ => { });

        Assert.Same(builder, result);
    }

    [Fact]
    public void WithConnectionInitializer_Throws_When_Null()
    {
        var builder = new DynamoDbLiteOptionsBuilder();

        _ = Assert.Throws<ArgumentNullException>(() => builder.WithConnectionInitializer(null!));
    }

    [Theory]
    [InlineData("busy_timeout", "5000")]
    [InlineData("cache_size", "-16000")]
    [InlineData("synchronous", "NORMAL")]
    [InlineData("foreign_keys", "ON")]
    public void WithPragma_Accepts_Integer_And_Keyword_Values(string name, string value)
    {
        var builder = new DynamoDbLiteOptionsBuilder();

        var result = builder.WithPragma(name, value);

        Assert.Same(builder, result);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("busy timeout")]   // space
    [InlineData("busy;drop")]      // statement separator
    [InlineData("1busy")]          // leading digit
    [InlineData("busy-timeout")]   // hyphen mid-name
    public void WithPragma_Throws_On_Invalid_Name(string? name)
    {
        var builder = new DynamoDbLiteOptionsBuilder();

        _ = Assert.Throws<DynamoDbLiteConfigurationException>(() => builder.WithPragma(name!, "1"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("-")]                          // lone minus
    [InlineData("3.14")]                       // decimal point
    [InlineData("'WAL'")]                      // quotes
    [InlineData("5000; DROP TABLE items")]     // injection attempt
    public void WithPragma_Throws_On_Invalid_Value(string? value)
    {
        var builder = new DynamoDbLiteOptionsBuilder();

        _ = Assert.Throws<DynamoDbLiteConfigurationException>(() => builder.WithPragma("busy_timeout", value!));
    }

    // The DynamoDbLiteOptions record can be constructed directly, bypassing the builder's eager validation.
    // SqliteStore re-validates as the injection-safety boundary, so a malformed pragma fails at client construction.
    [Fact]
    public void Direct_Construction_With_Invalid_Pragma_Throws_At_Client_Construction()
    {
        var options = new DynamoDbLiteOptions(MemoryConnectionString())
        {
            Pragmas = [new("busy_timeout", "1; DROP TABLE items")],
        };

        _ = Assert.Throws<DynamoDbLiteConfigurationException>(() =>
        {
            using var client = new DynamoDbClient(options);
        });
    }

    // ── Behavior: pragmas + initializer applied to operational connections ───

    [Fact]
    public async Task ConnectionInitializer_Is_Invoked_On_Operational_Connection()
    {
        var invoked = false;
        var options = new DynamoDbLiteOptions(MemoryConnectionString())
        {
            ConnectionInitializer = _ => invoked = true,
        };

        using var client = new DynamoDbClient(options);
        _ = await client.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.True(invoked);
    }

    [Fact]
    public async Task Builder_Path_Applies_Pragma_And_Runs_Initializer()
    {
        var invoked = false;
        long observedBusyTimeout = -1;

        using var provider = new ServiceCollection()
            .AddDynamoDbLite(o => o
                .WithConnectionString(MemoryConnectionString())
                .WithPragma("busy_timeout", "1500")
                .WithConnectionInitializer(conn =>
                {
                    invoked = true;
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = "PRAGMA busy_timeout;";
                    observedBusyTimeout = (long)cmd.ExecuteScalar()!;
                }))
            .BuildServiceProvider();

        var client = provider.GetRequiredService<IAmazonDynamoDB>();
        _ = await client.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.True(invoked);
        Assert.Equal(1500, observedBusyTimeout);
    }

    // File-based: confirms the configured pragma takes effect AND the library's default pragmas still apply
    // (synchronous=NORMAL=1) — i.e. user pragmas append to, rather than replace, the defaults.
    [Fact]
    public async Task File_Store_Applies_User_Pragma_And_Keeps_Library_Defaults()
    {
        var path = Path.Combine(Path.GetTempPath(), $"pragma_{Guid.NewGuid():N}.db");
        long observedBusyTimeout = -1;
        long observedSynchronous = -1;

        try
        {
            var options = new DynamoDbLiteOptions($"Data Source={path}")
            {
                Pragmas = [new("busy_timeout", "3000")],
                ConnectionInitializer = conn =>
                {
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = "PRAGMA busy_timeout;";
                    observedBusyTimeout = (long)cmd.ExecuteScalar()!;
                    cmd.CommandText = "PRAGMA synchronous;";
                    observedSynchronous = (long)cmd.ExecuteScalar()!;
                },
            };

            using var client = new DynamoDbClient(options);
            _ = await client.ListTablesAsync(TestContext.Current.CancellationToken);

            Assert.Equal(3000, observedBusyTimeout);
            Assert.Equal(1, observedSynchronous); // NORMAL
        }
        finally
        {
            FileBasedTestHelper.Cleanup(path);
        }
    }
}
