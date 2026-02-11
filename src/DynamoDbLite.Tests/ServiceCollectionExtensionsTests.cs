using Amazon.DynamoDBv2;
using DynamoDbLite.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace DynamoDbLite.Tests;

public sealed class ServiceCollectionExtensionsTests
{
    private static IConfiguration BuildConfig(string connectionString = "Data Source=Test;Mode=Memory;Cache=Shared") =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                [$"{nameof(DynamoDbLiteOptions)}:ConnectionString"] = connectionString
            })
            .Build();

    [Fact]
    public void AddDynamoDbLite_Registers_IAmazonDynamoDB()
    {
        using var provider = new ServiceCollection()
            .AddDynamoDbLite(BuildConfig())
            .BuildServiceProvider();

        var client = provider.GetService<IAmazonDynamoDB>();

        Assert.NotNull(client);
        _ = Assert.IsType<DynamoDbClient>(client);
    }

    [Fact]
    public void AddDynamoDbLite_Registers_As_Singleton()
    {
        using var provider = new ServiceCollection()
            .AddDynamoDbLite(BuildConfig())
            .BuildServiceProvider();

        var first = provider.GetRequiredService<IAmazonDynamoDB>();
        var second = provider.GetRequiredService<IAmazonDynamoDB>();

        Assert.Same(first, second);
    }

    [Fact]
    public void AddDynamoDbLite_Uses_ConnectionString_From_Config()
    {
        var config = BuildConfig("Data Source=CustomTest;Mode=Memory;Cache=Shared");

        using var provider = new ServiceCollection()
            .AddDynamoDbLite(config)
            .BuildServiceProvider();

        var client = provider.GetRequiredService<IAmazonDynamoDB>();

        _ = Assert.IsType<DynamoDbClient>(client);
    }

    [Fact]
    public void AddDynamoDbLite_Does_Not_Override_Existing_Registration()
    {
        using var existing = new DynamoDbClient();
        var services = new ServiceCollection();
        _ = services.AddSingleton<IAmazonDynamoDB>(existing);

        _ = services.AddDynamoDbLite(BuildConfig());

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IAmazonDynamoDB>();

        Assert.Same(existing, resolved);
    }

    [Fact]
    public void AddDynamoDbLite_Throws_When_Section_Missing()
    {
        var config = new ConfigurationBuilder().Build();

        _ = Assert.Throws<InvalidOperationException>(() =>
            new ServiceCollection().AddDynamoDbLite(config));
    }

    [Fact]
    public void AddDynamoDbLite_Supports_Custom_SectionName()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Custom:ConnectionString"] = "Data Source=Custom;Mode=Memory;Cache=Shared"
            })
            .Build();

        using var provider = new ServiceCollection()
            .AddDynamoDbLite(config, "Custom")
            .BuildServiceProvider();

        var client = provider.GetRequiredService<IAmazonDynamoDB>();

        _ = Assert.IsType<DynamoDbClient>(client);
    }

    [Fact]
    public void AddDynamoDbLite_Returns_ServiceCollection_For_Chaining()
    {
        var services = new ServiceCollection();

        var result = services.AddDynamoDbLite(BuildConfig());

        Assert.Same(services, result);
    }
}
