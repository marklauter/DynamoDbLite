using Amazon.DynamoDBv2;
using Microsoft.Extensions.DependencyInjection;

namespace DynamoDbLite.Tests;

public sealed class ServiceCollectionExtensionsTests
{
    private const string TestConnectionString = "Data Source=Test;Mode=Memory;Cache=Shared";

    [Fact]
    public void AddDynamoDbLite_Registers_IAmazonDynamoDB()
    {
        using var provider = new ServiceCollection()
            .AddDynamoDbLite(o => o.WithConnectionString(TestConnectionString))
            .BuildServiceProvider();

        var client = provider.GetService<IAmazonDynamoDB>();

        Assert.NotNull(client);
        _ = Assert.IsType<DynamoDbClient>(client);
    }

    [Fact]
    public void AddDynamoDbLite_Registers_As_Singleton()
    {
        using var provider = new ServiceCollection()
            .AddDynamoDbLite(o => o.WithConnectionString(TestConnectionString))
            .BuildServiceProvider();

        var first = provider.GetRequiredService<IAmazonDynamoDB>();
        var second = provider.GetRequiredService<IAmazonDynamoDB>();

        Assert.Same(first, second);
    }

    [Fact]
    public void AddDynamoDbLite_Uses_Configured_ConnectionString()
    {
        using var provider = new ServiceCollection()
            .AddDynamoDbLite(o => o.WithConnectionString("Data Source=CustomTest;Mode=Memory;Cache=Shared"))
            .BuildServiceProvider();

        var client = provider.GetRequiredService<IAmazonDynamoDB>();

        _ = Assert.IsType<DynamoDbClient>(client);
    }

    [Fact]
    public void AddDynamoDbLite_Without_Configure_Throws_At_Registration()
    {
        var services = new ServiceCollection();

        // AddDynamoDbLite calls builder.Build() eagerly during registration, so a configure lambda
        // that never sets the connection string fails fast at AddDynamoDbLite() rather than later
        // at service resolution. ConnectionString has no default — an empty configure block is a misuse.
        var ex = Assert.Throws<DynamoDbLiteConfigurationException>(() => services.AddDynamoDbLite(_ => { }));
        Assert.Contains("Connection string was not configured", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddDynamoDbLite_Does_Not_Override_Existing_Registration()
    {
        using var existing = new DynamoDbClient(new DynamoDbLiteOptions(
            $"Data Source=Test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));
        var services = new ServiceCollection();
        _ = services.AddSingleton<IAmazonDynamoDB>(existing);

        _ = services.AddDynamoDbLite(o => o.WithConnectionString(TestConnectionString));

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IAmazonDynamoDB>();

        Assert.Same(existing, resolved);
    }

    [Fact]
    public void AddDynamoDbLite_Throws_When_Configure_Is_Null()
    {
        var services = new ServiceCollection();

        _ = Assert.Throws<ArgumentNullException>(() =>
            services.AddDynamoDbLite(null!));
    }

    [Fact]
    public void AddDynamoDbLite_Returns_ServiceCollection_For_Chaining()
    {
        var services = new ServiceCollection();

        var result = services.AddDynamoDbLite(o => o.WithConnectionString(TestConnectionString));

        Assert.Same(services, result);
    }

    [Fact]
    public void WithConnectionString_Returns_Builder_For_Chaining()
    {
        var builder = new DynamoDbLiteOptionsBuilder();

        var result = builder.WithConnectionString(TestConnectionString);

        Assert.Same(builder, result);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void WithConnectionString_Throws_When_Null_Or_Whitespace(string? value)
    {
        var builder = new DynamoDbLiteOptionsBuilder();

        var ex = Assert.Throws<DynamoDbLiteConfigurationException>(() => builder.WithConnectionString(value!));
        _ = Assert.IsType<ArgumentException>(ex.InnerException, exactMatch: false);
    }

    [Fact]
    public void WithConnectionString_Throws_On_Malformed_String()
    {
        var builder = new DynamoDbLiteOptionsBuilder();

        var ex = Assert.Throws<DynamoDbLiteConfigurationException>(() =>
            builder.WithConnectionString("Not=A;Valid=Sqlite=Connection=String"));
        _ = Assert.IsType<ArgumentException>(ex.InnerException, exactMatch: false);
    }
}
