using Amazon.DynamoDBv2;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using System.Diagnostics.CodeAnalysis;

namespace DynamoDbLite;

public static class ServiceCollectionExtensions
{
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Lifetime managed by DI container")]
    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP004:Don't ignore created IDisposable", Justification = "Lifetime managed by DI container")]
    public static IServiceCollection AddDynamoDbLite(
        this IServiceCollection services,
        Action<DynamoDbLiteOptionsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new DynamoDbLiteOptionsBuilder();
        configure(builder);
        var options = builder.Build();
        services.TryAddSingleton<IAmazonDynamoDB>(sp =>
            new DynamoDbClient(options, sp.GetService<ILogger<DynamoDbClient>>()));
        return services;
    }
}
