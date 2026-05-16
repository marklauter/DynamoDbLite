using Amazon.DynamoDBv2;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using System.Diagnostics.CodeAnalysis;

namespace DynamoDbLite;

/// <summary>
/// DI registration extensions for DynamoDbLite.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="DynamoDbClient"/> as a singleton <see cref="IAmazonDynamoDB"/>. Uses
    /// <see cref="ServiceCollectionDescriptorExtensions.TryAddSingleton{TService}(IServiceCollection, Func{IServiceProvider, TService})"/>
    /// so an existing <see cref="IAmazonDynamoDB"/> registration is not overwritten. Configuration is validated eagerly —
    /// a missing or malformed connection string throws before the service descriptor is registered.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="configure">Builder callback. Must call <see cref="DynamoDbLiteOptionsBuilder.WithConnectionString"/>.</param>
    /// <returns>The same <see cref="IServiceCollection"/>, for chaining.</returns>
    /// <exception cref="DynamoDbLiteConfigurationException">The connection string is missing or malformed.</exception>
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
