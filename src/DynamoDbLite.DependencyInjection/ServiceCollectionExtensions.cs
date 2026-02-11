using Amazon.DynamoDBv2;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace DynamoDbLite.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDynamoDbLite(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName = nameof(DynamoDbLiteOptions))
    {
        services.TryAddSingleton<IAmazonDynamoDB>(
            new DynamoDbClient(
                configuration
                    .GetRequiredSection(sectionName)
                    .Get<DynamoDbLiteOptions>()
                    ?? new DynamoDbLiteOptions()));
        return services;
    }

    public static IServiceCollection AddDynamoDbLite(
        this IServiceCollection services,
        DynamoDbLiteOptions options)
    {
        services.TryAddSingleton<IAmazonDynamoDB>(new DynamoDbClient(options));
        return services;
    }
}
