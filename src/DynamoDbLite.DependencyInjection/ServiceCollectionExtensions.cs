using Amazon.DynamoDBv2;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace DynamoDbLite.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDynamoDbLite(
        this IServiceCollection services,
        DynamoDbLiteOptions? options = null)
    {
        services.TryAddSingleton<IAmazonDynamoDB>(_ => new DynamoDbClient(options ?? new DynamoDbLiteOptions()));
        return services;
    }
}
