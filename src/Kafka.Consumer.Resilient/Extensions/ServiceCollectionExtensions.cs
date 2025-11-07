using Ces.Kafka.Consumer.Resilient.Configuration;
using Ces.Kafka.Consumer.Resilient.Interfaces;
using Ces.Kafka.Consumer.Resilient.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Ces.Kafka.Consumer.Resilient.Extensions;

/// <summary>
/// Extension methods for IServiceCollection to register resilient Kafka consumer services
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds resilient Kafka consumer services to the service collection
    /// </summary>
    /// <typeparam name="TMessage">Type of message to consume</typeparam>
    /// <typeparam name="THandler">Implementation of IMessageHandler</typeparam>
    /// <param name="services">Service collection</param>
    /// <param name="configuration">Configuration section containing KafkaConsumerConfiguration</param>
    /// <returns>Service collection for chaining</returns>
    public static IServiceCollection AddResilientKafkaConsumer<TMessage, THandler>(
        this IServiceCollection services,
        IConfiguration configuration)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        // Register configuration
        services.Configure<KafkaConsumerConfiguration>(configuration);

        // Register message handler
        services.AddScoped<IMessageHandler<TMessage>, THandler>();

        // Register resilient consumer
        services.AddSingleton<IResilientKafkaConsumer<TMessage>, ResilientKafkaConsumer<TMessage>>();

        return services;
    }

    /// <summary>
    /// Adds resilient Kafka consumer services to the service collection with action configuration
    /// </summary>
    /// <typeparam name="TMessage">Type of message to consume</typeparam>
    /// <typeparam name="THandler">Implementation of IMessageHandler</typeparam>
    /// <param name="services">Service collection</param>
    /// <param name="configureOptions">Action to configure KafkaConsumerConfiguration</param>
    /// <returns>Service collection for chaining</returns>
    public static IServiceCollection AddResilientKafkaConsumer<TMessage, THandler>(
        this IServiceCollection services,
        Action<KafkaConsumerConfiguration> configureOptions)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        // Register configuration
        services.Configure(configureOptions);

        // Register message handler
        services.AddScoped<IMessageHandler<TMessage>, THandler>();

        // Register resilient consumer
        services.AddSingleton<IResilientKafkaConsumer<TMessage>, ResilientKafkaConsumer<TMessage>>();

        return services;
    }
}

