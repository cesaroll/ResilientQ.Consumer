using Microsoft.Extensions.Configuration;

namespace Ces.Kafka.Consumer.Resilient.Configuration;

/// <summary>
/// Utility for loading Kafka consumer configuration from JSON files
/// </summary>
public static class ConfigurationLoader
{
    /// <summary>
    /// Loads Kafka consumer configuration from a JSON file
    /// </summary>
    /// <param name="jsonFilePath">Path to the JSON configuration file</param>
    /// <param name="sectionName">Optional section name in the JSON file (default: "KafkaConsumer")</param>
    /// <returns>Kafka consumer configuration</returns>
    public static KafkaConsumerConfiguration LoadFromJsonFile(
        string jsonFilePath,
        string sectionName = "KafkaConsumer")
    {
        if (!File.Exists(jsonFilePath))
        {
            throw new FileNotFoundException($"Configuration file not found: {jsonFilePath}");
        }

        var configuration = new ConfigurationBuilder()
            .AddJsonFile(jsonFilePath, optional: false, reloadOnChange: true)
            .Build();

        var config = new KafkaConsumerConfiguration();
        configuration.GetSection(sectionName).Bind(config);

        ValidateConfiguration(config);

        return config;
    }

    /// <summary>
    /// Creates an IConfiguration from a JSON file
    /// </summary>
    /// <param name="jsonFilePath">Path to the JSON configuration file</param>
    /// <returns>IConfiguration instance</returns>
    public static IConfiguration LoadConfiguration(string jsonFilePath)
    {
        if (!File.Exists(jsonFilePath))
        {
            throw new FileNotFoundException($"Configuration file not found: {jsonFilePath}");
        }

        return new ConfigurationBuilder()
            .AddJsonFile(jsonFilePath, optional: false, reloadOnChange: true)
            .Build();
    }

    /// <summary>
    /// Validates the Kafka consumer configuration
    /// </summary>
    /// <param name="config">Configuration to validate</param>
    /// <exception cref="InvalidOperationException">Thrown when configuration is invalid</exception>
    private static void ValidateConfiguration(KafkaConsumerConfiguration config)
    {
        if (string.IsNullOrWhiteSpace(config.TopicName))
        {
            throw new InvalidOperationException("TopicName is required in configuration");
        }

        if (string.IsNullOrWhiteSpace(config.GroupId))
        {
            throw new InvalidOperationException("GroupId is required in configuration");
        }

        if (string.IsNullOrWhiteSpace(config.BootstrapServers))
        {
            throw new InvalidOperationException("BootstrapServers is required in configuration");
        }

        if (string.IsNullOrWhiteSpace(config.Error?.TopicName))
        {
            throw new InvalidOperationException("Error.TopicName is required in configuration");
        }

        if (config.ConsumerNumber < 1)
        {
            throw new InvalidOperationException("ConsumerNumber must be at least 1");
        }

        if (config.RetryPolicy.RetryAttempts < 0)
        {
            throw new InvalidOperationException("RetryPolicy.RetryAttempts cannot be negative");
        }

        if (config.RetryPolicy.Delay < 0)
        {
            throw new InvalidOperationException("RetryPolicy.Delay cannot be negative");
        }
    }
}

