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

        // Validate retry topics configuration
        if (config.RetryPolicy.RetryTopics.Any())
        {
            for (int i = 0; i < config.RetryPolicy.RetryTopics.Count; i++)
            {
                var retryTopic = config.RetryPolicy.RetryTopics[i];

                if (string.IsNullOrWhiteSpace(retryTopic.TopicName))
                {
                    throw new InvalidOperationException($"RetryPolicy.RetryTopics[{i}].TopicName is required");
                }

                if (string.IsNullOrWhiteSpace(retryTopic.Delay))
                {
                    throw new InvalidOperationException($"RetryPolicy.RetryTopics[{i}].Delay is required");
                }

                // Validate delay format
                try
                {
                    retryTopic.GetDelayTimeSpan();
                }
                catch (FormatException ex)
                {
                    throw new InvalidOperationException($"RetryPolicy.RetryTopics[{i}].Delay has invalid format: {ex.Message}");
                }

                if (retryTopic.RetryAttempts < 0)
                {
                    throw new InvalidOperationException($"RetryPolicy.RetryTopics[{i}].RetryAttempts cannot be negative");
                }
            }
        }
        else
        {
            throw new InvalidOperationException("RetryPolicy.RetryTopics must contain at least one retry topic configuration");
        }
    }
}

