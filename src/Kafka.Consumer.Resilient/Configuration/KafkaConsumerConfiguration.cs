namespace Ces.Kafka.Consumer.Resilient.Configuration;

/// <summary>
/// Main configuration for the resilient Kafka consumer
/// </summary>
public class KafkaConsumerConfiguration
{
    /// <summary>
    /// Name of the Kafka topic to consume from
    /// </summary>
    public string TopicName { get; set; } = string.Empty;

    /// <summary>
    /// Number of concurrent consumers (default: 1)
    /// </summary>
    public int ConsumerNumber { get; set; } = 1;

    /// <summary>
    /// Kafka consumer group ID
    /// </summary>
    public string GroupId { get; set; } = string.Empty;

    /// <summary>
    /// Schema Registry URL for Avro serialization
    /// </summary>
    public string SchemaRegistryUrl { get; set; } = string.Empty;

    /// <summary>
    /// Kafka bootstrap servers (comma-separated list)
    /// </summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>
    /// Error topic configuration
    /// </summary>
    public ErrorConfiguration Error { get; set; } = new();

    /// <summary>
    /// Retry policy configuration
    /// </summary>
    public RetryPolicyConfiguration RetryPolicy { get; set; } = new();
}

