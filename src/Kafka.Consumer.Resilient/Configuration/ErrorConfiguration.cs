namespace Ces.Kafka.Consumer.Resilient.Configuration;

/// <summary>
/// Configuration for error topic
/// </summary>
public class ErrorConfiguration
{
    /// <summary>
    /// Name of the error topic where failed messages will be sent
    /// </summary>
    public string TopicName { get; set; } = string.Empty;
}

