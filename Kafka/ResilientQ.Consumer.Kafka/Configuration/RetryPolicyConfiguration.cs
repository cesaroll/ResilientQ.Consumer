namespace ResilientQ.Consumer.Kafka.Configuration;

/// <summary>
/// Configuration for retry policy
/// </summary>
public class RetryPolicyConfiguration
{
    /// <summary>
    /// Array of retry topics with individual configuration
    /// Each retry topic can have its own delay, retry attempts, and group ID
    /// </summary>
    public List<RetryTopicConfiguration> RetryTopics { get; set; } = new();
}

