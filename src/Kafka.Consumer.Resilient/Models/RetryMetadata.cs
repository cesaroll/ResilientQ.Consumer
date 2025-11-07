namespace Ces.Kafka.Consumer.Resilient.Models;

/// <summary>
/// Metadata for tracking retry state of a message
/// </summary>
public class RetryMetadata
{
    /// <summary>
    /// Index of the current retry topic (0-based)
    /// </summary>
    public int RetryTopicIndex { get; set; }

    /// <summary>
    /// Number of attempts made in the current retry topic
    /// </summary>
    public int AttemptsInCurrentTopic { get; set; }

    /// <summary>
    /// Total number of retry attempts across all topics
    /// </summary>
    public int TotalAttempts { get; set; }
}

