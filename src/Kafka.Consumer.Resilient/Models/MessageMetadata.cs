namespace Ces.Kafka.Consumer.Resilient.Models;

/// <summary>
/// Metadata about a consumed message
/// </summary>
public class MessageMetadata
{
    /// <summary>
    /// Topic name
    /// </summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>
    /// Partition number
    /// </summary>
    public int Partition { get; set; }

    /// <summary>
    /// Message offset
    /// </summary>
    public long Offset { get; set; }

    /// <summary>
    /// Message key (if any)
    /// </summary>
    public string? Key { get; set; }

    /// <summary>
    /// Current retry attempt number
    /// </summary>
    public int RetryAttempt { get; set; }

    /// <summary>
    /// Timestamp when the message was originally produced
    /// </summary>
    public DateTime? Timestamp { get; set; }
}

