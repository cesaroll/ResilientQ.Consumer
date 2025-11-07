namespace Ces.Kafka.Consumer.Resilient.Models;

/// <summary>
/// Base class for consumer result types
/// </summary>
public abstract class ConsumerResult
{
    /// <summary>
    /// Optional message describing the result
    /// </summary>
    public string? Message { get; set; }

    /// <summary>
    /// Optional exception that occurred during processing
    /// </summary>
    public Exception? Exception { get; set; }

    /// <summary>
    /// Timestamp when the result was created
    /// </summary>
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}

