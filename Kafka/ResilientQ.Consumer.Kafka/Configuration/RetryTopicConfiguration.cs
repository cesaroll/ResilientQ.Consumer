namespace ResilientQ.Consumer.Kafka.Configuration;

/// <summary>
/// Configuration for an individual retry topic
/// </summary>
public class RetryTopicConfiguration
{
    /// <summary>
    /// Name of the retry topic (required)
    /// </summary>
    public string TopicName { get; set; } = string.Empty;

    /// <summary>
    /// Delay before consuming from this retry topic in format "HH:mm:ss" (required)
    /// Example: "00:25:00" for 25 minutes
    /// </summary>
    public string Delay { get; set; } = "00:00:30";

    /// <summary>
    /// Number of retry attempts in this topic before moving to next retry topic (optional, default: 1)
    /// </summary>
    public int RetryAttempts { get; set; } = 1;

    /// <summary>
    /// Consumer group ID for this retry topic (optional, uses main GroupId if not provided)
    /// </summary>
    public string? GroupId { get; set; }

    /// <summary>
    /// Number of concurrent consumers for this retry topic (default: 1)
    /// </summary>
    public int ConsumerNumber { get; set; } = 1;

    /// <summary>
    /// Gets the delay as TimeSpan
    /// </summary>
    public TimeSpan GetDelayTimeSpan()
    {
        if (TimeSpan.TryParse(Delay, out var timeSpan))
        {
            return timeSpan;
        }
        throw new FormatException($"Invalid delay format '{Delay}'. Expected format: HH:mm:ss (e.g., '00:25:00')");
    }
}

