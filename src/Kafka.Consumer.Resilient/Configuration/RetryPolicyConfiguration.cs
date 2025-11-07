namespace Ces.Kafka.Consumer.Resilient.Configuration;

/// <summary>
/// Configuration for retry policy
/// </summary>
public class RetryPolicyConfiguration
{
    /// <summary>
    /// Delay between retry attempts in milliseconds
    /// </summary>
    public int Delay { get; set; } = 1000;

    /// <summary>
    /// Maximum number of retry attempts
    /// </summary>
    public int RetryAttempts { get; set; } = 3;
}

