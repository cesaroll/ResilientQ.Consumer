namespace Ces.Kafka.Consumer.Resilient.Services;

/// <summary>
/// Strategy for naming retry topics
/// </summary>
public class RetryTopicNamingStrategy
{
    /// <summary>
    /// Gets the retry topic name for a given base topic and retry attempt
    /// </summary>
    /// <param name="baseTopicName">Base topic name</param>
    /// <param name="retryAttempt">Current retry attempt number (1-based)</param>
    /// <returns>Retry topic name</returns>
    public static string GetRetryTopicName(string baseTopicName, int retryAttempt)
    {
        return $"{baseTopicName}.retry.{retryAttempt}";
    }

    /// <summary>
    /// Extracts the base topic name from a retry topic
    /// </summary>
    /// <param name="retryTopicName">Retry topic name</param>
    /// <returns>Base topic name</returns>
    public static string GetBaseTopicName(string retryTopicName)
    {
        var retryIndex = retryTopicName.IndexOf(".retry.", StringComparison.OrdinalIgnoreCase);
        return retryIndex >= 0 ? retryTopicName[..retryIndex] : retryTopicName;
    }

    /// <summary>
    /// Extracts the retry attempt number from a retry topic name
    /// </summary>
    /// <param name="topicName">Topic name (may be base or retry topic)</param>
    /// <returns>Retry attempt number (0 for base topic, 1+ for retry topics)</returns>
    public static int GetRetryAttempt(string topicName)
    {
        var retryIndex = topicName.IndexOf(".retry.", StringComparison.OrdinalIgnoreCase);
        if (retryIndex < 0)
        {
            return 0;
        }

        var attemptString = topicName[(retryIndex + 7)..];
        return int.TryParse(attemptString, out var attempt) ? attempt : 0;
    }
}

