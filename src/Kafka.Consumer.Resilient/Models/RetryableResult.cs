namespace Ces.Kafka.Consumer.Resilient.Models;

/// <summary>
/// Represents a retryable failure - message will be sent to retry topic
/// </summary>
public class RetryableResult : ConsumerResult
{
    /// <summary>
    /// Creates a new retryable result
    /// </summary>
    /// <param name="message">Description of the retryable error</param>
    /// <param name="exception">Optional exception that caused the retryable failure</param>
    public RetryableResult(string message, Exception? exception = null)
    {
        Message = message;
        Exception = exception;
    }
}

