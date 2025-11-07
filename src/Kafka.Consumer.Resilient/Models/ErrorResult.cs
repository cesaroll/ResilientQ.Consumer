namespace Ces.Kafka.Consumer.Resilient.Models;

/// <summary>
/// Represents a non-retryable error - message will be sent directly to error topic
/// </summary>
public class ErrorResult : ConsumerResult
{
    /// <summary>
    /// Creates a new error result
    /// </summary>
    /// <param name="message">Description of the error</param>
    /// <param name="exception">Optional exception that caused the error</param>
    public ErrorResult(string message, Exception? exception = null)
    {
        Message = message;
        Exception = exception;
    }
}

