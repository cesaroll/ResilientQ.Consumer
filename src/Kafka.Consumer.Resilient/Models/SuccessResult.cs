namespace Ces.Kafka.Consumer.Resilient.Models;

/// <summary>
/// Represents a successful message processing result
/// </summary>
public class SuccessResult : ConsumerResult
{
    /// <summary>
    /// Creates a new success result
    /// </summary>
    /// <param name="message">Optional success message</param>
    public SuccessResult(string? message = null)
    {
        Message = message ?? "Message processed successfully";
    }
}

