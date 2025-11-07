namespace Kafka.Consumer.Resilient.Tests.Integration.TestModels;

public class TestMessage
{
    public string Id { get; set; } = string.Empty;
    public string Content { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    public int ProcessingBehavior { get; set; } = 0; // 0 = success, 1 = retryable, 2 = error
}

