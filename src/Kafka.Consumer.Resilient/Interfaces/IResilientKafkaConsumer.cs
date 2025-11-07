namespace Ces.Kafka.Consumer.Resilient.Interfaces;

/// <summary>
/// Interface for a resilient Kafka consumer
/// </summary>
public interface IResilientKafkaConsumer<TMessage> : IDisposable
{
    /// <summary>
    /// Starts consuming messages from Kafka
    /// </summary>
    /// <param name="cancellationToken">Cancellation token to stop consumption</param>
    /// <returns>Task representing the consumption operation</returns>
    Task StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops consuming messages
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Task representing the stop operation</returns>
    Task StopAsync(CancellationToken cancellationToken = default);
}

