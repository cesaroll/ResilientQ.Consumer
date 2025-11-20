using ResilientQ.Consumer.Kafka.Models;

namespace ResilientQ.Consumer.Kafka.Interfaces;

/// <summary>
/// Interface for handling consumed messages
/// </summary>
/// <typeparam name="TMessage">Type of the message to handle</typeparam>
public interface IMessageHandler<TMessage>
{
    /// <summary>
    /// Process a consumed message
    /// </summary>
    /// <param name="message">The message to process</param>
    /// <param name="metadata">Metadata about the message</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Consumer result indicating success, retry, or error</returns>
    Task<ConsumerResult> HandleAsync(TMessage message, MessageMetadata metadata, CancellationToken cancellationToken = default);
}

