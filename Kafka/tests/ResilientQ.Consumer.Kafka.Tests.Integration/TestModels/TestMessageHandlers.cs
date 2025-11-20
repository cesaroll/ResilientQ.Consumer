using ResilientQ.Consumer.Kafka.Interfaces;
using ResilientQ.Consumer.Kafka.Models;

namespace ResilientQ.Consumer.Kafka.Tests.Integration.TestModels;

/// <summary>
/// Test handler that always returns success
/// </summary>
public class AlwaysSuccessHandler : IMessageHandler<TestMessage>
{
    public Task<ConsumerResult> HandleAsync(TestMessage message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        return Task.FromResult<ConsumerResult>(new SuccessResult());
    }
}

/// <summary>
/// Test handler that always returns retryable error
/// </summary>
public class AlwaysRetryableHandler : IMessageHandler<TestMessage>
{
    public Task<ConsumerResult> HandleAsync(TestMessage message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        return Task.FromResult<ConsumerResult>(new RetryableResult($"Retryable error for message {message.Id}"));
    }
}

/// <summary>
/// Test handler that always returns error
/// </summary>
public class AlwaysErrorHandler : IMessageHandler<TestMessage>
{
    public Task<ConsumerResult> HandleAsync(TestMessage message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        return Task.FromResult<ConsumerResult>(new ErrorResult($"Error for message {message.Id}"));
    }
}

/// <summary>
/// Test handler that uses the ProcessingBehavior field to determine result
/// </summary>
public class ConfigurableHandler : IMessageHandler<TestMessage>
{
    public Task<ConsumerResult> HandleAsync(TestMessage message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        return Task.FromResult<ConsumerResult>(message.ProcessingBehavior switch
        {
            0 => new SuccessResult(),
            1 => new RetryableResult($"Retryable error for message {message.Id}"),
            2 => new ErrorResult($"Error for message {message.Id}"),
            _ => new ErrorResult("Unknown processing behavior")
        });
    }
}

/// <summary>
/// Test handler that succeeds after N retries
/// </summary>
public class SucceedAfterNRetriesHandler : IMessageHandler<TestMessage>
{
    private readonly int _successAfterAttempt;
    private readonly Dictionary<string, int> _attemptCounts = new();

    public SucceedAfterNRetriesHandler(int successAfterAttempt)
    {
        _successAfterAttempt = successAfterAttempt;
    }

    public Task<ConsumerResult> HandleAsync(TestMessage message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        if (!_attemptCounts.ContainsKey(message.Id))
        {
            _attemptCounts[message.Id] = 0;
        }

        _attemptCounts[message.Id]++;

        if (_attemptCounts[message.Id] >= _successAfterAttempt)
        {
            return Task.FromResult<ConsumerResult>(new SuccessResult());
        }

        return Task.FromResult<ConsumerResult>(new RetryableResult($"Retry attempt {_attemptCounts[message.Id]}"));
    }
}

/// <summary>
/// Test handler that tracks how many times each message was processed
/// </summary>
public class TrackingHandler : IMessageHandler<TestMessage>
{
    public List<(string MessageId, int RetryAttempt, DateTime ProcessedAt)> ProcessedMessages { get; } = new();

    public Task<ConsumerResult> HandleAsync(TestMessage message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        ProcessedMessages.Add((message.Id, metadata.RetryAttempt, DateTime.UtcNow));

        // Always return retryable to test retry flow
        return Task.FromResult<ConsumerResult>(new RetryableResult($"Tracking message {message.Id}"));
    }
}

