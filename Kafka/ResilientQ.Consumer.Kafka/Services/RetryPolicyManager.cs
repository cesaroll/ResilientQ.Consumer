using Confluent.Kafka;
using ResilientQ.Consumer.Kafka.Configuration;
using ResilientQ.Consumer.Kafka.Models;
using Microsoft.Extensions.Logging;

namespace ResilientQ.Consumer.Kafka.Services;

/// <summary>
/// Manages retry policy logic and routing decisions
/// </summary>
public class RetryPolicyManager
{
    private readonly KafkaConsumerConfiguration _configuration;
    private readonly MessageRouter _router;
    private readonly ILogger<RetryPolicyManager> _logger;

    public RetryPolicyManager(
        KafkaConsumerConfiguration configuration,
        MessageRouter router,
        ILogger<RetryPolicyManager> logger)
    {
        _configuration = configuration;
        _router = router;
        _logger = logger;
    }

    /// <summary>
    /// Handles the result of message processing
    /// </summary>
    public async Task HandleResultAsync(
        IProducer<string, byte[]> producer,
        ConsumeResult<string, byte[]> consumeResult,
        ConsumerResult result,
        RetryMetadata retryMetadata,
        string currentTopic,
        CancellationToken cancellationToken)
    {
        switch (result)
        {
            case SuccessResult:
                // Message processed successfully, nothing more to do
                _logger.LogDebug("Message processed successfully");
                break;

            case RetryableResult retryable:
                await HandleRetryableErrorAsync(producer, consumeResult, retryable, retryMetadata, cancellationToken);
                break;

            case ErrorResult error:
                // Non-retryable error, send directly to error topic
                _logger.LogError("Non-retryable error: {Message}. Sending to error topic", error.Message);
                await _router.SendToErrorTopicAsync(producer, consumeResult, error.Message ?? "Unknown error", cancellationToken);
                break;
        }
    }

    private async Task HandleRetryableErrorAsync(
        IProducer<string, byte[]> producer,
        ConsumeResult<string, byte[]> consumeResult,
        RetryableResult retryable,
        RetryMetadata retryMetadata,
        CancellationToken cancellationToken)
    {
        await HandleRetryWithConfiguredTopicsAsync(producer, consumeResult, retryable, retryMetadata, cancellationToken);
    }

    private async Task HandleRetryWithConfiguredTopicsAsync(
        IProducer<string, byte[]> producer,
        ConsumeResult<string, byte[]> consumeResult,
        RetryableResult retryable,
        RetryMetadata retryMetadata,
        CancellationToken cancellationToken)
    {
        var retryTopics = _configuration.RetryPolicy.RetryTopics;

        // If message is from main topic (RetryTopicIndex == -1), send to first retry topic
        if (retryMetadata.RetryTopicIndex == -1)
        {
            if (retryTopics.Count == 0)
            {
                _logger.LogError("No retry topics configured. Sending to error topic");
                await _router.SendToErrorTopicAsync(producer, consumeResult, $"No retry configuration: {retryable.Message}", cancellationToken);
                return;
            }

            var firstRetryTopicConfig = retryTopics[0];
            var newMetadata = new RetryMetadata
            {
                RetryTopicIndex = 0,
                AttemptsInCurrentTopic = 0,
                TotalAttempts = 1
            };

            _logger.LogWarning(
                "Retryable error: {Message}. Sending to first retry topic {RetryTopic} (attempt 1/{MaxAttempts} in this topic, total attempts: {TotalAttempts})",
                retryable.Message,
                firstRetryTopicConfig.TopicName,
                firstRetryTopicConfig.RetryAttempts,
                newMetadata.TotalAttempts);

            await _router.SendToRetryTopicAsync(producer, consumeResult, firstRetryTopicConfig.TopicName, newMetadata, firstRetryTopicConfig.GetDelayTimeSpan(), cancellationToken);
            return;
        }

        // Message is from a retry topic
        var currentRetryTopicConfig = retryTopics.ElementAtOrDefault(retryMetadata.RetryTopicIndex);

        if (currentRetryTopicConfig == null)
        {
            _logger.LogError("No retry topic configuration found for index {Index}. Sending to error topic", retryMetadata.RetryTopicIndex);
            await _router.SendToErrorTopicAsync(producer, consumeResult, $"Invalid retry configuration: {retryable.Message}", cancellationToken);
            return;
        }

        // Check if we should retry in the same topic or move to next
        var attemptsInCurrentTopic = retryMetadata.AttemptsInCurrentTopic + 1;
        var maxAttemptsForCurrentTopic = currentRetryTopicConfig.RetryAttempts;

        if (attemptsInCurrentTopic < maxAttemptsForCurrentTopic)
        {
            // Retry in the same topic
            var newMetadata = new RetryMetadata
            {
                RetryTopicIndex = retryMetadata.RetryTopicIndex,
                AttemptsInCurrentTopic = attemptsInCurrentTopic,
                TotalAttempts = retryMetadata.TotalAttempts + 1
            };

            _logger.LogWarning(
                "Retryable error: {Message}. Retrying in same topic {RetryTopic} (attempt {Attempt}/{MaxAttempts} in this topic, total attempts: {TotalAttempts})",
                retryable.Message,
                currentRetryTopicConfig.TopicName,
                attemptsInCurrentTopic,
                maxAttemptsForCurrentTopic,
                newMetadata.TotalAttempts);

            await _router.SendToRetryTopicAsync(producer, consumeResult, currentRetryTopicConfig.TopicName, newMetadata, currentRetryTopicConfig.GetDelayTimeSpan(), cancellationToken);
        }
        else if (retryMetadata.RetryTopicIndex + 1 < retryTopics.Count)
        {
            // Move to next retry topic
            var nextRetryTopicConfig = retryTopics[retryMetadata.RetryTopicIndex + 1];
            var newMetadata = new RetryMetadata
            {
                RetryTopicIndex = retryMetadata.RetryTopicIndex + 1,
                AttemptsInCurrentTopic = 0,
                TotalAttempts = retryMetadata.TotalAttempts + 1
            };

            _logger.LogWarning(
                "Max attempts in current topic reached. Moving to next retry topic {NextTopic} (total attempts: {TotalAttempts})",
                nextRetryTopicConfig.TopicName,
                newMetadata.TotalAttempts);

            await _router.SendToRetryTopicAsync(producer, consumeResult, nextRetryTopicConfig.TopicName, newMetadata, nextRetryTopicConfig.GetDelayTimeSpan(), cancellationToken);
        }
        else
        {
            // No more retry topics, send to error topic
            _logger.LogError(
                "All retry attempts exhausted across all topics (total: {TotalAttempts}). Sending to error topic",
                retryMetadata.TotalAttempts + 1);

            await _router.SendToErrorTopicAsync(producer, consumeResult, $"Max retries exceeded: {retryable.Message}", cancellationToken);
        }
    }
}
