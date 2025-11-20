using System.Text;
using Confluent.Kafka;
using ResilientQ.Consumer.Kafka.Configuration;
using Microsoft.Extensions.Logging;

namespace ResilientQ.Consumer.Kafka.Services;

/// <summary>
/// Handles routing messages to retry and error topics with delay management
/// </summary>
public class MessageRouter
{
    private readonly KafkaConsumerConfiguration _configuration;
    private readonly ILogger<MessageRouter> _logger;

    public MessageRouter(
        KafkaConsumerConfiguration configuration,
        ILogger<MessageRouter> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    /// <summary>
    /// Delays message processing if it has a process-after timestamp
    /// </summary>
    public async Task DelayIfNeededAsync(ConsumeResult<string, byte[]> consumeResult, CancellationToken cancellationToken)
    {
        if (consumeResult.Message.Headers == null)
        {
            return;
        }

        try
        {
            // Check if message has a process-after timestamp header
            var processAfterHeader = consumeResult.Message.Headers.FirstOrDefault(h => h.Key == "process-after-timestamp");
            if (processAfterHeader == null || processAfterHeader.GetValueBytes().Length < 8)
            {
                return;
            }

            var processAfterTicks = BitConverter.ToInt64(processAfterHeader.GetValueBytes());
            var processAfter = new DateTime(processAfterTicks, DateTimeKind.Utc);
            var now = DateTime.UtcNow;

            if (now < processAfter)
            {
                var delay = processAfter - now;
                _logger.LogInformation(
                    "Message from {Topic} at offset {Offset} not ready yet. Delaying for {Delay}",
                    consumeResult.Topic,
                    consumeResult.Offset,
                    delay);

                await Task.Delay(delay, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to process delay header. Processing message immediately.");
        }
    }

    /// <summary>
    /// Sends a message to a retry topic with delay timestamp
    /// </summary>
    public async Task SendToRetryTopicAsync(
        IProducer<string, byte[]> producer,
        ConsumeResult<string, byte[]> originalMessage,
        string retryTopic,
        RetryMetadata retryMetadata,
        TimeSpan delay,
        CancellationToken cancellationToken)
    {
        try
        {
            var message = new Message<string, byte[]>
            {
                Key = originalMessage.Message.Key,
                Value = originalMessage.Message.Value,
                Headers = originalMessage.Message.Headers ?? new Headers()
            };

            // Calculate when the message should be processed (current time + delay)
            var processAfter = DateTime.UtcNow.Add(delay);

            // Add/Update retry metadata in headers
            AddOrUpdateHeader(message.Headers, "retry-topic-index", BitConverter.GetBytes(retryMetadata.RetryTopicIndex));
            AddOrUpdateHeader(message.Headers, "retry-attempts-in-topic", BitConverter.GetBytes(retryMetadata.AttemptsInCurrentTopic));
            AddOrUpdateHeader(message.Headers, "retry-total-attempts", BitConverter.GetBytes(retryMetadata.TotalAttempts));
            AddOrUpdateHeader(message.Headers, "retry-timestamp", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")));

            // Add process-after timestamp for delay handling
            AddOrUpdateHeader(message.Headers, "process-after-timestamp", BitConverter.GetBytes(processAfter.Ticks));

            await producer.ProduceAsync(retryTopic, message, cancellationToken);
            _logger.LogInformation(
                "Message sent to retry topic {RetryTopic} (will be processed after {ProcessAfter})",
                retryTopic,
                processAfter);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send message to retry topic {RetryTopic}", retryTopic);
            throw;
        }
    }

    /// <summary>
    /// Sends a message to the error topic
    /// </summary>
    public async Task SendToErrorTopicAsync(
        IProducer<string, byte[]> producer,
        ConsumeResult<string, byte[]> originalMessage,
        string errorMessage,
        CancellationToken cancellationToken)
    {
        try
        {
            var message = new Message<string, byte[]>
            {
                Key = originalMessage.Message.Key,
                Value = originalMessage.Message.Value,
                Headers = originalMessage.Message.Headers ?? new Headers()
            };

            // Add error information to headers
            message.Headers.Add("error-message", Encoding.UTF8.GetBytes(errorMessage));
            message.Headers.Add("error-timestamp", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")));
            message.Headers.Add("original-topic", Encoding.UTF8.GetBytes(originalMessage.Topic));
            message.Headers.Add("original-partition", BitConverter.GetBytes(originalMessage.Partition.Value));
            message.Headers.Add("original-offset", BitConverter.GetBytes(originalMessage.Offset.Value));

            await producer.ProduceAsync(_configuration.Error.TopicName, message, cancellationToken);
            _logger.LogInformation("Message sent to error topic {ErrorTopic}", _configuration.Error.TopicName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send message to error topic {ErrorTopic}", _configuration.Error.TopicName);
            throw;
        }
    }

    /// <summary>
    /// Extracts retry metadata from message headers
    /// </summary>
    public RetryMetadata ExtractRetryMetadata(ConsumeResult<string, byte[]> consumeResult)
    {
        var metadata = new RetryMetadata
        {
            RetryTopicIndex = -1,  // -1 indicates main topic (no retry yet)
            AttemptsInCurrentTopic = 0,
            TotalAttempts = 0
        };

        if (consumeResult.Message.Headers == null)
        {
            return metadata;
        }

        try
        {
            var retryTopicIndexHeader = consumeResult.Message.Headers.FirstOrDefault(h => h.Key == "retry-topic-index");
            if (retryTopicIndexHeader != null && retryTopicIndexHeader.GetValueBytes().Length >= 4)
            {
                metadata.RetryTopicIndex = BitConverter.ToInt32(retryTopicIndexHeader.GetValueBytes());
            }

            var attemptsInTopicHeader = consumeResult.Message.Headers.FirstOrDefault(h => h.Key == "retry-attempts-in-topic");
            if (attemptsInTopicHeader != null && attemptsInTopicHeader.GetValueBytes().Length >= 4)
            {
                metadata.AttemptsInCurrentTopic = BitConverter.ToInt32(attemptsInTopicHeader.GetValueBytes());
            }

            var totalAttemptsHeader = consumeResult.Message.Headers.FirstOrDefault(h => h.Key == "retry-total-attempts");
            if (totalAttemptsHeader != null && totalAttemptsHeader.GetValueBytes().Length >= 4)
            {
                metadata.TotalAttempts = BitConverter.ToInt32(totalAttemptsHeader.GetValueBytes());
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to extract retry metadata from headers. Using default values.");
        }

        return metadata;
    }

    private void AddOrUpdateHeader(Headers headers, string key, byte[] value)
    {
        headers.Remove(key);
        headers.Add(key, value);
    }
}

/// <summary>
/// Retry metadata extracted from message headers
/// </summary>
public class RetryMetadata
{
    public int RetryTopicIndex { get; set; }
    public int AttemptsInCurrentTopic { get; set; }
    public int TotalAttempts { get; set; }
}
