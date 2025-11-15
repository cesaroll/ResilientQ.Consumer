using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Ces.Kafka.Consumer.Resilient.Configuration;
using Ces.Kafka.Consumer.Resilient.Interfaces;
using Ces.Kafka.Consumer.Resilient.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Ces.Kafka.Consumer.Resilient.Services;

/// <summary>
/// Resilient Kafka consumer with retry logic
/// </summary>
/// <typeparam name="TMessage">Type of message to consume</typeparam>
public class ResilientKafkaConsumer<TMessage> : IResilientKafkaConsumer<TMessage> where TMessage : class
{
    private readonly KafkaConsumerConfiguration _configuration;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<ResilientKafkaConsumer<TMessage>> _logger;
    private readonly List<Task> _consumerTasks = new();
    private readonly CancellationTokenSource _internalCts = new();
    private readonly bool _isAvro;
    private bool _disposed;

    public ResilientKafkaConsumer(
        IOptions<KafkaConsumerConfiguration> configuration,
        IServiceProvider serviceProvider,
        ILogger<ResilientKafkaConsumer<TMessage>> logger)
    {
        _configuration = configuration.Value;
        _serviceProvider = serviceProvider;
        _logger = logger;

        // Determine if we're using Avro based on whether SchemaRegistryUrl is configured
        _isAvro = !string.IsNullOrWhiteSpace(_configuration.SchemaRegistryUrl);
    }

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Starting {ConsumerCount} consumer(s) for topic {Topic} with group {GroupId}",
            _configuration.ConsumerNumber,
            _configuration.TopicName,
            _configuration.GroupId);

        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _internalCts.Token);

        // Create topics to consume (main topic + configured retry topics)
        var topicsToConsume = new List<string> { _configuration.TopicName };

        topicsToConsume.AddRange(_configuration.RetryPolicy.RetryTopics.Select(rt => rt.TopicName));
        _logger.LogInformation("Configured retry topics: {RetryTopics}",
            string.Join(", ", _configuration.RetryPolicy.RetryTopics.Select(rt => rt.TopicName)));

        // Start multiple consumers
        for (int i = 0; i < _configuration.ConsumerNumber; i++)
        {
            var consumerId = i;
            var task = Task.Run(() => ConsumeAsync(consumerId, topicsToConsume, linkedCts.Token), linkedCts.Token);
            _consumerTasks.Add(task);
        }

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Stopping Kafka consumer...");
        _internalCts.Cancel();

        try
        {
            await Task.WhenAll(_consumerTasks);
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error while stopping consumers");
        }

        _logger.LogInformation("Kafka consumer stopped");
    }

    private async Task ConsumeAsync(int consumerId, List<string> topics, CancellationToken cancellationToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _configuration.BootstrapServers,
            GroupId = _configuration.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        };

        IProducer<string, byte[]>? producer = null;
        IConsumer<string, byte[]>? consumer = null;
        CachedSchemaRegistryClient? schemaRegistry = null;
        AvroDeserializer<TMessage>? avroDeserializer = null;

        try
        {
            // Setup producer for retry/error topics
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = _configuration.BootstrapServers,
                EnableIdempotence = true
            };
            producer = new ProducerBuilder<string, byte[]>(producerConfig).Build();

            // Setup schema registry if using Avro
            if (_isAvro)
            {
                var schemaRegistryConfig = new SchemaRegistryConfig
                {
                    Url = _configuration.SchemaRegistryUrl
                };
                schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
                avroDeserializer = new AvroDeserializer<TMessage>(schemaRegistry);
            }

            consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();
            consumer.Subscribe(topics);

            _logger.LogInformation(
                "Consumer {ConsumerId} started, subscribed to topics: {Topics}",
                consumerId,
                string.Join(", ", topics));

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = consumer.Consume(cancellationToken);

                    if (consumeResult?.Message == null)
                    {
                        continue;
                    }

                    await ProcessMessageAsync(
                        consumer,
                        producer,
                        consumeResult,
                        avroDeserializer,
                        cancellationToken);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Error consuming message on consumer {ConsumerId}", consumerId);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unexpected error in consumer {ConsumerId}", consumerId);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Fatal error in consumer {ConsumerId}", consumerId);
        }
        finally
        {
            consumer?.Close();
            consumer?.Dispose();
            producer?.Dispose();
            schemaRegistry?.Dispose();
        }
    }

    private async Task ProcessMessageAsync(
        IConsumer<string, byte[]> consumer,
        IProducer<string, byte[]> producer,
        ConsumeResult<string, byte[]> consumeResult,
        AvroDeserializer<TMessage>? avroDeserializer,
        CancellationToken cancellationToken)
    {
        var currentTopic = consumeResult.Topic;

        // Extract retry metadata from message headers
        var retryMetadata = ExtractRetryMetadata(consumeResult);
        var currentRetryAttempt = retryMetadata.TotalAttempts;

        try
        {
            // Deserialize message
            TMessage? message = await DeserializeMessageAsync(consumeResult.Message.Value, avroDeserializer, cancellationToken);

            if (message == null)
            {
                _logger.LogWarning("Failed to deserialize message at offset {Offset}, sending to error topic", consumeResult.Offset);
                await SendToErrorTopicAsync(producer, consumeResult, "Deserialization failed", cancellationToken);
                consumer.StoreOffset(consumeResult);
                consumer.Commit();
                return;
            }

            // Create metadata
            var metadata = new Models.MessageMetadata
            {
                Topic = currentTopic,
                Partition = consumeResult.Partition.Value,
                Offset = consumeResult.Offset.Value,
                Key = consumeResult.Message.Key,
                RetryAttempt = currentRetryAttempt,
                Timestamp = consumeResult.Message.Timestamp.UtcDateTime
            };

            // Process the message using a scoped handler
            ConsumerResult result;
            using (var scope = _serviceProvider.CreateScope())
            {
                var handler = scope.ServiceProvider.GetRequiredService<IMessageHandler<TMessage>>();
                result = await handler.HandleAsync(message, metadata, cancellationToken);
            }

            // Handle the result
            await HandleResultAsync(
                producer,
                consumeResult,
                result,
                retryMetadata,
                currentTopic,
                cancellationToken);

            // Commit offset
            consumer.StoreOffset(consumeResult);
            consumer.Commit();

            _logger.LogDebug(
                "Successfully processed message from {Topic} at offset {Offset}",
                currentTopic,
                consumeResult.Offset);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Error processing message from {Topic} at offset {Offset}",
                currentTopic,
                consumeResult.Offset);

            // On unexpected error, send to error topic
            await SendToErrorTopicAsync(producer, consumeResult, ex.Message, cancellationToken);
            consumer.StoreOffset(consumeResult);
            consumer.Commit();
        }
    }

    private async Task<TMessage?> DeserializeMessageAsync(
        byte[] messageBytes,
        AvroDeserializer<TMessage>? avroDeserializer,
        CancellationToken cancellationToken)
    {
        try
        {
            if (_isAvro && avroDeserializer != null)
            {
                // Deserialize Avro
                return await avroDeserializer.DeserializeAsync(
                    messageBytes,
                    false,
                    new SerializationContext(MessageComponentType.Value, string.Empty));
            }
            else
            {
                // Deserialize JSON
                var json = Encoding.UTF8.GetString(messageBytes);
                return JsonSerializer.Deserialize<TMessage>(json);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to deserialize message");
            return null;
        }
    }

    private async Task HandleResultAsync(
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
                await HandleRetryableErrorAsync(producer, consumeResult, retryable, retryMetadata, currentTopic, cancellationToken);
                break;

            case ErrorResult error:
                // Non-retryable error, send directly to error topic
                _logger.LogError("Non-retryable error: {Message}. Sending to error topic", error.Message);
                await SendToErrorTopicAsync(producer, consumeResult, error.Message ?? "Unknown error", cancellationToken);
                break;
        }
    }

    private async Task HandleRetryableErrorAsync(
        IProducer<string, byte[]> producer,
        ConsumeResult<string, byte[]> consumeResult,
        RetryableResult retryable,
        RetryMetadata retryMetadata,
        string currentTopic,
        CancellationToken cancellationToken)
    {
        await HandleRetryWithConfiguredTopicsAsync(producer, consumeResult, retryable, retryMetadata, currentTopic, cancellationToken);
    }

    private async Task HandleRetryWithConfiguredTopicsAsync(
        IProducer<string, byte[]> producer,
        ConsumeResult<string, byte[]> consumeResult,
        RetryableResult retryable,
        RetryMetadata retryMetadata,
        string currentTopic,
        CancellationToken cancellationToken)
    {
        var retryTopics = _configuration.RetryPolicy.RetryTopics;

        // If message is from main topic (RetryTopicIndex == -1), send to first retry topic
        if (retryMetadata.RetryTopicIndex == -1)
        {
            if (retryTopics.Count == 0)
            {
                _logger.LogError("No retry topics configured. Sending to error topic");
                await SendToErrorTopicAsync(producer, consumeResult, $"No retry configuration: {retryable.Message}", cancellationToken);
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

            await SendToRetryTopicAsync(producer, consumeResult, firstRetryTopicConfig.TopicName, newMetadata, cancellationToken);
            await Task.Delay(firstRetryTopicConfig.GetDelayTimeSpan(), cancellationToken);
            return;
        }

        // Message is from a retry topic
        var currentRetryTopicConfig = retryTopics.ElementAtOrDefault(retryMetadata.RetryTopicIndex);

        if (currentRetryTopicConfig == null)
        {
            _logger.LogError("No retry topic configuration found for index {Index}. Sending to error topic", retryMetadata.RetryTopicIndex);
            await SendToErrorTopicAsync(producer, consumeResult, $"Invalid retry configuration: {retryable.Message}", cancellationToken);
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

            await SendToRetryTopicAsync(producer, consumeResult, currentRetryTopicConfig.TopicName, newMetadata, cancellationToken);
            await Task.Delay(currentRetryTopicConfig.GetDelayTimeSpan(), cancellationToken);
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

            await SendToRetryTopicAsync(producer, consumeResult, nextRetryTopicConfig.TopicName, newMetadata, cancellationToken);
            await Task.Delay(nextRetryTopicConfig.GetDelayTimeSpan(), cancellationToken);
        }
        else
        {
            // No more retry topics, send to error topic
            _logger.LogError(
                "All retry attempts exhausted across all topics (total: {TotalAttempts}). Sending to error topic",
                retryMetadata.TotalAttempts + 1);

            await SendToErrorTopicAsync(producer, consumeResult, $"Max retries exceeded: {retryable.Message}", cancellationToken);
        }
    }

    private async Task SendToRetryTopicAsync(
        IProducer<string, byte[]> producer,
        ConsumeResult<string, byte[]> originalMessage,
        string retryTopic,
        RetryMetadata retryMetadata,
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

            // Add/Update retry metadata in headers
            AddOrUpdateHeader(message.Headers, "retry-topic-index", BitConverter.GetBytes(retryMetadata.RetryTopicIndex));
            AddOrUpdateHeader(message.Headers, "retry-attempts-in-topic", BitConverter.GetBytes(retryMetadata.AttemptsInCurrentTopic));
            AddOrUpdateHeader(message.Headers, "retry-total-attempts", BitConverter.GetBytes(retryMetadata.TotalAttempts));
            AddOrUpdateHeader(message.Headers, "retry-timestamp", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")));

            await producer.ProduceAsync(retryTopic, message, cancellationToken);
            _logger.LogInformation("Message sent to retry topic {RetryTopic}", retryTopic);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send message to retry topic {RetryTopic}", retryTopic);
            throw;
        }
    }

    private void AddOrUpdateHeader(Headers headers, string key, byte[] value)
    {
        headers.Remove(key);
        headers.Add(key, value);
    }

    private RetryMetadata ExtractRetryMetadata(ConsumeResult<string, byte[]> consumeResult)
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

    private async Task SendToErrorTopicAsync(
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

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _internalCts.Cancel();
        _internalCts.Dispose();
        _disposed = true;

        GC.SuppressFinalize(this);
    }
}

