using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Ces.Kafka.Consumer.Resilient.Configuration;
using Ces.Kafka.Consumer.Resilient.Interfaces;
using Ces.Kafka.Consumer.Resilient.Models;
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
    private readonly IMessageHandler<TMessage> _messageHandler;
    private readonly ILogger<ResilientKafkaConsumer<TMessage>> _logger;
    private readonly List<Task> _consumerTasks = new();
    private readonly CancellationTokenSource _internalCts = new();
    private readonly bool _isAvro;
    private bool _disposed;

    public ResilientKafkaConsumer(
        IOptions<KafkaConsumerConfiguration> configuration,
        IMessageHandler<TMessage> messageHandler,
        ILogger<ResilientKafkaConsumer<TMessage>> logger)
    {
        _configuration = configuration.Value;
        _messageHandler = messageHandler;
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

        // Create all retry topics
        var topicsToConsume = new List<string> { _configuration.TopicName };
        for (int i = 1; i <= _configuration.RetryPolicy.RetryAttempts; i++)
        {
            topicsToConsume.Add(RetryTopicNamingStrategy.GetRetryTopicName(_configuration.TopicName, i));
        }

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
        var currentRetryAttempt = RetryTopicNamingStrategy.GetRetryAttempt(currentTopic);
        var baseTopicName = RetryTopicNamingStrategy.GetBaseTopicName(currentTopic);

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

            // Process the message
            var result = await _messageHandler.HandleAsync(message, metadata, cancellationToken);

            // Handle the result
            await HandleResultAsync(
                producer,
                consumeResult,
                result,
                currentRetryAttempt,
                baseTopicName,
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
        int currentRetryAttempt,
        string baseTopicName,
        CancellationToken cancellationToken)
    {
        switch (result)
        {
            case SuccessResult:
                // Message processed successfully, nothing more to do
                _logger.LogDebug("Message processed successfully");
                break;

            case RetryableResult retryable:
                // Check if we have more retry attempts
                if (currentRetryAttempt < _configuration.RetryPolicy.RetryAttempts)
                {
                    var nextRetryAttempt = currentRetryAttempt + 1;
                    var retryTopic = RetryTopicNamingStrategy.GetRetryTopicName(baseTopicName, nextRetryAttempt);

                    _logger.LogWarning(
                        "Retryable error: {Message}. Sending to retry topic {RetryTopic} (attempt {Attempt}/{MaxAttempts})",
                        retryable.Message,
                        retryTopic,
                        nextRetryAttempt,
                        _configuration.RetryPolicy.RetryAttempts);

                    await SendToRetryTopicAsync(
                        producer,
                        consumeResult,
                        retryTopic,
                        cancellationToken);

                    // Apply delay before next retry
                    await Task.Delay(_configuration.RetryPolicy.Delay, cancellationToken);
                }
                else
                {
                    _logger.LogError(
                        "Max retry attempts ({MaxAttempts}) reached. Sending to error topic",
                        _configuration.RetryPolicy.RetryAttempts);

                    await SendToErrorTopicAsync(
                        producer,
                        consumeResult,
                        $"Max retries exceeded: {retryable.Message}",
                        cancellationToken);
                }
                break;

            case ErrorResult error:
                // Non-retryable error, send directly to error topic
                _logger.LogError("Non-retryable error: {Message}. Sending to error topic", error.Message);
                await SendToErrorTopicAsync(producer, consumeResult, error.Message ?? "Unknown error", cancellationToken);
                break;
        }
    }

    private async Task SendToRetryTopicAsync(
        IProducer<string, byte[]> producer,
        ConsumeResult<string, byte[]> originalMessage,
        string retryTopic,
        CancellationToken cancellationToken)
    {
        try
        {
            var message = new Message<string, byte[]>
            {
                Key = originalMessage.Message.Key,
                Value = originalMessage.Message.Value,
                Headers = originalMessage.Message.Headers
            };

            await producer.ProduceAsync(retryTopic, message, cancellationToken);
            _logger.LogInformation("Message sent to retry topic {RetryTopic}", retryTopic);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send message to retry topic {RetryTopic}", retryTopic);
            throw;
        }
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

