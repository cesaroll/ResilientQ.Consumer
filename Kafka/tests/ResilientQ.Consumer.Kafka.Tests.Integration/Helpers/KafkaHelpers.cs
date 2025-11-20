using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System.Text;
using System.Text.Json;

namespace ResilientQ.Consumer.Kafka.Tests.Integration.Helpers;

public static class KafkaHelpers
{
    /// <summary>
    /// Creates Kafka topics if they don't exist
    /// </summary>
    public static async Task CreateTopicsAsync(string bootstrapServers, params string[] topicNames)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = bootstrapServers
        }).Build();

        var topicSpecs = topicNames.Select(name => new TopicSpecification
        {
            Name = name,
            NumPartitions = 1,
            ReplicationFactor = 1
        }).ToList();

        try
        {
            await adminClient.CreateTopicsAsync(topicSpecs);
        }
        catch (CreateTopicsException ex) when (ex.Results.Any(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // Topics already exist, ignore
        }
    }

    /// <summary>
    /// Deletes Kafka topics
    /// </summary>
    public static async Task DeleteTopicsAsync(string bootstrapServers, params string[] topicNames)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = bootstrapServers
        }).Build();

        try
        {
            await adminClient.DeleteTopicsAsync(topicNames);
        }
        catch (DeleteTopicsException)
        {
            // Ignore deletion errors
        }
    }

    /// <summary>
    /// Produces a JSON message to a topic
    /// </summary>
    public static async Task ProduceJsonMessageAsync<T>(
        string bootstrapServers,
        string topic,
        string key,
        T message)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();

        var json = JsonSerializer.Serialize(message);
        await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = key,
            Value = json
        });

        producer.Flush();
    }

    /// <summary>
    /// Consumes messages from a topic with a timeout
    /// </summary>
    public static async Task<List<ConsumeResult<string, string>>> ConsumeMessagesAsync(
        string bootstrapServers,
        string topic,
        string groupId,
        int maxMessages = 10,
        TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromSeconds(5);

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        var messages = new List<ConsumeResult<string, string>>();
        var cts = new CancellationTokenSource(timeout.Value);

        try
        {
            while (messages.Count < maxMessages && !cts.Token.IsCancellationRequested)
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(1));
                if (result != null)
                {
                    messages.Add(result);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Timeout reached
        }

        return messages;
    }

    /// <summary>
    /// Counts messages in a topic
    /// </summary>
    public static async Task<int> CountMessagesInTopicAsync(
        string bootstrapServers,
        string topic,
        TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromSeconds(5);
        var groupId = $"count-group-{Guid.NewGuid()}";
        var messages = await ConsumeMessagesAsync(bootstrapServers, topic, groupId, int.MaxValue, timeout);
        return messages.Count;
    }

    /// <summary>
    /// Waits for a condition to be true with timeout
    /// </summary>
    public static async Task<bool> WaitForConditionAsync(
        Func<Task<bool>> condition,
        TimeSpan timeout,
        TimeSpan? checkInterval = null)
    {
        checkInterval ??= TimeSpan.FromMilliseconds(500);
        var endTime = DateTime.UtcNow.Add(timeout);

        while (DateTime.UtcNow < endTime)
        {
            if (await condition())
            {
                return true;
            }

            await Task.Delay(checkInterval.Value);
        }

        return false;
    }

    /// <summary>
    /// Extracts retry metadata from message headers
    /// </summary>
    public static (int RetryTopicIndex, int AttemptsInCurrentTopic, int TotalAttempts) ExtractRetryMetadata(
        ConsumeResult<string, string> message)
    {
        int retryTopicIndex = -1;
        int attemptsInCurrentTopic = 0;
        int totalAttempts = 0;

        if (message.Message.Headers != null)
        {
            var retryTopicIndexHeader = message.Message.Headers.FirstOrDefault(h => h.Key == "retry-topic-index");
            if (retryTopicIndexHeader != null && retryTopicIndexHeader.GetValueBytes().Length >= 4)
            {
                retryTopicIndex = BitConverter.ToInt32(retryTopicIndexHeader.GetValueBytes());
            }

            var attemptsInTopicHeader = message.Message.Headers.FirstOrDefault(h => h.Key == "retry-attempts-in-topic");
            if (attemptsInTopicHeader != null && attemptsInTopicHeader.GetValueBytes().Length >= 4)
            {
                attemptsInCurrentTopic = BitConverter.ToInt32(attemptsInTopicHeader.GetValueBytes());
            }

            var totalAttemptsHeader = message.Message.Headers.FirstOrDefault(h => h.Key == "retry-total-attempts");
            if (totalAttemptsHeader != null && totalAttemptsHeader.GetValueBytes().Length >= 4)
            {
                totalAttempts = BitConverter.ToInt32(totalAttemptsHeader.GetValueBytes());
            }
        }

        return (retryTopicIndex, attemptsInCurrentTopic, totalAttempts);
    }
}

