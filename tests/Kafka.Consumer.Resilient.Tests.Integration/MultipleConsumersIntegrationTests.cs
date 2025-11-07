using Ces.Kafka.Consumer.Resilient.Configuration;
using Ces.Kafka.Consumer.Resilient.Interfaces;
using Ces.Kafka.Consumer.Resilient.Services;
using FluentAssertions;
using Kafka.Consumer.Resilient.Tests.Integration.Fixtures;
using Kafka.Consumer.Resilient.Tests.Integration.Helpers;
using Kafka.Consumer.Resilient.Tests.Integration.TestModels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit;

namespace Kafka.Consumer.Resilient.Tests.Integration;

[Collection("Kafka")]
public class MultipleConsumersIntegrationTests : IAsyncLifetime
{
    private readonly KafkaTestFixture _fixture;
    private readonly string _testId = Guid.NewGuid().ToString("N")[..8];
    private readonly List<string> _topicsToCleanup = new();

    public MultipleConsumersIntegrationTests(KafkaTestFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        await Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        if (_topicsToCleanup.Any())
        {
            await KafkaHelpers.DeleteTopicsAsync(_fixture.KafkaBootstrapServers, _topicsToCleanup.ToArray());
        }
    }

    [Fact]
    public async Task MultipleConsumers_ShouldDistributeWorkload()
    {
        // Arrange
        var mainTopic = $"test-multi-consumer-{_testId}";
        var retryTopic = $"{mainTopic}.retry.1";
        var errorTopic = $"{mainTopic}.error";

        _topicsToCleanup.AddRange(new[] { mainTopic, retryTopic, errorTopic });

        await KafkaHelpers.CreateTopicsAsync(_fixture.KafkaBootstrapServers, mainTopic, retryTopic, errorTopic);

        var config = new KafkaConsumerConfiguration
        {
            TopicName = mainTopic,
            ConsumerNumber = 3, // 3 consumers
            GroupId = $"test-group-{_testId}",
            BootstrapServers = _fixture.KafkaBootstrapServers,
            SchemaRegistryUrl = "",
            Error = new ErrorConfiguration { TopicName = errorTopic },
            RetryPolicy = new RetryPolicyConfiguration
            {
                RetryTopics = new List<RetryTopicConfiguration>
                {
                    new() { TopicName = retryTopic, Delay = "00:00:01", RetryAttempts = 1 }
                }
            }
        };

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Debug));
        services.AddSingleton(Microsoft.Extensions.Options.Options.Create(config));
        services.AddSingleton<IMessageHandler<TestMessage>, AlwaysSuccessHandler>();
        services.AddSingleton<IResilientKafkaConsumer<TestMessage>, ResilientKafkaConsumer<TestMessage>>();

        var provider = services.BuildServiceProvider();
        var consumer = provider.GetRequiredService<IResilientKafkaConsumer<TestMessage>>();

        // Act
        await consumer.StartAsync();

        // Produce multiple messages
        for (int i = 0; i < 10; i++)
        {
            var testMessage = new TestMessage { Id = $"MSG-{i:D3}", Content = $"Test message {i}" };
            await KafkaHelpers.ProduceJsonMessageAsync(_fixture.KafkaBootstrapServers, mainTopic, testMessage.Id, testMessage);
        }

        // Wait for processing
        await Task.Delay(TimeSpan.FromSeconds(5));

        await consumer.StopAsync();

        // Assert - No messages should be in retry or error (all successful)
        var retryMessages = await KafkaHelpers.ConsumeMessagesAsync(
            _fixture.KafkaBootstrapServers,
            retryTopic,
            $"verify-group-{_testId}",
            timeout: TimeSpan.FromSeconds(2));

        var errorMessages = await KafkaHelpers.ConsumeMessagesAsync(
            _fixture.KafkaBootstrapServers,
            errorTopic,
            $"verify-group-{_testId}",
            timeout: TimeSpan.FromSeconds(2));

        retryMessages.Should().BeEmpty("all messages should be processed successfully");
        errorMessages.Should().BeEmpty("all messages should be processed successfully");
    }

    [Fact]
    public async Task MultipleConsumers_WithRetries_ShouldHandleCorrectly()
    {
        // Arrange
        var mainTopic = $"test-multi-retry-{_testId}";
        var retryTopic = $"{mainTopic}.retry.1";
        var errorTopic = $"{mainTopic}.error";

        _topicsToCleanup.AddRange(new[] { mainTopic, retryTopic, errorTopic });

        await KafkaHelpers.CreateTopicsAsync(_fixture.KafkaBootstrapServers, mainTopic, retryTopic, errorTopic);

        var config = new KafkaConsumerConfiguration
        {
            TopicName = mainTopic,
            ConsumerNumber = 2, // 2 consumers
            GroupId = $"test-group-{_testId}",
            BootstrapServers = _fixture.KafkaBootstrapServers,
            SchemaRegistryUrl = "",
            Error = new ErrorConfiguration { TopicName = errorTopic },
            RetryPolicy = new RetryPolicyConfiguration
            {
                RetryTopics = new List<RetryTopicConfiguration>
                {
                    new() { TopicName = retryTopic, Delay = "00:00:01", RetryAttempts = 2 }
                }
            }
        };

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Debug));
        services.AddSingleton(Microsoft.Extensions.Options.Options.Create(config));
        services.AddSingleton<IMessageHandler<TestMessage>, AlwaysRetryableHandler>();
        services.AddSingleton<IResilientKafkaConsumer<TestMessage>, ResilientKafkaConsumer<TestMessage>>();

        var provider = services.BuildServiceProvider();
        var consumer = provider.GetRequiredService<IResilientKafkaConsumer<TestMessage>>();

        // Act
        await consumer.StartAsync();

        // Produce test messages
        for (int i = 0; i < 3; i++)
        {
            var testMessage = new TestMessage { Id = $"MSG-RETRY-{i:D3}", Content = $"Test retry {i}", ProcessingBehavior = 1 };
            await KafkaHelpers.ProduceJsonMessageAsync(_fixture.KafkaBootstrapServers, mainTopic, testMessage.Id, testMessage);
        }

        await Task.Delay(TimeSpan.FromSeconds(10));
        await consumer.StopAsync();

        // Assert
        var retryMessages = await KafkaHelpers.ConsumeMessagesAsync(
            _fixture.KafkaBootstrapServers,
            retryTopic,
            $"verify-group-{_testId}-retry",
            timeout: TimeSpan.FromSeconds(2));

        var errorMessages = await KafkaHelpers.ConsumeMessagesAsync(
            _fixture.KafkaBootstrapServers,
            errorTopic,
            $"verify-group-{_testId}-error",
            timeout: TimeSpan.FromSeconds(2));

        // Each message should be retried 2 times before going to error
        retryMessages.Should().HaveCount(6, "3 messages * 2 retries each");
        errorMessages.Should().HaveCount(3, "all 3 messages should end up in error after exhausting retries");
    }
}

