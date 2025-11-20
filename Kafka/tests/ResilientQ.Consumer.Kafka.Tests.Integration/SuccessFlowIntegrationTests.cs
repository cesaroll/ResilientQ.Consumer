using ResilientQ.Consumer.Kafka.Configuration;
using ResilientQ.Consumer.Kafka.Interfaces;
using ResilientQ.Consumer.Kafka.Services;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ResilientQ.Consumer.Kafka.Tests.Integration.Fixtures;
using ResilientQ.Consumer.Kafka.Tests.Integration.Helpers;
using ResilientQ.Consumer.Kafka.Tests.Integration.TestModels;
using Xunit;

namespace ResilientQ.Consumer.Kafka.Tests.Integration;

[Collection("Kafka")]
public class SuccessFlowIntegrationTests : IAsyncLifetime
{
    private readonly KafkaTestFixture _fixture;
    private readonly string _testId = Guid.NewGuid().ToString("N")[..8];
    private readonly List<string> _topicsToCleanup = new();

    public SuccessFlowIntegrationTests(KafkaTestFixture fixture)
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
    public async Task SuccessfulMessage_ShouldBeCommitted_AndNotRetried()
    {
        // Arrange
        var mainTopic = $"test-success-{_testId}";
        var retryTopic = $"{mainTopic}.retry.1";
        var errorTopic = $"{mainTopic}.error";

        _topicsToCleanup.AddRange(new[] { mainTopic, retryTopic, errorTopic });

        await KafkaHelpers.CreateTopicsAsync(_fixture.KafkaBootstrapServers, mainTopic, retryTopic, errorTopic);

        var config = new KafkaConsumerConfiguration
        {
            TopicName = mainTopic,
            ConsumerNumber = 1,
            GroupId = $"test-group-{_testId}",
            BootstrapServers = _fixture.KafkaBootstrapServers,
            SchemaRegistryUrl = "", // JSON mode
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
        services.AddSingleton<IMessageHandler<TestMessage>, AlwaysSuccessHandler>();
        services.AddSingleton<IResilientKafkaConsumer<TestMessage>, ResilientKafkaConsumer<TestMessage>>();

        var provider = services.BuildServiceProvider();
        var consumer = provider.GetRequiredService<IResilientKafkaConsumer<TestMessage>>();

        // Act
        await consumer.StartAsync();

        // Produce a test message
        var testMessage = new TestMessage { Id = "MSG-001", Content = "Test message", ProcessingBehavior = 0 };
        await KafkaHelpers.ProduceJsonMessageAsync(_fixture.KafkaBootstrapServers, mainTopic, testMessage.Id, testMessage);

        // Wait for processing
        await Task.Delay(TimeSpan.FromSeconds(3));

        await consumer.StopAsync();

        // Assert - Message should NOT be in retry or error topics
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

        retryMessages.Should().BeEmpty("successful messages should not be retried");
        errorMessages.Should().BeEmpty("successful messages should not go to error topic");
    }

    [Fact]
    public async Task MultipleSuccessfulMessages_ShouldAllBeProcessed()
    {
        // Arrange
        var mainTopic = $"test-multi-success-{_testId}";
        var retryTopic = $"{mainTopic}.retry.1";
        var errorTopic = $"{mainTopic}.error";

        _topicsToCleanup.AddRange(new[] { mainTopic, retryTopic, errorTopic });

        await KafkaHelpers.CreateTopicsAsync(_fixture.KafkaBootstrapServers, mainTopic, retryTopic, errorTopic);

        var config = new KafkaConsumerConfiguration
        {
            TopicName = mainTopic,
            ConsumerNumber = 1,
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
        services.AddSingleton<IMessageHandler<TestMessage>, AlwaysSuccessHandler>();
        services.AddSingleton<IResilientKafkaConsumer<TestMessage>, ResilientKafkaConsumer<TestMessage>>();

        var provider = services.BuildServiceProvider();
        var consumer = provider.GetRequiredService<IResilientKafkaConsumer<TestMessage>>();

        // Act
        await consumer.StartAsync();

        // Produce multiple test messages
        for (int i = 0; i < 5; i++)
        {
            var testMessage = new TestMessage { Id = $"MSG-{i:D3}", Content = $"Test message {i}", ProcessingBehavior = 0 };
            await KafkaHelpers.ProduceJsonMessageAsync(_fixture.KafkaBootstrapServers, mainTopic, testMessage.Id, testMessage);
        }

        // Wait for processing
        await Task.Delay(TimeSpan.FromSeconds(5));

        await consumer.StopAsync();

        // Assert - No messages should be in retry or error topics
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

        retryMessages.Should().BeEmpty();
        errorMessages.Should().BeEmpty();
    }
}

