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
public class RetryPolicyIntegrationTests : IAsyncLifetime
{
    private readonly KafkaTestFixture _fixture;
    private readonly string _testId = Guid.NewGuid().ToString("N")[..8];
    private readonly List<string> _topicsToCleanup = new();

    public RetryPolicyIntegrationTests(KafkaTestFixture fixture)
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
    public async Task RetryableMessage_ShouldRetryInSameTopic_BeforeMovingToNext()
    {
        // Arrange
        var mainTopic = $"test-retry-same-{_testId}";
        var retryTopic1 = $"{mainTopic}.retry.1";
        var retryTopic2 = $"{mainTopic}.retry.2";
        var errorTopic = $"{mainTopic}.error";

        _topicsToCleanup.AddRange(new[] { mainTopic, retryTopic1, retryTopic2, errorTopic });

        await KafkaHelpers.CreateTopicsAsync(_fixture.KafkaBootstrapServers, mainTopic, retryTopic1, retryTopic2, errorTopic);

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
                    new() { TopicName = retryTopic1, Delay = "00:00:01", RetryAttempts = 2 },
                    new() { TopicName = retryTopic2, Delay = "00:00:01", RetryAttempts = 1 }
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

        var testMessage = new TestMessage { Id = "MSG-RETRY-001", Content = "Test retry", ProcessingBehavior = 1 };
        await KafkaHelpers.ProduceJsonMessageAsync(_fixture.KafkaBootstrapServers, mainTopic, testMessage.Id, testMessage);

        // Wait for retries (2 attempts in topic1 + 1 attempt in topic2 = 3 retries, plus delays)
        await Task.Delay(TimeSpan.FromSeconds(10));

        await consumer.StopAsync();

        // Assert
        var retry1Messages = await KafkaHelpers.ConsumeMessagesAsync(
            _fixture.KafkaBootstrapServers,
            retryTopic1,
            $"verify-group-{_testId}-r1",
            timeout: TimeSpan.FromSeconds(2));

        var retry2Messages = await KafkaHelpers.ConsumeMessagesAsync(
            _fixture.KafkaBootstrapServers,
            retryTopic2,
            $"verify-group-{_testId}-r2",
            timeout: TimeSpan.FromSeconds(2));

        var errorMessages = await KafkaHelpers.ConsumeMessagesAsync(
            _fixture.KafkaBootstrapServers,
            errorTopic,
            $"verify-group-{_testId}-err",
            timeout: TimeSpan.FromSeconds(2));

        retry1Messages.Should().HaveCount(2, "message should be retried 2 times in first retry topic");
        retry2Messages.Should().HaveCount(1, "message should be retried 1 time in second retry topic");
        errorMessages.Should().HaveCount(1, "message should end up in error topic after all retries");
    }

    [Fact]
    public async Task RetryMetadata_ShouldBeTrackedCorrectly()
    {
        // Arrange
        var mainTopic = $"test-retry-metadata-{_testId}";
        var retryTopic1 = $"{mainTopic}.retry.1";
        var retryTopic2 = $"{mainTopic}.retry.2";
        var errorTopic = $"{mainTopic}.error";

        _topicsToCleanup.AddRange(new[] { mainTopic, retryTopic1, retryTopic2, errorTopic });

        await KafkaHelpers.CreateTopicsAsync(_fixture.KafkaBootstrapServers, mainTopic, retryTopic1, retryTopic2, errorTopic);

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
                    new() { TopicName = retryTopic1, Delay = "00:00:01", RetryAttempts = 2 },
                    new() { TopicName = retryTopic2, Delay = "00:00:01", RetryAttempts = 1 }
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

        var testMessage = new TestMessage { Id = "MSG-META-001", Content = "Test metadata", ProcessingBehavior = 1 };
        await KafkaHelpers.ProduceJsonMessageAsync(_fixture.KafkaBootstrapServers, mainTopic, testMessage.Id, testMessage);

        await Task.Delay(TimeSpan.FromSeconds(10));
        await consumer.StopAsync();

        // Assert - Check retry metadata in retry topics
        var retry1Messages = await KafkaHelpers.ConsumeMessagesAsync(
            _fixture.KafkaBootstrapServers,
            retryTopic1,
            $"verify-group-{_testId}-meta1",
            timeout: TimeSpan.FromSeconds(2));

        retry1Messages.Should().HaveCount(2);

        // First message in retry topic 1
        var (retryIndex1, attemptsInTopic1, totalAttempts1) = KafkaHelpers.ExtractRetryMetadata(retry1Messages[0]);
        retryIndex1.Should().Be(0, "first retry topic has index 0");
        attemptsInTopic1.Should().Be(0, "first attempt in this topic");
        totalAttempts1.Should().Be(1, "first overall attempt");

        // Second message in retry topic 1
        var (retryIndex2, attemptsInTopic2, totalAttempts2) = KafkaHelpers.ExtractRetryMetadata(retry1Messages[1]);
        retryIndex2.Should().Be(0, "still in first retry topic");
        attemptsInTopic2.Should().Be(1, "second attempt in this topic");
        totalAttempts2.Should().Be(2, "second overall attempt");

        // Check retry topic 2
        var retry2Messages = await KafkaHelpers.ConsumeMessagesAsync(
            _fixture.KafkaBootstrapServers,
            retryTopic2,
            $"verify-group-{_testId}-meta2",
            timeout: TimeSpan.FromSeconds(2));

        retry2Messages.Should().HaveCount(1);

        var (retryIndex3, attemptsInTopic3, totalAttempts3) = KafkaHelpers.ExtractRetryMetadata(retry2Messages[0]);
        retryIndex3.Should().Be(1, "moved to second retry topic (index 1)");
        attemptsInTopic3.Should().Be(0, "first attempt in new topic");
        totalAttempts3.Should().Be(3, "third overall attempt");
    }

    [Fact]
    public async Task AllRetriesExhausted_ShouldSendToErrorTopic()
    {
        // Arrange
        var mainTopic = $"test-retry-exhausted-{_testId}";
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
        services.AddSingleton<IMessageHandler<TestMessage>, AlwaysRetryableHandler>();
        services.AddSingleton<IResilientKafkaConsumer<TestMessage>, ResilientKafkaConsumer<TestMessage>>();

        var provider = services.BuildServiceProvider();
        var consumer = provider.GetRequiredService<IResilientKafkaConsumer<TestMessage>>();

        // Act
        await consumer.StartAsync();

        var testMessage = new TestMessage { Id = "MSG-EXHAUST-001", Content = "Test exhaustion", ProcessingBehavior = 1 };
        await KafkaHelpers.ProduceJsonMessageAsync(_fixture.KafkaBootstrapServers, mainTopic, testMessage.Id, testMessage);

        await Task.Delay(TimeSpan.FromSeconds(8));
        await consumer.StopAsync();

        // Assert
        var errorMessages = await KafkaHelpers.ConsumeMessagesAsync(
            _fixture.KafkaBootstrapServers,
            errorTopic,
            $"verify-group-{_testId}",
            timeout: TimeSpan.FromSeconds(2));

        errorMessages.Should().HaveCount(1, "message should end up in error topic after all retries exhausted");
        errorMessages[0].Message.Key.Should().Be(testMessage.Id);
    }

    [Fact]
    public async Task SucceedAfterRetries_ShouldNotContinueRetrying()
    {
        // Arrange
        var mainTopic = $"test-succeed-after-retry-{_testId}";
        var retryTopic1 = $"{mainTopic}.retry.1";
        var retryTopic2 = $"{mainTopic}.retry.2";
        var errorTopic = $"{mainTopic}.error";

        _topicsToCleanup.AddRange(new[] { mainTopic, retryTopic1, retryTopic2, errorTopic });

        await KafkaHelpers.CreateTopicsAsync(_fixture.KafkaBootstrapServers, mainTopic, retryTopic1, retryTopic2, errorTopic);

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
                    new() { TopicName = retryTopic1, Delay = "00:00:01", RetryAttempts = 3 },
                    new() { TopicName = retryTopic2, Delay = "00:00:01", RetryAttempts = 2 }
                }
            }
        };

        var handler = new SucceedAfterNRetriesHandler(2); // Succeed after 2 attempts

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Debug));
        services.AddSingleton(Microsoft.Extensions.Options.Options.Create(config));
        services.AddSingleton<IMessageHandler<TestMessage>>(handler);
        services.AddSingleton<IResilientKafkaConsumer<TestMessage>, ResilientKafkaConsumer<TestMessage>>();

        var provider = services.BuildServiceProvider();
        var consumer = provider.GetRequiredService<IResilientKafkaConsumer<TestMessage>>();

        // Act
        await consumer.StartAsync();

        var testMessage = new TestMessage { Id = "MSG-SUCCESS-AFTER-001", Content = "Test succeed after retry" };
        await KafkaHelpers.ProduceJsonMessageAsync(_fixture.KafkaBootstrapServers, mainTopic, testMessage.Id, testMessage);

        await Task.Delay(TimeSpan.FromSeconds(8));
        await consumer.StopAsync();

        // Assert
        var retry1Messages = await KafkaHelpers.ConsumeMessagesAsync(
            _fixture.KafkaBootstrapServers,
            retryTopic1,
            $"verify-group-{_testId}-r1",
            timeout: TimeSpan.FromSeconds(2));

        var retry2Messages = await KafkaHelpers.ConsumeMessagesAsync(
            _fixture.KafkaBootstrapServers,
            retryTopic2,
            $"verify-group-{_testId}-r2",
            timeout: TimeSpan.FromSeconds(2));

        var errorMessages = await KafkaHelpers.ConsumeMessagesAsync(
            _fixture.KafkaBootstrapServers,
            errorTopic,
            $"verify-group-{_testId}-err",
            timeout: TimeSpan.FromSeconds(2));

        // Should only retry once (first attempt fails, second succeeds)
        retry1Messages.Should().HaveCountLessThanOrEqualTo(1, "message should succeed after first retry");
        retry2Messages.Should().BeEmpty("message should not reach second retry topic");
        errorMessages.Should().BeEmpty("message should not go to error topic if it eventually succeeds");
    }
}

