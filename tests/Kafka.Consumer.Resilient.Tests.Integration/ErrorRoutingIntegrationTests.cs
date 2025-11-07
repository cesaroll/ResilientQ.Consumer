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
public class ErrorRoutingIntegrationTests : IAsyncLifetime
{
    private readonly KafkaTestFixture _fixture;
    private readonly string _testId = Guid.NewGuid().ToString("N")[..8];
    private readonly List<string> _topicsToCleanup = new();

    public ErrorRoutingIntegrationTests(KafkaTestFixture fixture)
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
    public async Task ErrorResult_ShouldGoDirectlyToErrorTopic_WithoutRetrying()
    {
        // Arrange
        var mainTopic = $"test-error-direct-{_testId}";
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
                    new() { TopicName = retryTopic, Delay = "00:00:01", RetryAttempts = 3 }
                }
            }
        };

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Debug));
        services.AddSingleton(Microsoft.Extensions.Options.Options.Create(config));
        services.AddSingleton<IMessageHandler<TestMessage>, AlwaysErrorHandler>();
        services.AddSingleton<IResilientKafkaConsumer<TestMessage>, ResilientKafkaConsumer<TestMessage>>();

        var provider = services.BuildServiceProvider();
        var consumer = provider.GetRequiredService<IResilientKafkaConsumer<TestMessage>>();

        // Act
        await consumer.StartAsync();

        var testMessage = new TestMessage { Id = "MSG-ERROR-001", Content = "Test error", ProcessingBehavior = 2 };
        await KafkaHelpers.ProduceJsonMessageAsync(_fixture.KafkaBootstrapServers, mainTopic, testMessage.Id, testMessage);

        await Task.Delay(TimeSpan.FromSeconds(3));
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

        retryMessages.Should().BeEmpty("error messages should not be retried");
        errorMessages.Should().HaveCount(1, "error message should go directly to error topic");
        errorMessages[0].Message.Key.Should().Be(testMessage.Id);
    }

    [Fact]
    public async Task MultipleErrorMessages_ShouldAllGoToErrorTopic()
    {
        // Arrange
        var mainTopic = $"test-multi-error-{_testId}";
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
        services.AddSingleton<IMessageHandler<TestMessage>, AlwaysErrorHandler>();
        services.AddSingleton<IResilientKafkaConsumer<TestMessage>, ResilientKafkaConsumer<TestMessage>>();

        var provider = services.BuildServiceProvider();
        var consumer = provider.GetRequiredService<IResilientKafkaConsumer<TestMessage>>();

        // Act
        await consumer.StartAsync();

        for (int i = 0; i < 3; i++)
        {
            var testMessage = new TestMessage { Id = $"MSG-ERR-{i:D3}", Content = $"Test error {i}", ProcessingBehavior = 2 };
            await KafkaHelpers.ProduceJsonMessageAsync(_fixture.KafkaBootstrapServers, mainTopic, testMessage.Id, testMessage);
        }

        await Task.Delay(TimeSpan.FromSeconds(5));
        await consumer.StopAsync();

        // Assert
        var errorMessages = await KafkaHelpers.ConsumeMessagesAsync(
            _fixture.KafkaBootstrapServers,
            errorTopic,
            $"verify-group-{_testId}",
            timeout: TimeSpan.FromSeconds(2));

        errorMessages.Should().HaveCount(3, "all error messages should go to error topic");
    }

    [Fact]
    public async Task MixedResults_ShouldRouteCorrectly()
    {
        // Arrange
        var mainTopic = $"test-mixed-{_testId}";
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
                    new() { TopicName = retryTopic, Delay = "00:00:01", RetryAttempts = 1 }
                }
            }
        };

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Debug));
        services.AddSingleton(Microsoft.Extensions.Options.Options.Create(config));
        services.AddSingleton<IMessageHandler<TestMessage>, ConfigurableHandler>();
        services.AddSingleton<IResilientKafkaConsumer<TestMessage>, ResilientKafkaConsumer<TestMessage>>();

        var provider = services.BuildServiceProvider();
        var consumer = provider.GetRequiredService<IResilientKafkaConsumer<TestMessage>>();

        // Act
        await consumer.StartAsync();

        // Produce messages with different behaviors
        await KafkaHelpers.ProduceJsonMessageAsync(_fixture.KafkaBootstrapServers, mainTopic, "MSG-SUCCESS",
            new TestMessage { Id = "MSG-SUCCESS", Content = "Success", ProcessingBehavior = 0 });

        await KafkaHelpers.ProduceJsonMessageAsync(_fixture.KafkaBootstrapServers, mainTopic, "MSG-RETRY",
            new TestMessage { Id = "MSG-RETRY", Content = "Retryable", ProcessingBehavior = 1 });

        await KafkaHelpers.ProduceJsonMessageAsync(_fixture.KafkaBootstrapServers, mainTopic, "MSG-ERROR",
            new TestMessage { Id = "MSG-ERROR", Content = "Error", ProcessingBehavior = 2 });

        await Task.Delay(TimeSpan.FromSeconds(8));
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

        // Success message should not appear in retry or error
        // Retryable message should appear in retry (1 time) then error (after exhaustion)
        // Error message should go directly to error

        retryMessages.Should().HaveCount(1, "only retryable message should be in retry topic");
        retryMessages[0].Message.Key.Should().Be("MSG-RETRY");

        errorMessages.Should().HaveCount(2, "error message + exhausted retryable message");
        errorMessages.Select(m => m.Message.Key).Should().Contain(new[] { "MSG-ERROR", "MSG-RETRY" });
    }
}

