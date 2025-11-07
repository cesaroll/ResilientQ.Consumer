using Ces.Kafka.Consumer.Resilient.Configuration;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafka.Consumer.Resilient.Tests.Unit;

public class ConfigurationValidationTests
{
    [Fact]
    public void LoadFromConfiguration_ValidConfig_ShouldLoadSuccessfully()
    {
        // Arrange
        var configData = new Dictionary<string, string>
        {
            ["TopicName"] = "orders",
            ["ConsumerNumber"] = "2",
            ["GroupId"] = "test-group",
            ["BootstrapServers"] = "localhost:9092",
            ["SchemaRegistryUrl"] = "http://localhost:8081",
            ["Error:TopicName"] = "orders.error",
            ["RetryPolicy:RetryTopics:0:TopicName"] = "orders.retry.1",
            ["RetryPolicy:RetryTopics:0:Delay"] = "00:00:30",
            ["RetryPolicy:RetryTopics:0:RetryAttempts"] = "2"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData!)
            .Build();

        // Act
        var config = new KafkaConsumerConfiguration();
        configuration.Bind(config);

        // Assert
        config.TopicName.Should().Be("orders");
        config.ConsumerNumber.Should().Be(2);
        config.GroupId.Should().Be("test-group");
        config.BootstrapServers.Should().Be("localhost:9092");
        config.SchemaRegistryUrl.Should().Be("http://localhost:8081");
        config.Error.TopicName.Should().Be("orders.error");
        config.RetryPolicy.RetryTopics.Should().HaveCount(1);
        config.RetryPolicy.RetryTopics[0].TopicName.Should().Be("orders.retry.1");
        config.RetryPolicy.RetryTopics[0].Delay.Should().Be("00:00:30");
        config.RetryPolicy.RetryTopics[0].RetryAttempts.Should().Be(2);
    }

    [Fact]
    public void ValidateConfiguration_MissingTopicName_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConsumerConfiguration
        {
            TopicName = "",
            BootstrapServers = "localhost:9092",
            GroupId = "test-group",
            Error = new ErrorConfiguration { TopicName = "error" },
            RetryPolicy = new RetryPolicyConfiguration
            {
                RetryTopics = new List<RetryTopicConfiguration>
                {
                    new() { TopicName = "retry", Delay = "00:00:30" }
                }
            }
        };

        // Act
        var act = () => ConfigurationLoader.ValidateConfiguration(config);

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*TopicName is required*");
    }

    [Fact]
    public void ValidateConfiguration_MissingBootstrapServers_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConsumerConfiguration
        {
            TopicName = "orders",
            BootstrapServers = "",
            GroupId = "test-group",
            Error = new ErrorConfiguration { TopicName = "error" },
            RetryPolicy = new RetryPolicyConfiguration
            {
                RetryTopics = new List<RetryTopicConfiguration>
                {
                    new() { TopicName = "retry", Delay = "00:00:30" }
                }
            }
        };

        // Act
        var act = () => ConfigurationLoader.ValidateConfiguration(config);

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*BootstrapServers is required*");
    }

    [Fact]
    public void ValidateConfiguration_MissingGroupId_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConsumerConfiguration
        {
            TopicName = "orders",
            BootstrapServers = "localhost:9092",
            GroupId = "",
            Error = new ErrorConfiguration { TopicName = "error" },
            RetryPolicy = new RetryPolicyConfiguration
            {
                RetryTopics = new List<RetryTopicConfiguration>
                {
                    new() { TopicName = "retry", Delay = "00:00:30" }
                }
            }
        };

        // Act
        var act = () => ConfigurationLoader.ValidateConfiguration(config);

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*GroupId is required*");
    }

    [Fact]
    public void ValidateConfiguration_MissingErrorTopicName_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConsumerConfiguration
        {
            TopicName = "orders",
            BootstrapServers = "localhost:9092",
            GroupId = "test-group",
            Error = new ErrorConfiguration { TopicName = "" },
            RetryPolicy = new RetryPolicyConfiguration
            {
                RetryTopics = new List<RetryTopicConfiguration>
                {
                    new() { TopicName = "retry", Delay = "00:00:30" }
                }
            }
        };

        // Act
        var act = () => ConfigurationLoader.ValidateConfiguration(config);

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Error.TopicName is required*");
    }

    [Fact]
    public void ValidateConfiguration_ConsumerNumberLessThanOne_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConsumerConfiguration
        {
            TopicName = "orders",
            BootstrapServers = "localhost:9092",
            GroupId = "test-group",
            ConsumerNumber = 0,
            Error = new ErrorConfiguration { TopicName = "error" },
            RetryPolicy = new RetryPolicyConfiguration
            {
                RetryTopics = new List<RetryTopicConfiguration>
                {
                    new() { TopicName = "retry", Delay = "00:00:30" }
                }
            }
        };

        // Act
        var act = () => ConfigurationLoader.ValidateConfiguration(config);

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*ConsumerNumber must be at least 1*");
    }

    [Fact]
    public void ValidateConfiguration_NoRetryTopics_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConsumerConfiguration
        {
            TopicName = "orders",
            BootstrapServers = "localhost:9092",
            GroupId = "test-group",
            Error = new ErrorConfiguration { TopicName = "error" },
            RetryPolicy = new RetryPolicyConfiguration
            {
                RetryTopics = new List<RetryTopicConfiguration>()
            }
        };

        // Act
        var act = () => ConfigurationLoader.ValidateConfiguration(config);

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*RetryPolicy.RetryTopics must contain at least one retry topic*");
    }

    [Fact]
    public void ValidateConfiguration_RetryTopicMissingTopicName_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConsumerConfiguration
        {
            TopicName = "orders",
            BootstrapServers = "localhost:9092",
            GroupId = "test-group",
            Error = new ErrorConfiguration { TopicName = "error" },
            RetryPolicy = new RetryPolicyConfiguration
            {
                RetryTopics = new List<RetryTopicConfiguration>
                {
                    new() { TopicName = "", Delay = "00:00:30" }
                }
            }
        };

        // Act
        var act = () => ConfigurationLoader.ValidateConfiguration(config);

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*RetryPolicy.RetryTopics[0].TopicName is required*");
    }

    [Fact]
    public void ValidateConfiguration_RetryTopicInvalidDelayFormat_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConsumerConfiguration
        {
            TopicName = "orders",
            BootstrapServers = "localhost:9092",
            GroupId = "test-group",
            Error = new ErrorConfiguration { TopicName = "error" },
            RetryPolicy = new RetryPolicyConfiguration
            {
                RetryTopics = new List<RetryTopicConfiguration>
                {
                    new() { TopicName = "retry", Delay = "invalid" }
                }
            }
        };

        // Act
        var act = () => ConfigurationLoader.ValidateConfiguration(config);

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*RetryPolicy.RetryTopics[0].Delay has invalid format*");
    }

    [Fact]
    public void ValidateConfiguration_RetryTopicNegativeRetryAttempts_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConsumerConfiguration
        {
            TopicName = "orders",
            BootstrapServers = "localhost:9092",
            GroupId = "test-group",
            Error = new ErrorConfiguration { TopicName = "error" },
            RetryPolicy = new RetryPolicyConfiguration
            {
                RetryTopics = new List<RetryTopicConfiguration>
                {
                    new() { TopicName = "retry", Delay = "00:00:30", RetryAttempts = -1 }
                }
            }
        };

        // Act
        var act = () => ConfigurationLoader.ValidateConfiguration(config);

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*RetryPolicy.RetryTopics[0].RetryAttempts cannot be negative*");
    }

    [Fact]
    public void ValidateConfiguration_ValidMultipleRetryTopics_ShouldNotThrow()
    {
        // Arrange
        var config = new KafkaConsumerConfiguration
        {
            TopicName = "orders",
            BootstrapServers = "localhost:9092",
            GroupId = "test-group",
            Error = new ErrorConfiguration { TopicName = "error" },
            RetryPolicy = new RetryPolicyConfiguration
            {
                RetryTopics = new List<RetryTopicConfiguration>
                {
                    new() { TopicName = "retry1", Delay = "00:00:30", RetryAttempts = 2 },
                    new() { TopicName = "retry2", Delay = "00:05:00", RetryAttempts = 1 },
                    new() { TopicName = "retry3", Delay = "00:30:00", RetryAttempts = 1 }
                }
            }
        };

        // Act
        var act = () => ConfigurationLoader.ValidateConfiguration(config);

        // Assert
        act.Should().NotThrow();
    }
}

