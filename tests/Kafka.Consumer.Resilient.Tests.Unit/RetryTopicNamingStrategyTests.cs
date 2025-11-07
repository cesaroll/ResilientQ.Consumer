using Ces.Kafka.Consumer.Resilient.Services;
using FluentAssertions;
using Xunit;

namespace Kafka.Consumer.Resilient.Tests.Unit;

public class RetryTopicNamingStrategyTests
{
    [Theory]
    [InlineData("orders", 1, "orders.retry.1")]
    [InlineData("orders", 2, "orders.retry.2")]
    [InlineData("orders", 3, "orders.retry.3")]
    [InlineData("payments.transactions", 1, "payments.transactions.retry.1")]
    [InlineData("my-topic", 5, "my-topic.retry.5")]
    public void GetRetryTopicName_ShouldReturnCorrectFormat(string baseTopic, int attemptNumber, string expected)
    {
        // Act
        var result = RetryTopicNamingStrategy.GetRetryTopicName(baseTopic, attemptNumber);

        // Assert
        result.Should().Be(expected);
    }

    [Theory]
    [InlineData("orders.retry.1", 1)]
    [InlineData("orders.retry.2", 2)]
    [InlineData("orders.retry.3", 3)]
    [InlineData("payments.transactions.retry.5", 5)]
    [InlineData("orders", 0)] // Main topic has no retry attempt
    public void GetRetryAttempt_ShouldExtractCorrectAttemptNumber(string topicName, int expected)
    {
        // Act
        var result = RetryTopicNamingStrategy.GetRetryAttempt(topicName);

        // Assert
        result.Should().Be(expected);
    }

    [Theory]
    [InlineData("orders.retry.1", "orders")]
    [InlineData("orders.retry.2", "orders")]
    [InlineData("orders.retry.3", "orders")]
    [InlineData("payments.transactions.retry.1", "payments.transactions")]
    [InlineData("orders", "orders")] // Main topic returns itself
    public void GetBaseTopicName_ShouldExtractCorrectBaseName(string topicName, string expected)
    {
        // Act
        var result = RetryTopicNamingStrategy.GetBaseTopicName(topicName);

        // Assert
        result.Should().Be(expected);
    }

    [Fact]
    public void GetRetryTopicName_WithZeroAttempt_ShouldReturnCorrectFormat()
    {
        // Act
        var result = RetryTopicNamingStrategy.GetRetryTopicName("orders", 0);

        // Assert
        result.Should().Be("orders.retry.0");
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void GetRetryTopicName_WithInvalidBaseTopic_ShouldStillReturnFormat(string? baseTopic)
    {
        // Act
        var result = RetryTopicNamingStrategy.GetRetryTopicName(baseTopic!, 1);

        // Assert
        result.Should().EndWith(".retry.1");
    }
}

