using ResilientQ.Consumer.Kafka.Models;
using FluentAssertions;
using Xunit;

namespace ResilientQ.Consumer.Kafka.Tests.Unit;

public class RetryMetadataTests
{
    [Fact]
    public void DefaultValues_ShouldBeZero()
    {
        // Arrange & Act
        var metadata = new RetryMetadata();

        // Assert
        metadata.RetryTopicIndex.Should().Be(0);
        metadata.AttemptsInCurrentTopic.Should().Be(0);
        metadata.TotalAttempts.Should().Be(0);
    }

    [Fact]
    public void Properties_ShouldBeSettable()
    {
        // Arrange & Act
        var metadata = new RetryMetadata
        {
            RetryTopicIndex = 2,
            AttemptsInCurrentTopic = 3,
            TotalAttempts = 5
        };

        // Assert
        metadata.RetryTopicIndex.Should().Be(2);
        metadata.AttemptsInCurrentTopic.Should().Be(3);
        metadata.TotalAttempts.Should().Be(5);
    }

    [Fact]
    public void InitialRetry_FromMainTopic_ShouldHaveCorrectValues()
    {
        // Arrange & Act
        var metadata = new RetryMetadata
        {
            RetryTopicIndex = -1,
            AttemptsInCurrentTopic = 0,
            TotalAttempts = 0
        };

        // Assert
        metadata.RetryTopicIndex.Should().Be(-1, "because -1 indicates message is from main topic");
        metadata.AttemptsInCurrentTopic.Should().Be(0);
        metadata.TotalAttempts.Should().Be(0);
    }

    [Fact]
    public void FirstRetryAttempt_ShouldHaveCorrectValues()
    {
        // Arrange & Act
        var metadata = new RetryMetadata
        {
            RetryTopicIndex = 0,
            AttemptsInCurrentTopic = 0,
            TotalAttempts = 1
        };

        // Assert
        metadata.RetryTopicIndex.Should().Be(0, "because it's the first retry topic");
        metadata.AttemptsInCurrentTopic.Should().Be(0, "because it's the first attempt in this topic");
        metadata.TotalAttempts.Should().Be(1, "because it's the first overall attempt");
    }

    [Fact]
    public void SecondAttemptInSameTopic_ShouldHaveCorrectValues()
    {
        // Arrange & Act
        var metadata = new RetryMetadata
        {
            RetryTopicIndex = 0,
            AttemptsInCurrentTopic = 1,
            TotalAttempts = 2
        };

        // Assert
        metadata.RetryTopicIndex.Should().Be(0, "because it's still in the first retry topic");
        metadata.AttemptsInCurrentTopic.Should().Be(1, "because it's the second attempt in this topic");
        metadata.TotalAttempts.Should().Be(2, "because it's the second overall attempt");
    }

    [Fact]
    public void MovingToNextTopic_ShouldResetAttemptsInCurrentTopic()
    {
        // Arrange & Act
        var metadata = new RetryMetadata
        {
            RetryTopicIndex = 1,
            AttemptsInCurrentTopic = 0,
            TotalAttempts = 3
        };

        // Assert
        metadata.RetryTopicIndex.Should().Be(1, "because it moved to the second retry topic");
        metadata.AttemptsInCurrentTopic.Should().Be(0, "because it's the first attempt in the new topic");
        metadata.TotalAttempts.Should().Be(3, "because total attempts continue incrementing");
    }

    [Theory]
    [InlineData(-1, 0, 0, true)] // Main topic
    [InlineData(0, 0, 1, false)] // First retry topic
    [InlineData(0, 1, 2, false)] // Second attempt in first retry topic
    [InlineData(1, 0, 3, false)] // Second retry topic
    public void Scenarios_ShouldMatchExpectedValues(
        int retryTopicIndex,
        int attemptsInCurrentTopic,
        int totalAttempts,
        bool isFromMainTopic)
    {
        // Arrange & Act
        var metadata = new RetryMetadata
        {
            RetryTopicIndex = retryTopicIndex,
            AttemptsInCurrentTopic = attemptsInCurrentTopic,
            TotalAttempts = totalAttempts
        };

        // Assert
        metadata.RetryTopicIndex.Should().Be(retryTopicIndex);
        metadata.AttemptsInCurrentTopic.Should().Be(attemptsInCurrentTopic);
        metadata.TotalAttempts.Should().Be(totalAttempts);
        (metadata.RetryTopicIndex == -1).Should().Be(isFromMainTopic);
    }
}

