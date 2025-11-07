using Ces.Kafka.Consumer.Resilient.Configuration;
using FluentAssertions;
using Xunit;

namespace Kafka.Consumer.Resilient.Tests.Unit;

public class RetryTopicConfigurationTests
{
    [Theory]
    [InlineData("00:00:30", 0, 0, 30)]
    [InlineData("00:05:00", 0, 5, 0)]
    [InlineData("00:25:00", 0, 25, 0)]
    [InlineData("01:00:00", 1, 0, 0)]
    [InlineData("02:30:15", 2, 30, 15)]
    [InlineData("23:59:59", 23, 59, 59)]
    public void GetDelayTimeSpan_ValidFormats_ShouldParseCorrectly(
        string delay, int expectedHours, int expectedMinutes, int expectedSeconds)
    {
        // Arrange
        var config = new RetryTopicConfiguration { Delay = delay };

        // Act
        var result = config.GetDelayTimeSpan();

        // Assert
        result.Hours.Should().Be(expectedHours);
        result.Minutes.Should().Be(expectedMinutes);
        result.Seconds.Should().Be(expectedSeconds);
    }

    [Theory]
    [InlineData("invalid")]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("abc:def:ghi")]
    public void GetDelayTimeSpan_InvalidFormats_ShouldThrowFormatException(string delay)
    {
        // Arrange
        var config = new RetryTopicConfiguration { Delay = delay };

        // Act
        var act = () => config.GetDelayTimeSpan();

        // Assert
        act.Should().Throw<FormatException>()
            .WithMessage($"*{delay}*");
    }

    [Theory]
    [InlineData("25:00:00")]  // TimeSpan allows hours > 23
    [InlineData("12:30")]     // TimeSpan allows format without seconds
    [InlineData("1:00:00")]   // TimeSpan allows single digit hours
    public void GetDelayTimeSpan_FlexibleFormats_ShouldParseCorrectly(string delay)
    {
        // Arrange
        var config = new RetryTopicConfiguration { Delay = delay };

        // Act
        var act = () => config.GetDelayTimeSpan();

        // Assert - These should NOT throw because TimeSpan.TryParseExact is flexible
        act.Should().NotThrow();
    }

    [Fact]
    public void DefaultValues_ShouldBeSet()
    {
        // Arrange & Act
        var config = new RetryTopicConfiguration();

        // Assert
        config.TopicName.Should().BeEmpty();
        config.Delay.Should().Be("00:00:30");
        config.RetryAttempts.Should().Be(1);
        config.GroupId.Should().BeNull();
    }

    [Fact]
    public void GetDelayTimeSpan_DefaultDelay_ShouldReturn30Seconds()
    {
        // Arrange
        var config = new RetryTopicConfiguration(); // Uses default "00:00:30"

        // Act
        var result = config.GetDelayTimeSpan();

        // Assert
        result.TotalSeconds.Should().Be(30);
    }

    [Fact]
    public void Properties_ShouldBeSettable()
    {
        // Arrange & Act
        var config = new RetryTopicConfiguration
        {
            TopicName = "orders.retry.1",
            Delay = "00:05:00",
            RetryAttempts = 3,
            GroupId = "custom-group"
        };

        // Assert
        config.TopicName.Should().Be("orders.retry.1");
        config.Delay.Should().Be("00:05:00");
        config.RetryAttempts.Should().Be(3);
        config.GroupId.Should().Be("custom-group");
    }
}

