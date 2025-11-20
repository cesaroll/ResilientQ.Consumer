using ResilientQ.Consumer.Kafka.Interfaces;
using ResilientQ.Consumer.Kafka.Models;
using Ces.Weather.Contracts.Models;

namespace Weather.Consumer.Consumers;

public class WeatherEventsConsumer : IMessageHandler<WeatherForecast>
{
    private readonly ILogger<WeatherEventsConsumer> _logger;

    public WeatherEventsConsumer(ILogger<WeatherEventsConsumer> logger)
    {
        _logger = logger;
    }

    public async Task<ConsumerResult> HandleAsync(WeatherForecast message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation(
                "Received weather event for {City}: {Temperature}°C, {Summary} at {DateTime}. " +
                "Topic: {Topic}, Partition: {Partition}, Offset: {Offset}",
                message.City,
                message.Temperature,
                message.Summary,
                message.DateTime,
                metadata.Topic,
                metadata.Partition,
                metadata.Offset);

            // TODO: Implement your business logic here
            // For example: store in database, trigger notifications, etc.

            await Task.CompletedTask;

            // Return success result
            return new SuccessResult();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error processing weather event for {City}",
                message.City);

            // Return error result - the library will handle retries according to configuration
            return new ErrorResult(ex.Message);
        }
    }
}
