using ResilientQ.Consumer.Kafka.Interfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ResilientQ.Consumer.Kafka.Services;

/// <summary>
/// Hosted service that automatically starts and stops the Kafka consumer
/// </summary>
/// <typeparam name="TMessage">The type of message to consume</typeparam>
public class ResilientKafkaConsumerHostedService<TMessage> : BackgroundService where TMessage : class
{
    private readonly IResilientKafkaConsumer<TMessage> _consumer;
    private readonly ILogger<ResilientKafkaConsumerHostedService<TMessage>> _logger;

    public ResilientKafkaConsumerHostedService(
        IResilientKafkaConsumer<TMessage> consumer,
        ILogger<ResilientKafkaConsumerHostedService<TMessage>> logger)
    {
        _consumer = consumer;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting Kafka consumer for {MessageType}", typeof(TMessage).Name);

        try
        {
            await _consumer.StartAsync(stoppingToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Kafka consumer for {MessageType} stopped gracefully", typeof(TMessage).Name);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Kafka consumer for {MessageType} encountered an error", typeof(TMessage).Name);
            throw;
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping Kafka consumer for {MessageType}", typeof(TMessage).Name);

        await _consumer.StopAsync(cancellationToken);
        await base.StopAsync(cancellationToken);
    }
}

