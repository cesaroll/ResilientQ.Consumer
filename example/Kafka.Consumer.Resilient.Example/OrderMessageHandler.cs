using Ces.Kafka.Consumer.Resilient.Interfaces;
using Ces.Kafka.Consumer.Resilient.Models;
using Microsoft.Extensions.Logging;

namespace Kafka.Consumer.Resilient.Example;

/// <summary>
/// Example message handler for order messages
/// </summary>
public class OrderMessageHandler : IMessageHandler<OrderMessage>
{
    private readonly ILogger<OrderMessageHandler> _logger;
    private static readonly Random _random = new();

    public OrderMessageHandler(ILogger<OrderMessageHandler> logger)
    {
        _logger = logger;
    }

    public async Task<ConsumerResult> HandleAsync(
        OrderMessage message,
        MessageMetadata metadata,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Processing order {OrderId} for customer {CustomerId}, amount: {Amount:C}, retry attempt: {RetryAttempt}",
            message.OrderId,
            message.CustomerId,
            message.Amount,
            metadata.RetryAttempt);

        try
        {
            // Simulate processing
            await Task.Delay(500, cancellationToken);

            // Simulate different scenarios for demonstration
            var scenario = _random.Next(0, 10);

            switch (scenario)
            {
                case 0: // 10% chance of retryable error
                    _logger.LogWarning("Simulating temporary database connection issue for order {OrderId}", message.OrderId);
                    return new RetryableResult(
                        "Database temporarily unavailable",
                        new InvalidOperationException("Connection timeout"));

                case 1: // 10% chance of non-retryable error
                    _logger.LogError("Simulating invalid order data for order {OrderId}", message.OrderId);
                    return new ErrorResult(
                        "Invalid order data: Amount cannot be negative",
                        new ArgumentException("Invalid amount"));

                default: // 80% success
                    _logger.LogInformation("Successfully processed order {OrderId}", message.OrderId);
                    return new SuccessResult($"Order {message.OrderId} processed successfully");
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Processing cancelled for order {OrderId}", message.OrderId);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error processing order {OrderId}", message.OrderId);
            return new ErrorResult($"Unexpected error: {ex.Message}", ex);
        }
    }
}

