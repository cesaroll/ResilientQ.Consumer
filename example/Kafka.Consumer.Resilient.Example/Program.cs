using Ces.Kafka.Consumer.Resilient.Extensions;
using Ces.Kafka.Consumer.Resilient.Interfaces;
using Kafka.Consumer.Resilient.Example;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

Console.WriteLine("=== Ces.Kafka.Consumer.Resilient Example ===");
Console.WriteLine();

// Create host builder
var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        // Register the resilient Kafka consumer with configuration from appsettings.json
        services.AddResilientKafkaConsumer<OrderMessage, OrderMessageHandler>(
            context.Configuration.GetSection("KafkaConsumer"));

        // Add console logging
        services.AddLogging(builder =>
        {
            builder.AddConsole();
            builder.SetMinimumLevel(LogLevel.Information);
        });
    })
    .Build();

Console.WriteLine("Starting Kafka consumer...");
Console.WriteLine("Configuration:");
Console.WriteLine("  - Topic: orders");
Console.WriteLine("  - Consumer Count: 2");
Console.WriteLine("  - Group ID: order-processor-group");
Console.WriteLine("  - Bootstrap Servers: localhost:9092");
Console.WriteLine("  - Retry Attempts: 3");
Console.WriteLine("  - Retry Delay: 2000ms");
Console.WriteLine("  - Error Topic: orders.error");
Console.WriteLine();
Console.WriteLine("Note: Make sure Kafka is running on localhost:9092");
Console.WriteLine("      You can produce test messages to the 'orders' topic");
Console.WriteLine();
Console.WriteLine("Press Ctrl+C to stop...");
Console.WriteLine();

// Get the consumer service
var consumer = host.Services.GetRequiredService<IResilientKafkaConsumer<OrderMessage>>();

// Create cancellation token source for graceful shutdown
using var cts = new CancellationTokenSource();

// Handle Ctrl+C
Console.CancelKeyPress += (sender, e) =>
{
    e.Cancel = true;
    Console.WriteLine("\nShutdown requested...");
    cts.Cancel();
};

try
{
    // Start consuming
    await consumer.StartAsync(cts.Token);

    // Wait until cancellation is requested
    await Task.Delay(Timeout.Infinite, cts.Token);
}
catch (OperationCanceledException)
{
    Console.WriteLine("Stopping consumer...");
}
catch (Exception ex)
{
    Console.WriteLine($"Error: {ex.Message}");
}
finally
{
    // Stop consuming
    await consumer.StopAsync();
    Console.WriteLine("Consumer stopped.");
}
