using ResilientQ.Consumer.Kafka.Extensions;
using Example.ConsoleApp;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

Console.WriteLine("=== ResilientQ.Consumer Example ===");
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
Console.WriteLine("  - Retry Topics: 3 configured retry topics");
Console.WriteLine("  - Error Topic: orders.error");
Console.WriteLine();
Console.WriteLine("Note: Make sure Kafka is running on localhost:9092");
Console.WriteLine("      You can produce test messages to the 'orders' topic");
Console.WriteLine();
Console.WriteLine("✨ Consumer will start automatically (BackgroundService)");
Console.WriteLine("Press Ctrl+C to stop...");
Console.WriteLine();

// Run the host - consumer starts automatically via IHostedService
await host.RunAsync();
