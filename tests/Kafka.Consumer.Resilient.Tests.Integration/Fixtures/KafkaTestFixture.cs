using Aspire.Hosting;
using Aspire.Hosting.Testing;
using Xunit;

namespace Kafka.Consumer.Resilient.Tests.Integration.Fixtures;

/// <summary>
/// Test fixture that manages Kafka infrastructure lifecycle using Aspire
/// This ensures tests are fully independent and can run anywhere
/// </summary>
public class KafkaTestFixture : IAsyncLifetime
{
    private DistributedApplication? _app;

    public string KafkaBootstrapServers { get; private set; } = string.Empty;
    public string SchemaRegistryUrl { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        // Create and start the Aspire AppHost
        var appHost = await DistributedApplicationTestingBuilder
            .CreateAsync<Projects.Kafka_Consumer_Resilient_Tests_AppHost>();

        _app = await appHost.BuildAsync();
        await _app.StartAsync();

        // Get the actual endpoints from Aspire resources
        // Aspire assigns dynamic ports, we need to retrieve them
        // Kafka has a 'tcp' endpoint for external connections
        var kafkaEndpoint = _app.GetEndpoint("kafka", "tcp");
        KafkaBootstrapServers = $"{kafkaEndpoint.Host}:{kafkaEndpoint.Port}";

        var schemaRegistryEndpoint = _app.GetEndpoint("schema-registry", "http");
        SchemaRegistryUrl = schemaRegistryEndpoint.ToString();

        // Wait for services to be fully ready
        await Task.Delay(TimeSpan.FromSeconds(15));

        Console.WriteLine($"✅ Aspire infrastructure started:");
        Console.WriteLine($"   Kafka: {KafkaBootstrapServers}");
        Console.WriteLine($"   Schema Registry: {SchemaRegistryUrl}");
    }

    public async Task DisposeAsync()
    {
        if (_app != null)
        {
            Console.WriteLine("🧹 Stopping Aspire infrastructure...");
            await _app.StopAsync();
            await _app.DisposeAsync();
            Console.WriteLine("✅ Aspire infrastructure stopped");
        }
    }
}

