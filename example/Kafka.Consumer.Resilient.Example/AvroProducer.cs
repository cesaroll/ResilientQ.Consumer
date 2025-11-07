using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace Kafka.Consumer.Resilient.Example;

/// <summary>
/// Example Avro producer for testing
/// This can be used as a C# alternative to the bash script
/// </summary>
public class AvroProducer
{
    private readonly IProducer<string, OrderMessage> _producer;

    public AvroProducer(string bootstrapServers, string schemaRegistryUrl)
    {
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = schemaRegistryUrl
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All
        };

        var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

        _producer = new ProducerBuilder<string, OrderMessage>(producerConfig)
            .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
            .SetValueSerializer(new AvroSerializer<OrderMessage>(schemaRegistry))
            .Build();
    }

    public async Task ProduceAsync(string key, OrderMessage message, CancellationToken cancellationToken = default)
    {
        try
        {
            var result = await _producer.ProduceAsync(
                "orders",
                new Message<string, OrderMessage>
                {
                    Key = key,
                    Value = message
                },
                cancellationToken);

            Console.WriteLine($"✓ Produced Avro message to {result.TopicPartitionOffset}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Failed to produce message: {ex.Message}");
        }
    }

    public void Dispose()
    {
        _producer?.Dispose();
    }
}

