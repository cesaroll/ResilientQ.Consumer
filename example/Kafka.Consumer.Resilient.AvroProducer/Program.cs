using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Kafka.Consumer.Resilient.AvroProducer;

Console.WriteLine("=== Avro Message Producer ===");
Console.WriteLine();

// Parse arguments
var count = args.Length > 0 && int.TryParse(args[0], out var c) ? c : 5;
var bootstrapServers = args.Length > 1 ? args[1] : "localhost:9092";
var schemaRegistryUrl = args.Length > 2 ? args[2] : "http://localhost:8081";
var topic = args.Length > 3 ? args[3] : "orders";

Console.WriteLine($"Producing {count} Avro messages to topic '{topic}'");
Console.WriteLine($"Bootstrap Servers: {bootstrapServers}");
Console.WriteLine($"Schema Registry: {schemaRegistryUrl}");
Console.WriteLine();

// Configure Schema Registry
var schemaRegistryConfig = new SchemaRegistryConfig
{
    Url = schemaRegistryUrl
};

// Configure Producer
var producerConfig = new ProducerConfig
{
    BootstrapServers = bootstrapServers,
    Acks = Acks.All,
    LingerMs = 10,
    EnableIdempotence = true
};

try
{
    using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

    // Avro serializer configuration - use reflection to generate schema from class
    var avroSerializerConfig = new AvroSerializerConfig
    {
        AutoRegisterSchemas = true,
        SubjectNameStrategy = SubjectNameStrategy.Topic
    };

    // Create producer with Avro serializers using generic serializer
    using var producer = new ProducerBuilder<string, OrderMessage>(producerConfig)
        .SetKeySerializer(Serializers.Utf8)
        .SetValueSerializer(new AvroSerializer<OrderMessage>(schemaRegistry, avroSerializerConfig))
        .Build();

    var random = new Random();
    var produced = 0;

    for (int i = 1; i <= count; i++)
    {
        var orderId = $"ORD-{i:D3}";
        var customerId = $"CUST-{random.Next(1, 100):D3}";
        var amount = Math.Round(random.NextDouble() * 490 + 10, 2);
        var orderDate = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");

        var message = new OrderMessage
        {
            OrderId = orderId,
            CustomerId = customerId,
            Amount = amount,
            OrderDate = orderDate,
            Status = "Pending"
        };

        try
        {
            var deliveryResult = await producer.ProduceAsync(
                topic,
                new Message<string, OrderMessage>
                {
                    Key = orderId,
                    Value = message
                });

            Console.WriteLine($"✓ Produced: {orderId} (Key: {orderId}, Customer: {customerId}, Amount: ${amount:F2}) -> Partition {deliveryResult.Partition}, Offset {deliveryResult.Offset}");
            produced++;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"✗ Failed to produce {orderId}: {ex.Message}");
            if (ex.InnerException != null)
            {
                Console.WriteLine($"   Inner: {ex.InnerException.Message}");
                Console.WriteLine($"   Stack: {ex.InnerException.StackTrace}");
            }
        }
    }

    // Flush to ensure all messages are sent
    producer.Flush(TimeSpan.FromSeconds(10));

    Console.WriteLine();
    Console.WriteLine($"✓ Successfully produced {produced}/{count} Avro messages!");
    Console.WriteLine();
    Console.WriteLine("Schema registered at: " + schemaRegistryUrl + "/subjects/" + topic + "-value/versions");
    Console.WriteLine();
    Console.WriteLine("To verify:");
    Console.WriteLine($"  kcat -b {bootstrapServers} -t {topic} -C -f 'Key: %k | Offset: %o\\n' -e -o beginning");
}
catch (Exception ex)
{
    Console.WriteLine($"Error: {ex.Message}");
    Console.WriteLine(ex.StackTrace);
    return 1;
}

return 0;

