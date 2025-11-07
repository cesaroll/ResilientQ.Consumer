var builder = DistributedApplication.CreateBuilder(args);

// Add Kafka with KafkaUI for visualization during tests
var kafka = builder.AddKafka("kafka")
    .WithKafkaUI();

// Add Schema Registry for Avro support
// Use Aspire's connection string expression to properly reference Kafka
var schemaRegistry = builder.AddContainer("schema-registry", "confluentinc/cp-schema-registry", "7.8.0")
    .WithEnvironment("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
    .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafka.Resource.ConnectionStringExpression)
    .WithEnvironment("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
    .WithHttpEndpoint(port: 8081, targetPort: 8081, name: "http")
    .WaitFor(kafka);

builder.Build().Run();

